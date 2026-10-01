const express = require('express');
const multer = require('multer');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegStatic = require('ffmpeg-static');
const ffprobePath = require('ffprobe-static').path;
const axios = require('axios');
const rateLimit = require('express-rate-limit');
const ipKeyGenerator = rateLimit.ipKeyGenerator;
const compression = require('compression');
const crypto = require('crypto');
const { URL } = require('url');
const pino = require('pino');
const qrcode = require('qrcode-terminal');
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  Browsers,
  fetchLatestBaileysVersion,
  fetchLatestWaWebVersion,
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const { enforceInputLimits } = require('./src/server/inputLimits');
const { validateUploadRequest } = require('./src/server/videoTypes');
const { verifyChallenge, isValidSignature, summarizeEvent } = require('./src/server/metaWebhook');
const { isStaleMessage, MessageDeduper, WelcomeThrottle } = require('./src/server/inboundGuards');
const { JobStore } = require('./src/server/jobStore');
const { routeRecipes, collectAvailableAssets } = require('./src/server/processRouting');
const { planRecipeAssets, markLoopingImageInputs } = require('./src/server/assetResolver');
const { rasterizeTextOverlay } = require('./src/server/textRaster');
const tgsRaster = require('./src/server/tgsRaster');
const musicRoutes = require('./src/server/musicRoutes');
const { validateRecipe } = require('./src/server/recipeValidator');
const { planRender, planChunk, chunkCount } = require('./src/server/renderEngine');
const { RequestContext, cleanupRequest, makeLocalObjectDeps, startupSweep, makeSweepDeps } = require('./src/server/cleanup');
const { defaultEncodeSemaphore } = require('./src/server/encodeSemaphore');

// ========================
// PRIORITÉ D'ENCODAGE — PREMIUM / GRATUIT
// ========================
// The existing EncodeSemaphore still enforces the CPU-derived FFmpeg limit.
// This second, request-level gate prioritizes ACTIVE paid subscriptions when
// several complete processing jobs are waiting to enter the encoding phase.
// Tune PROCESSING_JOB_CONCURRENCY for the host's RAM/CPU capacity.
const PROCESSING_JOB_CONCURRENCY = Math.max(
  1,
  Number.parseInt(process.env.PROCESSING_JOB_CONCURRENCY || '2', 10) || 2
);
let activeProcessingJobs = 0;
let processingQueueSequence = 0;
const processingPriorityQueue = [];

function pumpProcessingPriorityQueue() {
  processingPriorityQueue.sort((a, b) =>
    a.priority - b.priority || a.sequence - b.sequence
  );

  while (
    activeProcessingJobs < PROCESSING_JOB_CONCURRENCY &&
    processingPriorityQueue.length
  ) {
    const next = processingPriorityQueue.shift();
    activeProcessingJobs += 1;

    let released = false;
    next.resolve(() => {
      if (released) return;
      released = true;
      activeProcessingJobs = Math.max(0, activeProcessingJobs - 1);
      pumpProcessingPriorityQueue();
    });
  }
}

function acquireProcessingPrioritySlot(isPremium) {
  return new Promise((resolve) => {
    processingPriorityQueue.push({
      // Lower number = higher priority. Equal-priority jobs remain FIFO.
      priority: isPremium ? 0 : 1,
      sequence: processingQueueSequence++,
      resolve,
    });
    pumpProcessingPriorityQueue();
  });
}

const {
  runGatedEncode,
  buildRecipeCommand,
  encodeRecipePlan,
  tightenEncodeOptions,
  FULL_VIDEO_TIMEOUT_MS,
  CHUNK_TIMEOUT_MS,
  RETRY_SIZE_MB,
  MAX_ENCODE_ATTEMPTS,
} = require('./src/server/encodeExec');
const { CLIP_DURATION_LIMIT } = require('./src/shared/constants');
require('dotenv').config();

// ========================
// APP SETUP
// ========================
const app = express();
app.use(compression());

// IMPORTANT: JSON/body parsing must be registered BEFORE any API routes.
// The subscription SoftPay route is declared earlier in this file, so placing
// express.json() only in the later middleware section leaves req.body empty
// for /api/subscription/softpay/order and causes HTTP 400
// "Offre d'abonnement invalide".
// Keep rawBody for signed webhook verification.
app.use(express.json({
  verify: (req, res, buf) => { req.rawBody = buf; },
}));
app.use(express.urlencoded({ extended: false }));

const PORT = process.env.PORT || 3000;
app.set('trust proxy', 1);

// ========================
// LOCAL AUTH + SUBSCRIPTION (7-DAY TRIAL)
// ========================
const DATA_DIR = path.resolve(process.env.DATA_DIR || 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const AUTH_SESSIONS_FILE = path.join(DATA_DIR, 'auth-sessions.json');
const AUTH_CHALLENGES_FILE = path.join(DATA_DIR, 'auth-challenges.json');
const ACCOUNT_LINKS_FILE = path.join(DATA_DIR, 'account-links.json');
const ACCOUNT_LINK_ATTEMPTS_FILE = path.join(DATA_DIR, 'account-link-attempts.json');
const SUBSCRIPTION_ORDERS_FILE = path.join(DATA_DIR, 'subscription-orders.json');
function readJson(file, fallback) { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; } }
function writeJson(file, value) { fs.writeFileSync(file, JSON.stringify(value, null, 2)); }
function readAccountLinkAttempts() { return readJson(ACCOUNT_LINK_ATTEMPTS_FILE, {}); }
function writeAccountLinkAttempts(v) { writeJson(ACCOUNT_LINK_ATTEMPTS_FILE, v); }
function readUsers() { return readJson(USERS_FILE, {}); }
function writeUsers(v) { writeJson(USERS_FILE, v); }
function readAuthSessions() { return readJson(AUTH_SESSIONS_FILE, {}); }
function writeAuthSessions(v) { writeJson(AUTH_SESSIONS_FILE, v); }
function readAuthChallenges() { return readJson(AUTH_CHALLENGES_FILE, {}); }
function writeAuthChallenges(v) { writeJson(AUTH_CHALLENGES_FILE, v); }
function readAccountLinks() { return readJson(ACCOUNT_LINKS_FILE, {}); }
function writeAccountLinks(v) { writeJson(ACCOUNT_LINKS_FILE, v); }
function readSubscriptionOrders() { return readJson(SUBSCRIPTION_ORDERS_FILE, {}); }
function writeSubscriptionOrders(v) { writeJson(SUBSCRIPTION_ORDERS_FILE, v); }
const TRIAL_DAYS = Number(process.env.TRIAL_DAYS || 7);
const SUBSCRIPTION_DAYS = Number(process.env.SUBSCRIPTION_DAYS || 30);
const AUTH_DEV_MODE = String(process.env.AUTH_DEV_MODE || 'true').toLowerCase() === 'true';
const MOMO_ENABLED = String(process.env.MOMO_ENABLED || 'false').toLowerCase() === 'true';
const MOMO_MONTHLY_AMOUNT_XOF = Number(process.env.MOMO_MONTHLY_AMOUNT_XOF || 0);
// ============================================================
// CARTFLOX SANDBOX — CLÉS
// ============================================================
// Clé PUBLIQUE Sandbox (peut être utilisée côté frontend) :
// af_test_pub_52f4d87549f7619ec93d8119d5272071e43e626f9d9f74ef
//
// IMPORTANT : ne mets JAMAIS la clé SECRÈTE dans le frontend.
// Mets ta clé secrète Cartflox Sandbox dans le fichier .env du serveur :
// CARTFLOX_SECRET_KEY=COLLE_ICI_TA_CLE_SECRETE_SANDBOX
//
// La clé secrète doit rester uniquement côté backend et ne doit pas être
// envoyée dans le chat, GitHub, Netlify, le HTML ou le JavaScript frontend.
const CARTFLOX_SECRET_KEY = String(process.env.CARTFLOX_SECRET_KEY || '').trim();
const CARTFLOX_PUBLIC_KEY = String(process.env.CARTFLOX_PUBLIC_KEY || 'af_test_pub_52f4d87549f7619ec93d8119d5272071e43e626f9d9f74ef').trim();
const CARTFLOX_ENABLED = Boolean(CARTFLOX_SECRET_KEY);
const CARTFLOX_API_URL = String(process.env.CARTFLOX_API_URL || 'https://cartflox.com/api').replace(/\/$/, '');
const PUBLIC_SITE_URL = String(process.env.PUBLIC_SITE_URL || process.env.APP_URL || '').trim().replace(/\/$/, '');

const SUBSCRIPTION_PLANS = Object.freeze({
  '1m':  { amount: 2000,  days: 30,  label: '1 mois' },
  '6m':  { amount: 10000, days: 180, label: '6 mois' },
  '12m': { amount: 18000, days: 365, label: '12 mois' },
});

function subscriptionPlan(plan) {
  const raw = String(plan || '').trim().toLowerCase();
  if (SUBSCRIPTION_PLANS[raw]) return SUBSCRIPTION_PLANS[raw];

  // Accept the common values that may come from an older frontend build.
  // The canonical keys remain 1m, 6m and 12m.
  const aliases = {
    '1': '1m', '1month': '1m', '1 mois': '1m', '1mois': '1m', '2000': '1m',
    '6': '6m', '6month': '6m', '6 mois': '6m', '6mois': '6m', '10000': '6m',
    '12': '12m', '12month': '12m', '12 mois': '12m', '12mois': '12m', '18000': '12m'
  };
  const key = aliases[raw];
  return key ? SUBSCRIPTION_PLANS[key] : null;
}

function subscriptionBaseUntil(user) {
  const now = Date.now();
  let until = Math.max(Number(user?.subscriptionUntil || 0), now);
  if (Number(user?.trialUntil || 0) > until) until = Number(user.trialUntil);
  const queue = Array.isArray(user?.subscriptionQueue) ? user.subscriptionQueue : [];
  for (const item of queue) {
    if (item && item.status === 'PAID') until = Math.max(until, Number(item.endsAt || 0));
  }
  return until;
}

function hasPaidSubscription(user) {
  const now = Date.now();
  return Boolean(
    Number(user?.subscriptionUntil || 0) > now ||
    (Array.isArray(user?.subscriptionQueue) && user.subscriptionQueue.some(item => item && item.status === 'PAID'))
  );
}

// Payment helpers. Cartflox is server-side only; the secret key never reaches the browser.
function paymentEnabled() { return CARTFLOX_ENABLED; }
function paymentAmount() { return 0; }
const authLimiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 20, standardHeaders: true, legacyHeaders: false });
const AUTH_CODE_TTL_MS = 10 * 60 * 1000;
function normalizePhone(v) { return String(v || '').replace(/[^0-9+]/g, '').replace(/^00/, '+'); }
function generateAuthCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let out=''; for(let i=0;i<5;i++) out += chars[Math.floor(Math.random()*chars.length)];
  return out;
}
function generateWhatsAppAuthCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let out=''; for(let i=0;i<6;i++) out += chars[Math.floor(Math.random()*chars.length)];
  return `AUTH-${out}`;
}
function generateToken() { return crypto.randomBytes(32).toString('hex'); }
const ACCOUNT_LINK_TTL_MS = Number(process.env.ACCOUNT_LINK_TTL_DAYS || 365) * 24 * 60 * 60 * 1000;
function hashAccountLinkToken(token) {
  return crypto.createHash('sha256').update(String(token)).digest('hex');
}
function getPublicSiteUrl(req) {
  // The WhatsApp authentication handler does not have the original browser
  // request, so the personal link must come from a stable public URL.
  const configuredRaw = String(
    process.env.PUBLIC_SITE_URL ||
    process.env.APP_URL ||
    (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : '') ||
    (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : '') ||
    (process.env.WEBSITE_HOSTNAME ? `https://${process.env.WEBSITE_HOSTNAME}` : '') ||
    ''
  ).trim().replace(/\/$/, '');

  // Never generate a broken host such as `http:///#account=...`.
  // A configured public URL must contain a real hostname.
  if (configuredRaw) {
    try {
      const parsed = new URL(configuredRaw);
      if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
        return configuredRaw;
      }
    } catch (_) {
      console.warn(`Invalid PUBLIC_SITE_URL/APP_URL ignored: ${configuredRaw}`);
    }
  }

  const forwardedProto = String(req?.headers?.['x-forwarded-proto'] || '').split(',')[0].trim();
  const protocol = forwardedProto || req?.protocol || 'http';
  let host = req?.get ? req.get('host') : '';
  host = String(host || process.env.PUBLIC_HOST || `localhost:${PORT}`).trim();
  if (!host) host = `localhost:${PORT}`;
  return `${protocol}://${host}`;
}
function generateAccountLinkSlug() {
  // Short opaque identifier used only to start the verification flow.
  // It is NOT an authenticated session and is never exposed after the redirect.
  return crypto.randomBytes(7).toString('base64url');
}

function createPersonalAccountLink(phone, req) {
  const cleanPhone = normalizePhone(phone);
  const links = readAccountLinks();
  const now = Date.now();

  // One stable short identifier per WhatsApp number. The identifier only starts
  // a 10-minute WhatsApp verification attempt; it never authenticates by itself.
  let existingHash = null;
  let existingEntry = null;
  for (const [hash, entry] of Object.entries(links)) {
    if (entry && normalizePhone(entry.phone) === cleanPhone) {
      existingHash = hash;
      existingEntry = entry;
      break;
    }
  }

  if (!existingEntry) {
    const slug = generateAccountLinkSlug();
    const hash = hashAccountLinkToken(crypto.randomBytes(32).toString('base64url'));
    links[hash] = {
      phone: cleanPhone,
      slug,
      createdAt: now,
      expiresAt: 0,
    };
    writeAccountLinks(links);
    return `${getPublicSiteUrl(req)}/a/${encodeURIComponent(slug)}`;
  }

  // Migrate every previous personal-link record to a stable short slug.
  if (!existingEntry.slug) existingEntry.slug = generateAccountLinkSlug();
  // Personal account links never expire. Only the verification attempt expires.
  existingEntry.expiresAt = 0;
  existingEntry.phone = cleanPhone;
  links[existingHash] = existingEntry;
  writeAccountLinks(links);

  return `${getPublicSiteUrl(req)}/a/${encodeURIComponent(existingEntry.slug)}`;
}

function getUserFromPersonalAccountSlug(slug) {
  const clean = String(slug || '').trim();
  if (!/^[A-Za-z0-9_-]{6,20}$/.test(clean)) return null;
  const links = readAccountLinks();
  for (const entry of Object.values(links)) {
    if (!entry || String(entry.slug || '') !== clean) continue;
    const users = readUsers();
    return users[normalizePhone(entry.phone)]
      ? { phone: normalizePhone(entry.phone), ...users[normalizePhone(entry.phone)] }
      : null;
  }
  return null;
}

function getUserFromPersonalAccountLink(token) {
  const clean = String(token || '').trim();
  if (!clean) return null;
  const hash = hashAccountLinkToken(clean);
  const links = readAccountLinks();
  const entry = links[hash];

  // BACKWARD COMPATIBILITY: links generated by the older version were
  // deterministic HMAC tokens. If that old hash was migrated/replaced by a
  // newer random token, the original URL must still be accepted as long as
  // the same AUTH_SECRET is still configured. This prevents an already-issued
  // personal link from suddenly becoming unusable after a server update.
  if (!entry) {
    const secret = process.env.AUTH_SECRET || process.env.VISITOR_SECRET || 'change-this-account-link-secret';
    const users = readUsers();
    for (const [phone, legacyUser] of Object.entries(users)) {
      if (!legacyUser) continue;
      const legacyToken = crypto.createHmac('sha256', secret)
        .update(`POWERD-BY-TRESOR:ACCOUNT:${normalizePhone(phone)}`)
        .digest('base64url');
      const legacyHash = hashAccountLinkToken(legacyToken);
      if (legacyHash === hash && legacyToken === clean) {
        return { phone: normalizePhone(phone), ...legacyUser };
      }
    }
    return null;
  }

  if (Number(entry.expiresAt || 0) < Date.now()) {
    delete links[hash];
    writeAccountLinks(links);
    return null;
  }
  // For modern links, require the exact stored token as an additional check.
  // Legacy entries created before this patch have no `token`, so their hash is
  // still sufficient and remains backwards-compatible.
  if (entry.token) {
    const stored = Buffer.from(String(entry.token));
    const supplied = Buffer.from(clean);
    if (stored.length !== supplied.length || !crypto.timingSafeEqual(stored, supplied)) return null;
  }
  const users = readUsers();
  return users[entry.phone] ? { phone: entry.phone, ...users[entry.phone] } : null;
}
const SESSION_COOKIE = 'pbt_session';
const ACCOUNT_LINK_ATTEMPT_COOKIE = 'pbt_account_link_attempt';
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const ACCOUNT_LINK_ATTEMPT_TTL_MS = 10 * 60 * 1000;

function isHttpsRequest(req) {
  const proto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim().toLowerCase();
  return req.secure === true || proto === 'https';
}

function parseCookie(req, name) {
  const raw = String(req.headers.cookie || '').split(';').map(v => v.trim()).find(v => v.startsWith(`${name}=`));
  if (!raw) return '';
  try { return decodeURIComponent(raw.slice(name.length + 1)); } catch { return ''; }
}

function setSessionCookie(req, res, token) {
  const secure = isHttpsRequest(req);
  const cookie = `${SESSION_COOKIE}=${encodeURIComponent(token)}; Max-Age=${Math.floor(SESSION_TTL_MS/1000)}; Path=/; HttpOnly; SameSite=Lax${secure ? '; Secure' : ''}`;
  if (typeof res.append === 'function') res.append('Set-Cookie', cookie); else res.setHeader('Set-Cookie', cookie);
}

function clearSessionCookie(req, res) {
  const secure = isHttpsRequest(req);
  const cookie = `${SESSION_COOKIE}=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax${secure ? '; Secure' : ''}`;
  if (typeof res.append === 'function') res.append('Set-Cookie', cookie); else res.setHeader('Set-Cookie', cookie);
}

function setAccountLinkAttemptCookie(req, res, attemptId) {
  const secure = isHttpsRequest(req);
  const cookie = `${ACCOUNT_LINK_ATTEMPT_COOKIE}=${encodeURIComponent(attemptId)}; Max-Age=${Math.floor(ACCOUNT_LINK_ATTEMPT_TTL_MS/1000)}; Path=/; HttpOnly; SameSite=Lax${secure ? '; Secure' : ''}`;
  if (typeof res.append === 'function') res.append('Set-Cookie', cookie); else res.setHeader('Set-Cookie', cookie);
}

function clearAccountLinkAttemptCookie(req, res) {
  const secure = isHttpsRequest(req);
  const cookie = `${ACCOUNT_LINK_ATTEMPT_COOKIE}=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax${secure ? '; Secure' : ''}`;
  if (typeof res.append === 'function') res.append('Set-Cookie', cookie); else res.setHeader('Set-Cookie', cookie);
}

function getPendingAccountLinkAttempt(req) {
  const attemptId = parseCookie(req, ACCOUNT_LINK_ATTEMPT_COOKIE);
  if (!attemptId) return null;
  const attempts = readAccountLinkAttempts();
  const attempt = attempts[attemptId];
  if (!attempt || Number(attempt.expiresAt || 0) < Date.now()) {
    if (attempt) { delete attempts[attemptId]; writeAccountLinkAttempts(attempts); }
    return null;
  }
  return { id: attemptId, ...attempt };
}

function createSessionForUser(req, res, phone) {
  const sessionToken = generateToken();
  const sessions = readAuthSessions();
  sessions[sessionToken] = { phone, createdAt: Date.now(), expiresAt: Date.now() + SESSION_TTL_MS };
  writeAuthSessions(sessions);
  setSessionCookie(req, res, sessionToken);
  return sessionToken;
}

function getAuthUser(req) {
  const cookieToken = parseCookie(req, SESSION_COOKIE);
  const auth = String(req.headers.authorization || '');
  const bearerToken = auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
  const token = cookieToken || bearerToken;
  if (!token) return null;
  const sessions = readAuthSessions(); const session = sessions[token];
  if (!session || session.expiresAt < Date.now()) { if (session) { delete sessions[token]; writeAuthSessions(sessions); } return null; }
  const users = readUsers(); return users[session.phone] ? { phone: session.phone, ...users[session.phone] } : null;
}
function syncSubscriptionQueue(user) {
  if (!user) return user;
  const now = Date.now();
  const queue = Array.isArray(user.subscriptionQueue) ? user.subscriptionQueue : [];
  let changed = false;
  let activeUntil = Number(user.subscriptionUntil || 0);

  for (const item of queue) {
    if (!item || item.status !== 'PAID') continue;
    const startsAt = Number(item.startsAt || 0);
    const endsAt = Number(item.endsAt || 0);
    if (startsAt > 0 && startsAt <= now && endsAt > activeUntil) {
      activeUntil = endsAt;
      changed = true;
    }
  }

  if (activeUntil !== Number(user.subscriptionUntil || 0)) {
    user.subscriptionUntil = activeUntil;
    user.subscriptionStartedAt = Number(user.subscriptionStartedAt || now);
    changed = true;
  }

  if (changed && user.phone) {
    const users = readUsers();
    users[normalizePhone(user.phone)] = user;
    writeUsers(users);
  }
  return user;
}

function accountAccess(user) {
  if (!user) return { authenticated:false, access:false, status:'SIGNED_OUT', hasPaidSubscription:false };
  user = syncSubscriptionQueue(user);
  const now=Date.now();
  const base = {
    authenticated:true,
    phone:user.phone || null,
    phoneDisplay:formatWhatsAppPhone(user.phone),
    userId:user.userId || null,
    hasPaidSubscription:hasPaidSubscription(user),
    queuedSubscriptions:Array.isArray(user.subscriptionQueue) ? user.subscriptionQueue.filter(x => x && x.status === 'PAID').map(x => ({ plan:x.plan, label:x.label, startsAt:x.startsAt, endsAt:x.endsAt })) : []
  };
  if (user.subscriptionUntil && user.subscriptionUntil > now) return { ...base, access:true, status:'SUBSCRIBED', until:user.subscriptionUntil };
  if (user.trialUntil && user.trialUntil > now) return { ...base, access:true, status:'TRIAL', until:user.trialUntil };
  // A paid subscription can be queued after an expired trial/account. It does
  // not grant access before its start date, but the UI still knows a paid plan exists.
  return { ...base, access:false, status:'EXPIRED' };
}

// ========================
// FIRST USE / VISITOR ACCESS
// ========================
// The first processing can happen without website login. A signed visitor id
// stored in a cookie makes that first-use rule server-side instead of relying
// on localStorage alone. After the first successful WhatsApp delivery, the
// visitor is linked to the WhatsApp phone number.
const VISITOR_COOKIE = 'pbt_visitor';
const VISITOR_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const VISITOR_SECRET = process.env.VISITOR_SECRET || process.env.AUTH_SECRET || 'change-this-visitor-secret';

function signVisitorId(id) {
  return crypto.createHmac('sha256', VISITOR_SECRET).update(String(id)).digest('hex');
}

function makeVisitorToken() {
  const id = crypto.randomBytes(18).toString('hex');
  return `${id}.${signVisitorId(id)}`;
}

function readVisitorToken(req) {
  const raw = String(req.headers.cookie || '').split(';').map(v => v.trim()).find(v => v.startsWith(`${VISITOR_COOKIE}=`));
  if (!raw) return null;
  const token = decodeURIComponent(raw.slice(VISITOR_COOKIE.length + 1));
  const dot = token.indexOf('.');
  if (dot <= 0) return null;
  const id = token.slice(0, dot);
  const sig = token.slice(dot + 1);
  const expected = signVisitorId(id);
  if (sig.length !== expected.length) return null;
  if (!crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected))) return null;
  return id;
}

function ensureVisitor(req, res) {
  let id = readVisitorToken(req);
  if (!id) {
    const token = makeVisitorToken();
    id = token.slice(0, token.indexOf('.'));
    const cookie = `${VISITOR_COOKIE}=${encodeURIComponent(token)}; Max-Age=${Math.floor(VISITOR_TTL_MS/1000)}; Path=/; HttpOnly; SameSite=Lax${isHttpsRequest(req) ? '; Secure' : ''}`;
    if (typeof res.append === 'function') res.append('Set-Cookie', cookie); else res.setHeader('Set-Cookie', cookie);
  }
  return id;
}

function visitorWasUsed(visitorId) {
  if (!visitorId) return false;
  const users = readUsers();
  return Object.values(users).some(u => u && u.visitorId === visitorId && u.firstUseCompletedAt);
}

function markFirstUseCompleted(visitorId, phone, userId) {
  if (!visitorId || !phone) return;
  const users = readUsers();
  const now = Date.now();
  const cleanPhone = normalizePhone(phone);
  const user = users[cleanPhone] || {
    phone: cleanPhone,
    createdAt: now,
    trialUntil: 0,
    subscriptionUntil: 0,
  };
  if (!user.firstUseCompletedAt) user.firstUseCompletedAt = now;
  // IMPORTANT: the 7-day trial starts only after the separate AUTH message
  // is sent by the user. First video delivery only links the visitor/phone.
  user.visitorId = visitorId;
  if (userId) user.userId = String(userId);
  user.lastWhatsAppUseAt = now;
  users[cleanPhone] = user;
  writeUsers(users);
  return user;
}

function activateTrialForAuth(visitorId, phone, userId) {
  if (!visitorId || !phone) return null;
  const users = readUsers();
  const now = Date.now();
  const cleanPhone = normalizePhone(phone);
  const cleanUserId = userId ? String(userId) : '';

  // Recover accounts created before the LID -> real-phone fix. Older records
  // could have the long LID stored as the phone key. If the same WhatsApp LID
  // is seen again, migrate that existing account to its real phone number so
  // the trial/subscription history is preserved.
  let existingKey = users[cleanPhone] ? cleanPhone : null;
  if (!existingKey && cleanUserId) {
    existingKey = Object.keys(users).find((key) =>
      // Older broken records may have the LID itself as the JSON key and no
      // separate userId field. Match both forms so the old account is migrated.
      String(key) === cleanUserId ||
      String(users[key]?.userId || '') === cleanUserId
    ) || null;
  }

  const user = existingKey
    ? users[existingKey]
    : {
        phone: cleanPhone,
        createdAt: now,
        trialUntil: 0,
        subscriptionUntil: 0,
      };

  if (existingKey && existingKey !== cleanPhone) delete users[existingKey];
  user.phone = cleanPhone;
  if (cleanUserId) user.userId = cleanUserId;
  user.visitorId = visitorId;
  user.authenticatedAt = now;
  // Never restart an existing trial/subscription. Only the first AUTH
  // activation can start the free trial.
  if (!user.subscriptionUntil && !user.trialUntil) {
    user.trialUntil = now + TRIAL_DAYS * 86400000;
    user.trialStartedAt = now;
  }

  users[cleanPhone] = user;
  writeUsers(users);
  return user;
}

function processingAccess(req, res) {
  // A valid authenticated session always takes priority. A stale account-link
  // attempt cookie must never block a user who is already logged in.
  const user = getAuthUser(req);
  if (user) {
    const access = accountAccess(user);
    if (!access.access) {
      return { ok:false, status:403, body:{ error:'Votre période d’essai est terminée. Abonnez-vous pour continuer.', subscriptionRequired:true, ...access } };
    }
    return { ok:true, user, access, firstUse:false };
  }

  const pendingLink = getPendingAccountLinkAttempt(req);
  if (pendingLink) {
    return { ok:false, status:401, body:{ error:'Vérification WhatsApp requise pour accéder à cet espace.', authenticationRequired:true } };
  }

  const visitorId = ensureVisitor(req, res);
  if (visitorWasUsed(visitorId)) {
    return { ok:false, status:401, body:{ error:'Connexion requise pour continuer.', authenticationRequired:true } };
  }

  return { ok:true, user:null, access:{ authenticated:false, access:true, status:'FIRST_USE' }, firstUse:true, visitorId };
}

app.post('/api/auth/request-code', authLimiter, (req,res) => {
  const phone=normalizePhone(req.body?.phone);
  if (!/^\+?[1-9]\d{7,14}$/.test(phone)) return res.status(400).json({error:'Numéro de téléphone invalide.'});
  const users=readUsers();
  // Legacy phone/code endpoint is kept only for compatibility. It MUST NOT
  // create or restart a free trial: the WhatsApp AUTH flow below is the only
  // place where the 7-day trial can ever start.
  if (!users[phone]) {
    users[phone]={ phone, createdAt:Date.now(), trialUntil:0, subscriptionUntil:0 };
  }
  users[phone].pendingCode=generateAuthCode(); users[phone].pendingCodeExpiresAt=Date.now()+AUTH_CODE_TTL_MS; writeUsers(users);
  // Development/local mode returns the code so the site can be tested without an SMS/WhatsApp provider.
  const response={ok:true, expiresIn:AUTH_CODE_TTL_MS/1000};
  if(AUTH_DEV_MODE) response.devCode=users[phone].pendingCode;
  res.json(response);
});

app.post('/api/auth/verify-code', authLimiter, (req,res) => {
  const phone=normalizePhone(req.body?.phone), code=String(req.body?.code||'').trim().toUpperCase();
  const users=readUsers(); const user=users[phone];
  if(!user || !user.pendingCode || user.pendingCodeExpiresAt<Date.now() || user.pendingCode!==code) return res.status(401).json({error:'Code invalide ou expiré.'});
  delete user.pendingCode; delete user.pendingCodeExpiresAt; writeUsers(users);
  const token=createSessionForUser(req,res,phone);
  res.json({ok:true,user:{phone},...accountAccess(user)});
});

app.get('/api/auth/me',(req,res)=>{
  const user=getAuthUser(req);
  if (user) return res.json({ phone:user.phone, ...accountAccess(user) });

  const pendingLink = getPendingAccountLinkAttempt(req);
  if (pendingLink) {
    return res.json({ authenticated:false, access:false, status:'ACCOUNT_LINK_VERIFICATION_REQUIRED', authenticationRequired:true });
  }

  const visitorId = ensureVisitor(req, res);
  if (visitorWasUsed(visitorId)) {
    return res.json({ authenticated:false, access:false, status:'AUTH_REQUIRED', authenticationRequired:true });
  }

  return res.json({ authenticated:false, access:true, status:'FIRST_USE' });
});

// Prepare the silent WhatsApp authentication flow. No phone number is entered
// on the website. The backend generates a short AUTH code and opens our relay
// WhatsApp number with that code already written in the message.
app.post('/api/auth/prepare', authLimiter, (req,res) => {
  const pendingLink = getPendingAccountLinkAttempt(req);
  const visitorId = pendingLink?.visitorId || ensureVisitor(req, res);
  const code = generateWhatsAppAuthCode();
  const challengeId = crypto.randomBytes(24).toString('hex');
  const challenges = readAuthChallenges();
  const now = Date.now();

  // Remove stale challenges while we are here.
  for (const [id, c] of Object.entries(challenges)) {
    if (!c || Number(c.expiresAt || 0) < now) delete challenges[id];
  }

  const pollToken = crypto.randomBytes(24).toString('hex');

  challenges[challengeId] = {
    code,
    pollToken,
    visitorId,
    createdAt: now,
    expiresAt: now + AUTH_CODE_TTL_MS,
    status: 'pending',
    expectedPhone: pendingLink?.phone || null,
    accountLinkAttemptId: pendingLink?.id || null,
    phone: null,
    sessionToken: null,
  };
  writeAuthChallenges(challenges);

  const relayNumber = String(process.env.WHATSAPP_BUSINESS_NUMBER || '').replace(/\D/g, '');
  if (!relayNumber) return res.status(503).json({ error:'Le numéro WhatsApp relais n’est pas configuré.' });

  const waMessage = `LE CODE : ${code}\n\nNE PAS MODIFIER CE MESSAGE AVANT DE L'ENVOYER.`;
  const waLink = `https://wa.me/${relayNumber}?text=${encodeURIComponent(waMessage)}`;

  res.json({ ok:true, challengeId, pollToken, waLink, expiresIn:AUTH_CODE_TTL_MS/1000 });
});

app.get('/api/auth/challenge-status',(req,res)=>{
  const challengeId = String(req.query.challenge || '').trim();
  const challenges = readAuthChallenges();
  const challenge = challenges[challengeId];
  if (!challenge || Number(challenge.expiresAt || 0) < Date.now()) {
    if (challenge) { delete challenges[challengeId]; writeAuthChallenges(challenges); }
    return res.status(404).json({ error:'Demande d’authentification expirée.' });
  }

  const pollToken = String(req.query.pollToken || '').trim();
  if (!pollToken || pollToken !== challenge.pollToken) {
    return res.status(403).json({ error:'Demande d’authentification invalide.' });
  }

  if (challenge.status !== 'verified') return res.json({ status:'pending' });

  const users = readUsers();
  const user = challenge.phone ? users[challenge.phone] : null;
  if (!user) return res.json({ status:'pending' });

  // Finalize the browser session only after the polling browser proves the
  // WhatsApp verification succeeded.
  createSessionForUser(req,res,user.phone);
  if (challenge.accountLinkAttemptId) {
    const attempts = readAccountLinkAttempts();
    delete attempts[challenge.accountLinkAttemptId];
    writeAccountLinkAttempts(attempts);
    clearAccountLinkAttemptCookie(req,res);
  }

  return res.json({
    status:'verified',
    phone:user.phone,
    ...accountAccess(user),
  });
});

// Exchange the personal account link for a normal authenticated session.
// The slug is a random, stable bearer credential unique to the WhatsApp account.
// It is NOT the session token and changing one character makes it invalid.
// Server-side trial/subscription checks still apply after the session is created.
app.get('/a/:slug', authLimiter, (req,res) => {
  const slug = String(req.params.slug || '').trim();
  const user = getUserFromPersonalAccountSlug(slug);
  if (!user) return res.status(404).send('Lien personnel invalide.');

  createSessionForUser(req, res, user.phone);
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
  return res.redirect(302, '/');
});

// Backward-compatible API endpoint for old #account= links.
app.get('/api/account-link/exchange', authLimiter, (req,res)=>{
  const token = String(req.query.token || '').trim();
  const user = getUserFromPersonalAccountLink(token);
  if (!user) return res.status(401).json({ error:'Lien personnel invalide ou expiré.' });

  const sessionToken = createSessionForUser(req, res, user.phone);
  return res.json({ ok:true, token:sessionToken, phone:user.phone, ...accountAccess(user) });
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
  res.json({ ok:true, requiresWhatsApp:true, ...accountAccess(user) });
});

app.post('/api/auth/logout', (req,res)=>{
  const token = parseCookie(req, SESSION_COOKIE);
  const auth = String(req.headers.authorization || '');
  const bearer = auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
  const sessionToken = token || bearer;
  if (sessionToken) {
    const sessions = readAuthSessions();
    if (sessions[sessionToken]) { delete sessions[sessionToken]; writeAuthSessions(sessions); }
  }
  clearSessionCookie(req,res);
  clearAccountLinkAttemptCookie(req,res);
  res.set('Cache-Control','no-store');
  res.json({ ok:true });
});

app.post('/api/subscription/softpay/order', authLimiter, (req,res)=>{
  const user=getAuthUser(req);
  if(!user) return res.status(401).json({error:'Connexion requise.', authenticationRequired:true});
  if(!CARTFLOX_ENABLED) {
    return res.status(503).json({error:'Le paiement SoftPay n’est pas encore configuré sur le serveur.'});
  }

  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const requestedPlan = String(
    body.plan || body.planId || body.subscriptionPlan || body.offer || body.id || ''
  ).trim();
  const amountHint = Number(body.amount || 0);
  const planKey = requestedPlan || (amountHint === 2000 ? '1m' : amountHint === 10000 ? '6m' : amountHint === 18000 ? '12m' : '');
  const plan = subscriptionPlan(planKey);
  if(!plan) return res.status(400).json({error:'Offre d’abonnement invalide.', receivedPlan: requestedPlan || null});
  const canonicalPlanKey = Object.keys(SUBSCRIPTION_PLANS).find(key => SUBSCRIPTION_PLANS[key] === plan) || planKey;

  const orders = readSubscriptionOrders();
  const orderId = `MIABE-${Date.now().toString(36).toUpperCase()}-${crypto.randomBytes(4).toString('hex').toUpperCase()}`;
  const order = {
    id: orderId,
    phone: normalizePhone(user.phone),
    plan: canonicalPlanKey,
    label: plan.label,
    amount: plan.amount,
    days: plan.days,
    status: 'PENDING',
    paymentMode: 'SOFTPAY',
    createdAt: Date.now(),
  };

  orders[orderId] = order;
  writeSubscriptionOrders(orders);

  return res.json({
    ok:true,
    orderId,
    amount:plan.amount,
    currency:'XOF',
    plan:canonicalPlanKey,
    planLabel:plan.label,
    publicKey:CARTFLOX_PUBLIC_KEY,
    country:'tg'
  });
});

app.post('/api/subscription/momo/init', authLimiter, async (req,res)=>{
  const user=getAuthUser(req);
  if(!user) return res.status(401).json({error:'Connexion requise.', authenticationRequired:true});
  if(!CARTFLOX_ENABLED) return res.status(503).json({error:'Le paiement n’est pas encore configuré sur le serveur.'});

  const planKey = String(req.body?.plan || '').trim();
  const plan = subscriptionPlan(planKey);
  if(!plan) return res.status(400).json({error:'Offre d’abonnement invalide.'});

  const orders = readSubscriptionOrders();
  const orderId = `MIABE-${Date.now().toString(36).toUpperCase()}-${crypto.randomBytes(4).toString('hex').toUpperCase()}`;
  const baseUrl = PUBLIC_SITE_URL || getPublicSiteUrl(req);
  const order = {
    id: orderId,
    phone: normalizePhone(user.phone),
    plan: planKey,
    label: plan.label,
    amount: plan.amount,
    days: plan.days,
    status: 'PENDING',
    createdAt: Date.now(),
  };
  orders[orderId] = order;
  writeSubscriptionOrders(orders);

  try {
    const providerResponse = await axios.post(`${CARTFLOX_API_URL}/v1/checkout/sessions`, {
      amount: plan.amount,
      currency: 'XOF',
      customer_phone: normalizePhone(user.phone),
      description: `MIABE STATUS HD — ${plan.label}`,
      success_url: `${baseUrl}/?payment=success&order=${encodeURIComponent(orderId)}`,
      cancel_url: `${baseUrl}/?payment=cancelled&order=${encodeURIComponent(orderId)}`,
      metadata: {
        order_id: orderId,
        phone: normalizePhone(user.phone),
        plan: planKey,
        days: plan.days,
      },
      merchant_name: 'MIABE STATUS HD',
    }, {
      headers: {
        Authorization: `Bearer ${CARTFLOX_SECRET_KEY}`,
        'Content-Type': 'application/json',
        'Idempotency-Key': orderId,
      },
      timeout: 15000,
      validateStatus: () => true,
    });

    const data = providerResponse.data || {};
    if (providerResponse.status < 200 || providerResponse.status >= 300 || !data.url) {
      delete orders[orderId];
      writeSubscriptionOrders(orders);
      return res.status(502).json({error:data.error || 'Impossible de créer la session de paiement.'});
    }

    order.providerSessionId = data.id || null;
    order.providerOrderId = data.order_id || null;
    order.providerStatus = data.status || 'pending';
    orders[orderId] = order;
    writeSubscriptionOrders(orders);

    return res.json({ ok:true, paymentUrl:data.url, orderId, status:data.status || 'pending', plan:planKey, amount:plan.amount });
  } catch (error) {
    delete orders[orderId];
    writeSubscriptionOrders(orders);
    console.error('Cartflox session error:', error?.message || error);
    return res.status(502).json({error:'Impossible de contacter le service de paiement pour le moment.'});
  }
});

function verifyCartfloxWebhook(rawBody, headers, secret) {
  const timestamp = String(headers['x-afriflow-timestamp'] || '').trim();
  const signatureHeader = String(headers['x-afriflow-signature'] || '');
  const signature = signatureHeader.split(',').find(part => part.trim().startsWith('v1='))?.trim().slice(3) || '';
  if (!timestamp || !signature || !rawBody || !secret) return false;
  const ts = Number(timestamp);
  if (!Number.isFinite(ts) || Math.abs(Date.now() / 1000 - ts) > 300) return false;
  const expected = crypto.createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest('hex');
  if (expected.length !== signature.length) return false;
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature));
}

function applyPaidSubscription(order) {
  const users = readUsers();
  const phone = normalizePhone(order.phone);
  const user = users[phone];
  if (!user) return false;

  const now = Date.now();
  const queue = Array.isArray(user.subscriptionQueue) ? user.subscriptionQueue : [];
  let startAt = Math.max(now, Number(user.subscriptionUntil || 0), Number(user.trialUntil || 0));
  for (const item of queue) {
    if (item && item.status === 'PAID') startAt = Math.max(startAt, Number(item.endsAt || 0));
  }
  const endsAt = startAt + Number(order.days) * 86400000;

  const existing = queue.find(item => item && item.orderId === order.id);
  if (!existing) {
    queue.push({ orderId:order.id, plan:order.plan, label:order.label, amount:order.amount, days:order.days, status:'PAID', paidAt:now, startsAt:startAt, endsAt });
  }
  user.subscriptionQueue = queue;

  // If payment starts immediately, activate it now. If trial/current premium is
  // still active, leave it queued so the current period is never shortened.
  if (startAt <= now) {
    user.subscriptionUntil = Math.max(Number(user.subscriptionUntil || 0), endsAt);
    user.subscriptionStartedAt = Number(user.subscriptionStartedAt || now);
  }

  users[phone] = user;
  writeUsers(users);
  return true;
}

app.post('/api/subscription/momo/webhook', (req,res)=>{
  const rawBody = Buffer.isBuffer(req.rawBody) ? req.rawBody.toString('utf8') : '';
  if (!verifyCartfloxWebhook(rawBody, req.headers, CARTFLOX_SECRET_KEY)) {
    return res.status(400).send('signature invalide');
  }

  let event;
  try { event = JSON.parse(rawBody); } catch (_) { return res.status(400).send('payload invalide'); }

  if (event.event === 'payment.completed') {
    const data = event.data || {};
    const metadata = data.metadata || {};
    const orderId = String(metadata.order_id || '').trim();
    if (orderId) {
      const orders = readSubscriptionOrders();
      const order = orders[orderId];
      if (order && order.status !== 'PAID') {
        const receivedAmount = Number(data.amount ?? event.amount ?? order.amount);
        if (receivedAmount !== Number(order.amount)) {
          console.warn(`Cartflox amount mismatch for ${orderId}: expected ${order.amount}, received ${receivedAmount}`);
        } else {
          order.status = 'PAID';
          order.paidAt = Date.now();
          order.providerStatus = data.status || 'completed';
          orders[orderId] = order;
          writeSubscriptionOrders(orders);
          applyPaidSubscription(order);
        }
      }
    }
  }

  return res.sendStatus(200);
});


if (!ffmpegStatic) {
  console.error('ffmpeg-static path is null!');
  process.exit(1);
}

console.log('FFmpeg path:', ffmpegStatic);
console.log('FFprobe path:', ffprobePath);
ffmpeg.setFfmpegPath(ffmpegStatic);
ffmpeg.setFfprobePath(ffprobePath);

// ========================
// MIDDLEWARE
// ========================
const allowedOrigins = new Set([
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  // Local-network testing from a phone/tablet. Keep this explicit rather than
  // using '*' so credentialed requests remain restricted to known origins.
  'http://172.20.10.3:3000',
  ...(process.env.FRONTEND_ORIGIN ? process.env.FRONTEND_ORIGIN.split(',').map(s => s.trim()).filter(Boolean) : []),
]);

app.use(cors({
  origin: (origin, callback) => {
    if (!origin || allowedOrigins.has(origin)) return callback(null, true);
    return callback(new Error('CORS origin not allowed'));
  },
  methods: ['GET', 'POST'],
  credentials: true
}));
app.use(express.static('public'));
// Serve Bootstrap Icons fonts/CSS for the in-app editor controls.
app.use('/vendor/bootstrap-icons', express.static(path.join(__dirname, 'node_modules/bootstrap-icons/font')));
// Serve @fontsource font CSS + files for the editor's text font picker.
app.use('/vendor/fonts', express.static(path.join(__dirname, 'node_modules/@fontsource')));
// Serve pako (gunzip .tgs) + lottie-web (render animated stickers) for the editor.
app.use('/vendor/pako', express.static(path.join(__dirname, 'node_modules/pako/dist')));
app.use('/vendor/lottie', express.static(path.join(__dirname, 'node_modules/lottie-web/build/player')));

// Mount the music / asset endpoints: GET /api/library (curated royalty-free
// library) and POST /api/music/upload-url + POST /api/music/validate (user
// audio upload path). No streaming-rip/import surface is exposed (Req 8.4).
musicRoutes.register(app);
app.use((req, res, next) => {
  req.on('aborted', () => {
    console.warn(`Request aborted by client: ${req.path}`);
  });
  next();
});

// ========================
// FOLDERS
// ========================
if (!fs.existsSync('uploads')) fs.mkdirSync('uploads', { recursive: true });
if (!fs.existsSync('compressed')) fs.mkdirSync('compressed', { recursive: true });
// Persistent cache for animated .tgs stickers rendered to .webp (Stage 3), so
// built-in stickers are rendered once and reused across requests.
if (!fs.existsSync('tgs-cache')) fs.mkdirSync('tgs-cache', { recursive: true });

// ========================
// LOCAL OBJECT STORAGE (R2-free)
// ========================
const OBJECT_ROOT = path.resolve(process.env.LOCAL_STORAGE_DIR || 'storage/objects');
fs.mkdirSync(OBJECT_ROOT, { recursive: true });

function safeObjectPath(key) {
  const clean = String(key || '').replace(/\\/g, '/').replace(/^\/+/, '');
  const resolved = path.resolve(OBJECT_ROOT, clean);
  if (resolved !== OBJECT_ROOT && !resolved.startsWith(OBJECT_ROOT + path.sep)) {
    throw new Error('Invalid storage key');
  }
  return resolved;
}

function objectUrl(key) {
  return `/api/object/${encodeURIComponent(String(key)).replace(/%2F/g, '/')}`;
}

// Local equivalent of the old R2 cleanup wiring. The rest of the processing
// pipeline keeps its object-key ledger names, but objects live on local disk.
const cleanupDeps = makeLocalObjectDeps({ root: OBJECT_ROOT, fs, logger: console });

/**
 * Collect the R2 object keys of UPLOADED Music_Track / Sticker assets referenced
 * by a recipe. These are non-output assets baked into the single encode, so they
 * are safe to purge once rendering is done; they are registered in the request
 * ledger (cleaned in the request `finally`) AND carried on the session so
 * delivery-time cleanup re-attempts their removal within 60s of delivery (Req
 * 14.2/14.4). Library (server-owned) assets are not uploaded and carry no key,
 * so they are never deleted.
 *
 * @param {Object|null} recipe
 * @returns {string[]}
 */
function collectAssetR2Keys(recipe) {
  const keys = [];
  if (!recipe) return keys;
  const music = recipe.audio && recipe.audio.music;
  if (music) {
    if (music.key) keys.push(String(music.key));
    else if (music.assetRef) keys.push(`music/${music.assetRef}`);
  }
  const stickers = Array.isArray(recipe.stickers) ? recipe.stickers : [];
  for (const sticker of stickers) {
    // Only uploaded stickers carry an R2 key; library stickers are server-owned.
    if (sticker && sticker.key) keys.push(String(sticker.key));
  }
  return keys;
}

// ========================
// SESSION STORAGE
// ========================
const sessions = new Map();
// Ledger of requests currently being processed; used by startup cleanup.
const activeRequests = new Set();
const recentlySentCodes = new Set();
// A send normally finishes in seconds; if a code has been "processing" for
// longer than this, the attempt is considered wedged and a new webhook may
// re-attempt delivery (kept above the per-send timeouts to avoid double-sends).
const PROCESSING_STALE_MS = 300000;

setInterval(async () => {
  const now = Date.now();
  for (const [code, session] of sessions.entries()) {
    if (now - session.createdAt > 300000) {
      for (const file of session.files) {
        try {
          await deleteFromR2(file.fileName);
          console.log(`R2 cleanup: ${file.fileName} deleted`);
        } catch (err) {
          console.error('R2 cleanup error:', err.message);
        }
      }
      sessions.delete(code);
      console.log(`Session expired & R2 cleaned: ${code}`);
    }
  }
}, 60000);

// =======================
// RATE LIMITING
// =======================
// Azure App Service's X-Forwarded-For includes the client PORT
// (e.g. "1.2.3.4:56789"), which express-rate-limit rejects as an invalid IP
// (ERR_ERL_INVALID_IP_ADDRESS) — breaking correct per-IP keying. Normalize to a
// clean IP here so requests are rate-limited by address, not address+port.
function clientIpKey(req) {
  let ip = (req.ip || req.socket?.remoteAddress || 'unknown').trim();
  const v6 = ip.match(/^\[(.+)\]:\d+$/);            // "[::1]:5678" -> "::1"
  if (v6) ip = v6[1];
  else if (/^\d{1,3}(?:\.\d{1,3}){3}:\d+$/.test(ip)) { // "1.2.3.4:5678" -> "1.2.3.4"
    ip = ip.slice(0, ip.lastIndexOf(':'));
  }
  // Run through the library's helper so IPv6 addresses are keyed by subnet
  // (prevents IPv6 users bypassing limits by rotating within their block).
  return ipKeyGenerator ? ipKeyGenerator(ip) : ip;
}

const limiter = rateLimit({
  windowMs: 24 * 60 * 60 * 1000,
  max: 50,
  message: { error: 'Daily limit reached! Try again tomorrow.' },
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => req.path === '/api/health',
  keyGenerator: clientIpKey,
  // We normalize the IP ourselves (Azure appends a port), so disable the
  // library's built-in IP validation that would otherwise error on it.
  validate: { ip: false },
});

// ========================
// HELPER FUNCTIONS
// ========================
function generateCode() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let code = '';
  for (let i = 0; i < 9; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  // Guarantee at least one digit. This lets the inbound parser pick the code out
  // of a natural sentence without ever matching a plain 9-letter English word.
  if (!/[0-9]/.test(code)) {
    const pos = Math.floor(Math.random() * 9);
    code = code.slice(0, pos) + Math.floor(Math.random() * 10) + code.slice(pos + 1);
  }
  return code;
}

function getVideoDuration(filePath, durationHint = null) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error('ffprobe timeout after 30s'));
    }, 30000);

    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (settled) return;
      clearTimeout(timer);

      if (err) {
        settled = true;
        return reject(err);
      }

      // WebM files created by MediaRecorder (especially photo -> video)
      // can have no duration in format.duration. Try the video stream too.
      const formatDuration = Number(metadata?.format?.duration);
      const videoStream = Array.isArray(metadata?.streams)
        ? metadata.streams.find(s => s && s.codec_type === 'video')
        : null;
      const streamDuration = Number(videoStream?.duration);

      let duration = Number.isFinite(formatDuration) && formatDuration > 0
        ? formatDuration
        : (Number.isFinite(streamDuration) && streamDuration > 0 ? streamDuration : NaN);

      // MediaRecorder WebM files created by the photo tool can contain a bogus
      // ffprobe duration (commonly around 2s). The frontend sends an explicit
      // durationHint based on the selected duration; trust that hint for these
      // generated files instead of the broken container metadata.
      if (Number.isFinite(Number(durationHint)) && Number(durationHint) > 0) {
        duration = Number(durationHint);
      } else if (!Number.isFinite(duration) || duration <= 0) {
        const baseName = path.basename(filePath);
        const match = baseName.match(/POWERD-BY-TRESOR-(\d+(?:\.\d+)?)s\.webm$/i);
        if (match) duration = Number(match[1]);
      }

      if (!Number.isFinite(duration) || duration <= 0) {
        settled = true;
        return reject(new Error('Impossible de déterminer la durée de la vidéo. Le fichier WebM ne contient pas de durée exploitable.'));
      }

      settled = true;
      console.log(`Video duration: ${duration} seconds`);
      resolve(duration);
    });
  });
}

function getVideoDimensions(filePath) {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) return reject(err);
      const v = metadata.streams.find(s => s.codec_type === 'video');
      const hasAudio = metadata.streams.some(s => s.codec_type === 'audio');
      resolve({ width: v?.width || 1080, height: v?.height || 1920, hasAudio });
    });
  });
}

function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', (c) => hash.update(c));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

// Convert phone number to Baileys JID
function toJid(numberOrJid) {
  if (!numberOrJid) return null;
  if (numberOrJid.includes('@')) return numberOrJid;
  return `${numberOrJid.replace(/^\+/, '').replace(/\D/g, '')}@s.whatsapp.net`;
}

// Extract phone number from JID
function jidToNumber(jid) {
  // PN JIDs returned by Baileys' LID mapping can be device-scoped
  // (e.g. 22891597856:0@s.whatsapp.net). The :device suffix is not part of
  // the customer's phone number.
  return String(jid || '').split('@')[0].split(':')[0];
}

// WhatsApp can identify a sender with a LID (@lid). In that case the real
// phone number may be exposed as remoteJidAlt or may only be available through
// Baileys' persisted LID mapping. The phone number is the account identity;
// the LID remains the long WhatsApp user ID shown separately in the account.
async function getWhatsAppIdentity(message, sock) {
  const jid = message?.key?.remoteJid || '';
  // senderPn is the direct-message phone-number field used by newer Baileys
  // builds. Prefer it before the generic alternate fields when WhatsApp provides it.
  const alt = message?.key?.senderPn
    || message?.key?.remoteJidAlt
    || message?.key?.participantPn
    || message?.key?.participantAlt
    || '';
  const isLid = jid.endsWith('@lid') || jid.includes('@hosted.lid');

  let phoneJid = (!isLid || alt.endsWith('@s.whatsapp.net') || alt.endsWith('@hosted')) ? alt || jid : '';

  // Prefer the explicit phone JID supplied by the message envelope.
  if (isLid && !phoneJid) {
    try {
      const mapped = await sock?.signalRepository?.lidMapping?.getPNForLID(jid);
      if (mapped) phoneJid = mapped;
    } catch (err) {
      console.warn(`Could not resolve WhatsApp LID ${jid}: ${err.message}`);
    }
  }

  // Some WhatsApp/Baileys envelopes expose the LID -> PN mapping only after
  // processing the message. Try the LID without a device suffix as a second
  // lookup because mapping stores can normalize that form differently.
  if (isLid && !phoneJid && jid.includes(':')) {
    try {
      const bareLid = `${jid.split('@')[0].split(':')[0]}@lid`;
      const mapped = await sock?.signalRepository?.lidMapping?.getPNForLID(bareLid);
      if (mapped) phoneJid = mapped;
    } catch (err) {
      console.warn(`Could not resolve bare WhatsApp LID ${jid}: ${err.message}`);
    }
  }

  // If the mapping is returned as a device-scoped JID, jidToNumber() strips
  // the :device suffix before normalizing the actual phone number.
  const phone = normalizePhone(jidToNumber(phoneJid));
  const userId = isLid ? jidToNumber(jid) : (jidToNumber(jid) || phone.replace(/^\+/, ''));
  return { jid, phone, userId };
}

function formatWhatsAppPhone(phone) {
  const clean = normalizePhone(phone);
  if (!clean) return '—';
  // Togo: +228 91 59 78 56. For other countries, preserve the full E.164
  // number instead of guessing a country-specific grouping.
  if (/^\+228\d{8}$/.test(clean)) {
    return clean.replace(/^(\+228)(\d{2})(\d{2})(\d{2})(\d{2})$/, '$1 $2 $3 $4 $5');
  }
  return clean;
}



// Inbound guards (see src/server/inboundGuards.js for the full rationale).
// These exist because one customer received the welcome reply eight times in a
// minute: history replays looked live, duplicate deliveries were answered twice,
// and there was no per-sender throttle at all.
const messageDeduper = new MessageDeduper();
const welcomeThrottle = new WelcomeThrottle();

// Keep the throttle map from growing unbounded on a long-running process.
setInterval(() => welcomeThrottle.prune(), 10 * 60 * 1000).unref?.();

// ========================
// FFMPEG OPTIONS
// ========================
// Output frame rate handed to ffmpeg's -r. Capping at ~30fps keeps WhatsApp from
// re-encoding the clip at the Status step — it does not pass 60fps through.
// Set FPS_CAP=off to remove the flag and let the source frame rate survive.
const FPS_CAP = process.env.FPS_CAP || '29.97';

// Is a bare `ffmpeg` resolvable on PATH? Baileys needs this (it shells out to
// `ffmpeg` for video thumbnails + metadata); our own encoding uses the
// ffmpeg-static absolute path and does NOT depend on it. Cached after first call.
let _ffmpegOnPath = null;
function ffmpegOnPath() {
  if (_ffmpegOnPath !== null) return _ffmpegOnPath;
  try {
    require('child_process').execFileSync('ffmpeg', ['-version'], {
      stdio: 'ignore',
      timeout: 5000,
    });
    _ffmpegOnPath = true;
  } catch (_) {
    _ffmpegOnPath = false;
  }
  return _ffmpegOnPath;
}

function getOutputOptions(duration, inputHeight = 1920, attempt = 0, enc = {}) {
  console.log(`✓ getOutputOptions called!`);
  // Encode target — defaults reproduce the exact WhatsApp_Spec 1080×1920 output
  // (byte-identical to the legacy path). The optional 60s mode overrides these
  // with a 720×1280 / lower-bitrate profile so a 60s clip still fits under 16MB.
  const W = enc.width || 1080;
  const H = enc.height || 1920;
  const crf = enc.crf != null ? enc.crf : 23;
  const maxrateK = enc.maxrateK != null ? enc.maxrateK : 3800;
  const bufsizeK = enc.bufsizeK != null ? enc.bufsizeK : 5700;
  let vfFilter = `scale=w=${W}:h=${H}:force_original_aspect_ratio=decrease,pad=${W}:${H}:(ow-iw)/2:(oh-ih)/2,scale=trunc(iw/2)*2:trunc(ih/2)*2`;
  const base = [
    '-vf', vfFilter,
    '-c:v', 'libx264',
    '-pix_fmt', 'yuv420p',
    '-color_range', 'tv',
    '-color_primaries', 'bt470bg',
    '-color_trc', 'bt709',
    '-colorspace', 'bt470bg',
    '-crf', String(crf),
    '-maxrate', maxrateK + 'k',
    '-bufsize', bufsizeK + 'k',
    '-g', '250',
    '-profile:v', 'high',
    '-level:v', '4.0',
    '-x264-params', 'sei=0',
    // Frame-rate cap. This was disabled in "fps boosting" (2bbca92) to keep 60fps
    // smoothness, but that removed the guard the earlier comment warned about:
    // WhatsApp struggles with 60 FPS statuses and re-encodes them at the Status
    // step, which destroys the passthrough this whole spec exists to achieve.
    // Modern phones record 1080p60/4K60 by default, so most uploads were hitting it.
    // Set FPS_CAP=off to restore the uncapped behaviour for comparison.
    ...(FPS_CAP === 'off' ? [] : ['-r', FPS_CAP]),
    '-c:a', 'aac',
    '-ar', '44100',
    '-ac', '2',
    '-b:a', '128k',
    '-brand', 'isom',
    '-movflags', '+faststart',
    '-f', 'mp4',
    '-threads', '2',
  ];
  // Attempt 0 is the exact WhatsApp_Spec encode (byte-identical legacy output).
  // Retries (attempt > 0) progressively lower the bitrate so an over-size clip
  // actually shrinks instead of failing the "under 16MB" check forever.
  return tightenEncodeOptions(base, attempt);
}

// Encode profile for the optional "Longer clips — 60s per part" mode: 720×1280
// at ~2 Mbps. At this bitrate 720p looks cleaner than a starved 1080p, and a
// ~59s clip lands comfortably under the 16MB WhatsApp limit.
const LONG_CLIP_ENC = { width: 720, height: 1280, crf: 24, maxrateK: 2200, bufsizeK: 3300 };
const LONG_CLIP_SECONDS = 59; // safety margin under WhatsApp's ~60s ceiling

// ========================
// ENCODE EXECUTION (semaphore-gated, with timeouts + size retry)
// ========================
// The encode-execution helpers (runGatedEncode, buildRecipeCommand,
// encodeRecipePlan) and the timeout / retry constants live in
// ./src/server/encodeExec.js so they can be unit-tested deterministically with
// mocked ffmpeg, a mock semaphore, and a mock fs (no real encoding / real time).
// They are imported at the top of this file and used unchanged below.

async function splitVideo(inputPath, outputDir, duration, chunkDuration = 29, inputHeight = 1920, enc = {}) {
  if (!Number.isFinite(duration) || duration <= 0) {
    throw new Error(`Invalid video duration: ${duration}`);
  }
  const totalChunks = Math.ceil(duration / chunkDuration);
  console.log(`Splitting into ${totalChunks} chunks of ${chunkDuration}s each`);

  const chunks = [];
  for (let i = 0; i < totalChunks; i++) {
    const startTime = i * chunkDuration;
    const chunkFileName = `chunk_${uuidv4()}.mp4`;
    const chunkPath = path.join(outputDir, chunkFileName);
    chunks.push({ index: i, startTime, chunkPath });
  }

  // Semaphore-gated fan-out: every chunk encode acquires a permit from the
  // shared EncodeSemaphore, so simultaneous encodes never exceed the CPU-derived
  // Concurrency_Limit and the rest queue (Req 13.1/13.2). This replaces the old
  // hardcoded BATCH_SIZE ladder + per-batch Promise.all. The ffmpeg command per
  // chunk is unchanged, so skip-path chunk output stays byte-for-byte identical.
  const chunkPaths = await Promise.all(
    chunks.map(async ({ index, startTime, chunkPath }) => {
      let startAttempt = (chunkDuration >= 59) ? 1 : 0;
      for (let attempt = startAttempt; attempt < MAX_ENCODE_ATTEMPTS; attempt++) {
        await runGatedEncode(
          () => ffmpeg(inputPath)
            .setStartTime(startTime)
            .setDuration(chunkDuration)
            .outputOptions(getOutputOptions(chunkDuration, inputHeight, attempt, enc)),
          chunkPath,
          CHUNK_TIMEOUT_MS,
          `Chunk ${index + 1}`
        );

        const sizeMB = fs.statSync(chunkPath).size / (1024 * 1024);
        if (sizeMB <= RETRY_SIZE_MB) {
          console.log(`✓ Chunk ${index + 1}/${totalChunks} done! Size: ${sizeMB.toFixed(2)}MB`);
          return chunkPath;
        }
        console.log(`⚠️ Chunk ${index + 1} failed size check (${sizeMB.toFixed(2)}MB > 15.5MB). Retrying...`);
      }
      throw new Error(`Chunk ${index + 1} could not be compressed under 16MB.`);
    })
  );
  return chunkPaths;
}

async function compressVideo(inputPath, outputPath, knownDuration, inputHeight = 1920, enc = {}) {
  const duration = Number.isFinite(knownDuration) ? knownDuration : await getVideoDuration(inputPath);
  let startAttempt = (duration >= 59) ? 1 : 0;

  for (let attempt = startAttempt; attempt < MAX_ENCODE_ATTEMPTS; attempt++) {
    console.log(`🎬 compressVideo → Attempt ${attempt} | height:${inputHeight}`);
    // Gated through the EncodeSemaphore with the 600s full-video timeout. The
    // ffmpeg command is unchanged, so skip-path output stays byte-for-byte
    // identical to the no-editor flow (Req 1.2/1.4/1.5).
    await runGatedEncode(
      () => ffmpeg(inputPath).outputOptions(getOutputOptions(duration, inputHeight, attempt, enc)),
      outputPath,
      FULL_VIDEO_TIMEOUT_MS,
      `compressVideo attempt ${attempt}`
    );

    const postFFmpegSha = await sha256File(outputPath);
    console.log(`🔬 [HASH] Post-FFmpeg local file: ${postFFmpegSha}`);

    const sizeMB = fs.statSync(outputPath).size / (1024 * 1024);
    if (sizeMB <= RETRY_SIZE_MB) {
      console.log(`✓ Compression successful! Size: ${sizeMB.toFixed(2)}MB`);
      return;
    }
    console.log(`⚠️ Attempt ${attempt} failed size check (${sizeMB.toFixed(2)}MB > 15.5MB). Retrying...`);
  }
  throw new Error('Video could not be compressed under 16MB even at 540p resolution.');
}

async function uploadToR2(filePath, fileName) {
  // Kept under the historical function name to minimize changes to the
  // processing pipeline. No network storage is used anymore.
  const target = safeObjectPath(fileName);
  await fs.promises.mkdir(path.dirname(target), { recursive: true });
  await fs.promises.copyFile(filePath, target);
  console.log(`Local object stored: ${fileName}`);
  return target;
}

async function deleteFromR2(fileName) {
  const target = safeObjectPath(fileName);
  await fs.promises.rm(target, { force: true, recursive: true });
}

const R2_ORPHAN_AGE_MS = 30 * 60 * 1000;
const R2_EPHEMERAL_PREFIXES = ['compressed_', 'chunk_', 'uploads/', 'music/'];

async function listR2ObjectsByPrefix(prefix) {
  const dir = safeObjectPath(prefix);
  const out = [];
  async function walk(current, rel) {
    let entries=[];
    try { entries = await fs.promises.readdir(current, { withFileTypes: true }); } catch (e) { if (e.code === 'ENOENT') return; throw e; }
    for (const entry of entries) {
      const abs = path.join(current, entry.name);
      const key = path.relative(OBJECT_ROOT, abs).replace(/\\/g, '/');
      if (entry.isDirectory()) await walk(abs, key + '/');
      else { const st = await fs.promises.stat(abs); out.push({ Key:key, LastModified:st.mtime }); }
    }
  }
  await walk(dir, prefix);
  return out;
}

async function sweepOrphanR2Objects() {
  const tracked = new Set();
  for (const session of sessions.values()) {
    for (const f of (session.files || [])) if (f && f.fileName) tracked.add(f.fileName);
    for (const k of (session.assetKeys || [])) if (k) tracked.add(k);
  }
  const now = Date.now(); let removed=0;
  for (const prefix of R2_EPHEMERAL_PREFIXES) {
    let objects; try { objects=await listR2ObjectsByPrefix(prefix); } catch(e) { console.error(`Local orphan sweep: list "${prefix}" failed: ${e.message}`); continue; }
    for (const obj of objects) {
      if (!obj.Key || tracked.has(obj.Key)) continue;
      if (!(now - new Date(obj.LastModified).getTime() > R2_ORPHAN_AGE_MS)) continue;
      try { await deleteFromR2(obj.Key); removed++; console.log(`🧹 Local orphan removed: ${obj.Key}`); } catch(e) { console.error(`Local orphan delete "${obj.Key}" failed: ${e.message}`); }
    }
  }
  if (removed) console.log(`🧹 Local orphan sweep: removed ${removed} stale object(s).`);
  return removed;
}

// Direct local upload endpoint. It replaces the old Cloudflare Worker PUT.
app.put(/^\/api\/upload\/(.+)$/, express.raw({ type: '*/*', limit: '350mb' }), async (req, res) => {
  try {
    const key = decodeURIComponent(req.params[0]);
    if (!/^uploads\/[A-Za-z0-9._-]+$/.test(key)) return res.status(400).json({ error: 'Invalid upload key.' });
    if (!Buffer.isBuffer(req.body)) return res.status(400).json({ error: 'Empty upload body.' });
    const target = safeObjectPath(key);
    await fs.promises.mkdir(path.dirname(target), { recursive: true });
    await fs.promises.writeFile(target, req.body);
    res.json({ ok: true, key });
  } catch (error) {
    console.error('Local upload error:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get(/^\/api\/object\/(.+)$/, async (req, res) => {
  try {
    const key = decodeURIComponent(req.params[0]);
    const target = safeObjectPath(key);
    if (!fs.existsSync(target)) return res.status(404).end();
    res.sendFile(target);
  } catch { res.status(404).end(); }
});

// ========================
// BAILEYS WHATSAPP TRANSPORT
// ========================
let sock = null;
let baileysConnected = false;
let reconnecting = false;
let reconnectAttempts = 0; // drives exponential backoff so a 403 loop doesn't hammer WhatsApp
// Where the Baileys session is stored. Configurable so it can point at a mounted
// persistent volume on any host (Railway volume, Azure Files, etc.). Default keeps
// the current relative folder so existing deployments are unaffected.
const BAILEYS_AUTH_DIR = process.env.BAILEYS_AUTH_DIR || 'baileys_auth';

// Device footprint = the client type WhatsApp shows for the linked device. We
// persist a randomly chosen footprint alongside the auth so it stays consistent
// across reconnects of the SAME session — but a RESET_BAILEYS re-link (which
// wipes the auth dir) regenerates a NEW random one. So each fresh link presents
// as a different device type, not a repeat of the previously flagged one.
// (Baileys already regenerates the Signal identity keys on re-link, so a re-link
// is cryptographically a new device regardless; this just varies the visible
// device type to match.)
const DEVICE_FOOTPRINT_POOL = [
  Browsers.macOS('Desktop'),
  Browsers.windows('Desktop'),
  Browsers.macOS('Chrome'),
  Browsers.windows('Chrome'),
  Browsers.ubuntu('Chrome'),
  Browsers.macOS('Safari'),
  Browsers.windows('Edge'),
];

function getOrCreateDeviceFootprint(dir) {
  const file = path.join(dir, 'device-footprint.json');
  try {
    if (fs.existsSync(file)) {
      const saved = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (Array.isArray(saved) && saved.length === 3) return saved;
    }
  } catch (e) { /* fall through and pick a fresh one */ }
  const choice = DEVICE_FOOTPRINT_POOL[Math.floor(Math.random() * DEVICE_FOOTPRINT_POOL.length)];
  try {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(file, JSON.stringify(choice));
  } catch (e) { /* non-fatal — footprint just won't persist across restarts */ }
  return choice;
}

// Resolve the CURRENT WhatsApp Web protocol version at runtime. WhatsApp rejects
// clients announcing a stale version with a `405` close right after connecting —
// which looks exactly like a broken link. Baileys ships a hardcoded version that
// goes stale over time, so we fetch the live version instead: first straight from
// WhatsApp, then Baileys' own tracker, and only fall back to the bundled default
// if both network fetches fail. Cached so reconnects don't refetch every time.
let cachedWaVersion = null;
async function resolveWaVersion() {
  if (cachedWaVersion) return cachedWaVersion;
  try {
    const { version } = await fetchLatestWaWebVersion();
    cachedWaVersion = version;
    console.log(`WA Web version (live from WhatsApp): ${version.join('.')}`);
    return cachedWaVersion;
  } catch (e) {
    console.warn('Could not fetch live WA Web version:', e.message);
  }
  try {
    const { version } = await fetchLatestBaileysVersion();
    cachedWaVersion = version;
    console.log(`WA Web version (Baileys tracker): ${version.join('.')}`);
    return cachedWaVersion;
  } catch (e) {
    console.warn('Could not fetch Baileys tracker version:', e.message);
  }
  console.warn('Falling back to Baileys bundled WA Web version (may be stale → 405 risk).');
  return undefined; // let makeWASocket use its bundled default
}

async function startBaileys() {
  if (reconnecting) return;
  reconnecting = true;

  try {
    const { state, saveCreds } = await useMultiFileAuthState(BAILEYS_AUTH_DIR);
    const waVersion = await resolveWaVersion();

    const rawSock = makeWASocket({
      auth: state,
      logger: pino({ level: 'silent' }),
      // Use the live WhatsApp Web version so WhatsApp doesn't 405 us for looking
      // outdated. `undefined` means "use Baileys' bundled default" (fetch failed).
      ...(waVersion ? { version: waVersion } : {}),
      // Random-but-persisted device footprint: consistent across reconnects of
      // this session, but a fresh RESET_BAILEYS re-link picks a new one so the
      // new device doesn't look like the previously flagged one.
      browser: getOrCreateDeviceFootprint(BAILEYS_AUTH_DIR),
      syncFullHistory: false,
      markOnlineOnConnect: false,
      generateHighQualityLinkPreview: false,
      printQRInTerminal: false,  // 🆕 disable QR
    });

    // NOTE: baileys-antiban v4.10.0 `wrapSocket().sendMessage()` crashes on every
    // send ("Cannot read properties of undefined (reading 'circuitBreaker')"),
    // which took down all delivery. So we do NOT wrap the socket. Delivery uses
    // the raw socket directly; anti-ban behavior is handled natively below
    // (typing/recording presence, paced outbound sends, reconnect backoff).
    sock = rawSock;

    // Low-level protocol (events, auth, pairing) is bound to the RAW socket —
    // the antiban wrapper only needs to intercept outbound sends, and may not
    // proxy these internals. Outbound sends use the wrapped `sock`.
    rawSock.ev.on('creds.update', saveCreds);

    rawSock.ev.on('connection.update', (update) => {
      const { connection, lastDisconnect } = update;
      // QR intentionally not printed — we link via pairing code only (below).
      if (connection === 'close') {
        baileysConnected = false;
        const code = new Boom(lastDisconnect?.error)?.output?.statusCode;
        const shouldReconnect = code !== DisconnectReason.loggedOut;
        console.log(`Baileys closed (code ${code}). Reconnect: ${shouldReconnect}`);
        reconnecting = false;
        if (shouldReconnect) {
          reconnectAttempts += 1;
          // If we're not linked yet, reconnecting fast spits out a brand-new
          // pairing code every few seconds — the user can never type one before
          // it changes. So while UNREGISTERED, wait a full minute between tries
          // so ONE code stays on screen long enough to actually enter.
          const isRegistered = rawSock?.authState?.creds?.registered;
          let delay;
          if (!isRegistered) {
            delay = 60000 + randBetween(0, 3000);
            console.log('Not linked yet — showing one pairing code per minute. Enter the MOST RECENT code above and ignore the older ones.');
          } else {
            // Fast recovery for transient drops; mild backoff only if it keeps
            // failing, capped low (15s) so the bot never stays offline long.
            delay = Math.min(15000, 3000 * reconnectAttempts) + randBetween(0, 1500);
          }
          console.log(`Reconnecting in ${Math.round(delay / 1000)}s (attempt ${reconnectAttempts})`);
          setTimeout(() => startBaileys().catch(e => console.error('Reconnect failed:', e)), delay);
        } else {
          console.error('!!! Baileys logged out — set RESET_BAILEYS=true and redeploy to re-link.');
        }
      } else if (connection === 'open') {
        baileysConnected = true;
        reconnecting = false;
        reconnectAttempts = 0; // healthy connection — reset backoff
        console.log('✓ Baileys connected to WhatsApp');
      }
    });

    // Pairing-code linking (no QR). Retries a few times since requestPairingCode
    // can fail if called before the socket is fully ready.
    if (!rawSock.authState.creds.registered) {
      const phoneNumber = (process.env.WHATSAPP_BUSINESS_NUMBER || '').replace(/\D/g, '');
      if (!phoneNumber) {
        console.error('!!! Cannot request pairing code: WHATSAPP_BUSINESS_NUMBER is not set (use full international number, e.g. +9198XXXXXXXX).');
      } else {
        let attempts = 0;
        const requestPair = async () => {
          attempts++;
          try {
            const code = await rawSock.requestPairingCode(phoneNumber);
            const formatted = code?.match(/.{1,4}/g)?.join('-') || code;
            console.log('\n=========================================');
            console.log(`  PAIRING CODE: ${formatted}`);
            console.log(`  For number: +${phoneNumber}`);
            console.log('  In WhatsApp on that phone:');
            console.log('  Settings → Linked Devices → Link a Device');
            console.log('  → "Link with phone number instead" → enter this code');
            console.log('=========================================\n');
          } catch (err) {
            console.error(`Pairing code attempt ${attempts} failed:`, err.message);
            if (attempts < 4) setTimeout(requestPair, 5000);
            else console.error('!!! Could not obtain a pairing code after several tries. Verify WHATSAPP_BUSINESS_NUMBER and redeploy.');
          }
        };
        setTimeout(requestPair, 3000);
      }
    }

    // Keep Baileys' LID -> phone mappings warm whenever WhatsApp exposes one.
    // This is supplementary; direct message fields and signalRepository lookup
    // remain the primary resolution paths.
    rawSock.ev.on('lid-mapping.update', async (update) => {
      try {
        if (!update || typeof update !== 'object') return;
        const pairs = Object.entries(update)
          .map(([lid, pn]) => ({ lid: String(lid), pn: String(pn) }))
          .filter(({ lid, pn }) => lid.includes('@lid') && pn.includes('@s.whatsapp.net'));
        if (pairs.length) {
          await rawSock.signalRepository?.lidMapping?.storeLIDPNMappings?.(pairs);
          console.log(`✓ LID/phone mappings updated: ${pairs.length}`);
        }
      } catch (err) {
        console.warn(`Could not store LID/phone mapping update: ${err.message}`);
      }
    });

    rawSock.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify') return;
      for (const msg of messages) {
        if (!msg.message || msg.key.fromMe) continue;
        const from = msg.key.remoteJid;
        if (!from || from.endsWith('@g.us') || from === 'status@broadcast') continue;
        const identity = await getWhatsAppIdentity(msg, rawSock);
        console.log(`WhatsApp identity resolved: jid=${identity.jid || from} phone=${identity.phone || 'UNRESOLVED'} userId=${identity.userId || '—'} senderPn=${msg.key?.senderPn || '—'} remoteJidAlt=${msg.key?.remoteJidAlt || '—'}`);

        // GUARD 1 — drop history replays. On reconnect/relink Baileys re-emits
        // old messages as type 'notify', so they looked brand new and were
        // answered again.
        const staleness = isStaleMessage(msg.messageTimestamp);
        if (staleness.stale) {
          console.log(`Ignoring stale message from ${jidToNumber(from)} (${staleness.ageSeconds}s old)`);
          continue;
        }

        // GUARD 2 — exact-duplicate suppression. WhatsApp can deliver the same
        // message id more than once; two upsert events for one message would
        // otherwise produce two replies.
        if (messageDeduper.isDuplicate(msg.key.id)) {
          console.log(`Duplicate message id ${msg.key.id} — ignored`);
          continue;
        }

        const text = msg.message.conversation
          || msg.message.extendedTextMessage?.text
          || msg.message.imageMessage?.caption
          || msg.message.videoMessage?.caption
          || '';
        try {
          await handleIncomingMessage(from, text.trim(), identity);
        } catch (err) {
          console.error('Message handler error:', err.message);
        }
      }
    });
  } catch (err) {
    reconnecting = false;
    console.error('Baileys startup error:', err);
    setTimeout(() => startBaileys().catch(e => console.error('Restart failed:', e)), 5000);
  }
}

// Reject a promise if it doesn't settle within `ms`, so a stalled network call
// (R2 download, Baileys send) can never hang the delivery flow forever. The
// underlying operation is abandoned (its result is ignored) once we time out.
function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const e = new Error(`${label || 'operation'} timed out after ${Math.round(ms / 1000)}s`);
      e.isTimeout = true;
      reject(e);
    }, ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// Per-send wall-clock limits for WhatsApp delivery (prevents a stuck send from
// stranding a session in `processing`).
const WA_MESSAGE_TIMEOUT_MS = 30000;   // text message send
const WA_DOWNLOAD_TIMEOUT_MS = 120000; // R2 -> buffer download
const WA_VIDEO_SEND_TIMEOUT_MS = 180000; // Baileys video upload+send

// Jitter + human-pacing helpers.
//
// randBetween  : uniform integer in [min,max] (used by the reconnect backoff).
// gaussianDelay: normally-distributed delay that CLUSTERS around `mean` (via the
//                Box–Muller transform), clamped to [min,max]. Real human timing
//                is bell-shaped, not flat — a fixed or uniform gap is easy for
//                WhatsApp to fingerprint as a bot, a Gaussian one is not.
// humanPause   : before a send, show typing/recording presence, hold for a
//                Gaussian pause (~6s), then clear it — mimicking a person
//                composing a reply. Best-effort: presence errors never block the
//                real send. Set HUMANIZE_SENDS=false to disable if you need raw speed.
function randBetween(min, max) { return Math.floor(min + Math.random() * (max - min + 1)); }

function gaussianDelay(mean, stddev, min, max) {
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  const z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  return Math.round(Math.min(max, Math.max(min, mean + z * stddev)));
}

const HUMANIZE_SENDS = process.env.HUMANIZE_SENDS !== 'false'; // on by default

async function humanPause(jid, kind = 'text') {
  if (!HUMANIZE_SENDS) return;
  const presence = kind === 'video' ? 'recording' : 'composing';
  try {
    await sock.sendPresenceUpdate('available', jid);
    await sock.sendPresenceUpdate(presence, jid);
  } catch (e) { /* presence is best-effort — never block the actual send */ }
  // Cluster around ~6s: most pauses land ~4–8s, hard-capped to 2.5–11s.
  await new Promise(r => setTimeout(r, gaussianDelay(6000, 1500, 2500, 11000)));
  try { await sock.sendPresenceUpdate('paused', jid); } catch (e) { /* ignore */ }
}

// ========================
// MESSAGE VARIATION (anti-fingerprint)
// Hundreds of chats using the *identical* inbound/outbound text is a strong
// "automated broadcast service" signal. Rotating the phrasing makes each
// conversation look a little different. The inbound code is still parsed by the
// 9-char token fallback regex regardless of the words around it.
// ========================
function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

// Prefilled text for the wa.me link the user taps to message the bot. Each
// keeps the 9-char code as a standalone token so handleIncomingMessage still
// parses it.
// The code is embedded as a natural "order/reference" token. We deliberately
// avoid the word "code" so the message reads like an ordinary customer request.
// Parsing still works: codes are UPPERCASE A-Z0-9, and the parser's third
// pattern is a CASE-SENSITIVE 9-char token match, so it picks out the token
// without matching ordinary lowercase words around it.
const INBOUND_CODE_TEMPLATES = [
  c => `Hey! Just made my video on MIABE STATUS HD — order no. ${c}. Could you send the HD version? 🙏`,
  c => `Hi, my reference is ${c}, could you send my compressed video? 😊`,
  c => `Hello! My order number is ${c} — please send my HD status video.`,
  c => `Hi there, ref ${c} — could you send me my video in HD? 🎬`,
  c => `Just finished on MIABE STATUS HD, my reference number ${c}. Please send it over!`,
  c => `Hey, order ${c} — can you send my compressed video please? 🙏`,
  c => `Hi! My ref is ${c}, would love my HD video, thank you 😊`,
];
function buildInboundText(code) { return pick(INBOUND_CODE_TEMPLATES)(code); }

// The posting instructions are important, so they stay constant; only the
// surrounding phrasing rotates.
// Posting instructions.
//
// Why the download + re-send detour: forwarding the video straight from this
// chat makes WhatsApp re-encode it at the Status step (you see a loading spinner
// and the result is soft). Saving it and sending it back into the chat makes
// WhatsApp re-upload it with its own full metadata, and forwarding THAT copy to
// Status is instant and keeps full HD. Verified by testing both paths with the
// same file. Baileys cannot attach everything a real WhatsApp client does
// (notably no streamingSidecar), so this is handled in the instructions.
const HD_STEPS =
  '📱 *Comment publier sur ton Statut WhatsApp :*\n' +
  '1. Ouvre la vidéo ou la photo reçue dans WhatsApp.\n' +
  '2. Appuie sur le bouton ↪ *Transférer*.\n' +
  '3. Sélectionne *Mon statut*.\n' +
  '4. Publie directement sur ton Statut.\n\n' +
  '✅ *C’est tout !*\n\n' +
  '⚠️ *Important :* ne rogne pas, ne modifie pas et ne réédite pas la vidéo ou la photo. Transfère simplement le fichier reçu directement vers ton Statut.';

function buildVerifiedMessage(count) {
  const multi = count > 1;
  const openers = [
    '✓ Code confirmé !',
    '✅ Parfait, ton code est confirmé !',
    'Code validé ! ✓',
    'Vérification réussie ! ✅',
  ];
  const closers = [
    'Un instant, tes fichiers arrivent ! 🎬',
    'Envoi en cours… 🎬',
    'Encore un instant, ça arrive ! 🎬',
    'Tes fichiers arrivent ! 🎬',
  ];
  return (
    pick(openers) + '\n\n' +
    `📤 Envoi de ${count} vidéo${multi ? 's' : ''} en cours…\n\n` +
    (multi ? `📱 Ta vidéo a été découpée en ${count} parties pour ton Statut WhatsApp.\n\n` : '') +
    HD_STEPS + '\n\n' +
    pick(closers)
  );
}

const WELCOME_MESSAGES = [
  '👋 Welcome to MIABE STATUS HD! Send your activation code here to receive your optimized HD videos. 🌐',
  'Hey there! 👋 To get your HD video, use the MIABE STATUS HD website and send the activation code here.',
  'Hi! 👋 MIABE STATUS HD prepares videos for WhatsApp Status in HD. 🌐',
];

async function sendWhatsAppMessage(to, message) {
  if (!sock || !baileysConnected) throw new Error('Baileys not connected');
  const jid = toJid(to);
  await humanPause(jid, 'text');
  await withTimeout(sock.sendMessage(jid, { text: message }), WA_MESSAGE_TIMEOUT_MS, 'WhatsApp message send');
}

/**
 * Probe a video buffer for the metadata WhatsApp expects on a videoMessage.
 *
 * ffprobe needs a real path, so the buffer is written to a short-lived temp file
 * and removed immediately. Failures are non-fatal: we return {} and the send
 * proceeds without metadata rather than dropping the user's video.
 *
 * @param {Buffer} buffer encoded mp4 bytes
 * @returns {Promise<{width?: number, height?: number, seconds?: number}>}
 */
async function probeVideoMeta(buffer) {
  const tmp = path.join('compressed', `meta_${uuidv4()}.mp4`);
  try {
    await fs.promises.writeFile(tmp, buffer);
    const [dims, duration] = await Promise.all([
      getVideoDimensions(tmp),
      getVideoDuration(tmp),
    ]);
    const meta = {};
    if (Number.isFinite(dims?.width) && Number.isFinite(dims?.height)) {
      meta.width = Math.round(dims.width);
      meta.height = Math.round(dims.height);
    }
    if (Number.isFinite(duration) && duration > 0) {
      meta.seconds = Math.round(duration);
    }
    return meta;
  } catch (err) {
    console.warn('Video metadata probe failed (sending without it):', err.message);
    return {};
  } finally {
    await fs.promises.unlink(tmp).catch(() => { });
  }
}

async function sendWhatsAppVideo(to, videoUrl, caption) {
  if (!sock || !baileysConnected) throw new Error('Baileys not connected');
  const jid = toJid(to);

  // Human pacing: show "recording" presence and hold a Gaussian pause before
  // sending (the R2 download below also runs during this window).
  await humanPause(jid, 'video');

  // Download from R2 into a buffer (bounded by a timeout so a stalled fetch
  // can't hang the whole delivery).
  const videoBuffer = await fs.promises.readFile(videoUrl);

  // Baileys does NOT populate video metadata: its message builder computes
  // `seconds` only for audio (mediaType === 'audio'), and backfills width/height
  // from the 32x32 thumbnail probe. So videos ship with no duration and wrong or
  // missing dimensions — unlike a real WhatsApp client, which always sends them.
  // The receiving app then has to DECODE the file to derive them, and once it is
  // decoding it re-encodes, which is what destroys quality when forwarding to
  // Status. Probe the real values and pass them explicitly.
  const meta = await probeVideoMeta(videoBuffer);

  await withTimeout(
    sock.sendMessage(jid, {
      video: videoBuffer,
      ...(typeof caption === 'string' && caption.trim() ? { caption: caption.trim().slice(0, 1000) } : {}),
      mimetype: 'video/mp4',
      ...meta, // width, height, seconds — omitted entirely if the probe failed
    }),
    WA_VIDEO_SEND_TIMEOUT_MS,
    'WhatsApp video send'
  );
  console.log(`Video sent via Baileys! ✓ ${meta.width ? `(${meta.width}x${meta.height}, ${meta.seconds}s)` : '(metadata unavailable)'}`);
}

// ========================
// INCOMING MESSAGE HANDLER
// (your old webhook logic, transport-swapped)
// ========================
async function handleIncomingMessage(from, text, identity = {}) {
  const senderPhone = normalizePhone(identity.phone || jidToNumber(from));
  const senderUserId = identity.userId || (from.endsWith('@lid') ? jidToNumber(from) : senderPhone.replace(/^\+/, ''));
  console.log(`Message from ${senderPhone} [ID ${senderUserId}]: ${text}`);

  // WEBSITE AUTH FLOW: only the AUTH-XXXXXX token matters. Everything else
  // in the WhatsApp message is ignored, including the explanatory text.
  const authCodeMatch = text?.match(/\bAUTH-[A-Z0-9]{6}\b/i);
  if (authCodeMatch) {
    const authCode = authCodeMatch[0].toUpperCase();
    const challenges = readAuthChallenges();
    const entry = Object.entries(challenges).find(([, c]) =>
      c && c.status === 'pending' && c.code === authCode && Number(c.expiresAt || 0) > Date.now()
    );

    if (!entry) {
      console.log(`Unknown or expired website AUTH code: ${authCode}`);
      try {
        await sendWhatsAppMessage(from,
          '❌ Ce code d’authentification est invalide ou a déjà été utilisé.\n\nRetourne sur MIABE STATUS HD et demande un nouveau code.'
        );
      } catch (err) {
        console.error('Failed invalid AUTH message:', err.message);
      }
      return;
    }

    const [challengeId, challenge] = entry;
    const phone = senderPhone;

    // A personal-link login must be verified by the WhatsApp number that owns
    // the link. A shared URL opened by another person cannot authenticate their browser.
    if (challenge.expectedPhone && normalizePhone(challenge.expectedPhone) !== phone) {
      console.log(`Rejected AUTH for personal-link attempt: expected ${challenge.expectedPhone}, got ${phone}`);
      try {
        await sendWhatsAppMessage(from,
          '❌ Ce code ne correspond pas au numéro WhatsApp associé à ce lien.\n\nUtilise le même numéro WhatsApp que celui associé à ton compte MIABE STATUS HD.'
        );
      } catch (err) {
        console.error('Failed AUTH phone-mismatch message:', err.message);
      }
      return;
    }

    // IMPORTANT: the WhatsApp number is the account identity. Before activating
    // anything, inspect the existing record so a returning user can NEVER receive
    // a second 7-day trial.
    const usersBeforeAuth = readUsers();
    const existingUser = usersBeforeAuth[phone] || null;
    const hadExistingTrial = !!(existingUser && existingUser.trialUntil);
    const hadExistingSubscription = !!(existingUser && existingUser.subscriptionUntil);
    const user = activateTrialForAuth(challenge.visitorId, phone, senderUserId);
    if (!user) return;

    // The authenticated state is now delivered only through an HttpOnly cookie.
    // The browser never receives a reusable session token in JSON/localStorage.
    challenge.sessionToken = null;
    challenge.status = 'verified';
    challenge.phone = user.phone;
    challenge.verifiedAt = Date.now();
    challenges[challengeId] = challenge;
    writeAuthChallenges(challenges);

    const nowAfterAuth = Date.now();
    const remainingTrialDays = user.trialUntil > nowAfterAuth
      ? Math.ceil((user.trialUntil - nowAfterAuth) / 86400000)
      : 0;
    const remainingSubscriptionDays = user.subscriptionUntil > nowAfterAuth
      ? Math.ceil((user.subscriptionUntil - nowAfterAuth) / 86400000)
      : 0;

    console.log(`✓ Website AUTH verified for ${user.phone} — ${existingUser ? 'RETURNING ACCOUNT' : 'NEW ACCOUNT'} — trialUntil=${user.trialUntil}`);

    // Confirmation sent directly to the WhatsApp number that sent AUTH-XXXXXX.
    // Returning users receive their existing remaining time instead of a new trial.
    try {
      const personalAccountLink = createPersonalAccountLink(user.phone, {
        protocol: 'http',
        get: (name) => process.env.PUBLIC_SITE_URL ? '' : (typeof req !== 'undefined' && req?.get ? req.get(name) : ''),
        headers: {},
      });

      let accessMessage;
      if (!existingUser) {
        accessMessage = `🎁 Ton ESSAI GRATUIT de ${TRIAL_DAYS} jours est maintenant actif.`;
      } else if (hadExistingTrial && remainingTrialDays > 0) {
        accessMessage = `🎁 Ton compte a déjà été activé. Il te reste ${remainingTrialDays} jour${remainingTrialDays > 1 ? 's' : ''} de ESSAI GRATUIT.`;
      } else if (hadExistingSubscription && remainingSubscriptionDays > 0) {
        accessMessage = `⭐ Ton abonnement est déjà actif. Il te reste ${remainingSubscriptionDays} jour${remainingSubscriptionDays > 1 ? 's' : ''}.`;
      } else {
        accessMessage = '⏳ Ton ESSAI GRATUIT est déjà terminé. Aucun nouvel ESSAI GRATUIT ne sera créé pour ce numéro.';
      }

      const confirmation =
        '✅ AUTHENTIFICATION RÉUSSIE\n\n' +
        (existingUser
          ? 'Ton compte MIABE STATUS HD a été reconnu.\n'
          : 'Ton compte MIABE STATUS HD a été créé avec succès.\n') +
        accessMessage + '\n\n' +
        '🔐 Voici ton lien personnel pour accéder directement à ton espace compte :\n' +
        personalAccountLink + '\n\n' +
        'Ce lien est permanent et unique à ton compte. Ne le partage pas avec une autre personne.';

      if (!sock || !baileysConnected) throw new Error('Baileys not connected');
      const confirmationJid = toJid(from);
      await withTimeout(
        sock.sendMessage(confirmationJid, { text: confirmation }),
        WA_MESSAGE_TIMEOUT_MS,
        'WhatsApp auth confirmation'
      );
      console.log(`✓ Auth confirmation sent to ${user.phone}`);
    } catch (err) {
      // The website authentication is already valid even if the confirmation
      // message fails. Log it so the transport issue can be diagnosed.
      console.error(`Auth confirmation failed for ${user.phone}:`, err.message);
    }

    return;
  }

  // Extract the 9-char activation code. The auth code above is handled first,
  // so it can never be mistaken for a video delivery code.
  //  1) "code (is/:) XXXXXXXXX" — the natural-sentence prefill (case-insensitive).
  //  2) legacy "Activation Code: XXXXXXXXX" prefill (older links).
  //  3) bare UPPERCASE 9-char token — CASE-SENSITIVE on purpose: codes are always
  //     uppercase A-Z0-9, so this won't grab an ordinary lowercase word that
  //     happens to be 9 letters long (e.g. "reference") when the code is embedded
  //     in a sentence.
  const codeMatch =
       text?.match(/\bcode\b\s*(?:is)?\s*:?\s*([A-Z0-9]{9})\b/i)
    || text?.match(/Activation Code[:\s]+([A-Z0-9]{9})/i)
    || text?.match(/\b([A-Z0-9]{9})\b/);

  if (!codeMatch) {
    // GUARD 3 — one welcome per sender per cooldown window. Someone typing
    // "hi", "hello", "?" in quick succession used to get a reply to every one,
    // which reads as spam and is exactly the behaviour that gets a number
    // reported. Silence is the right response to a repeat non-code message.
    if (!welcomeThrottle.shouldSend(from)) {
      console.log(`Welcome already sent to ${jidToNumber(from)} recently — staying quiet`);
      return;
    }
    welcomeThrottle.markSent(from);
    try {
      await sendWhatsAppMessage(from, pick(WELCOME_MESSAGES));
    } catch (err) {
      console.error('Failed welcome message:', err.message);
      welcomeThrottle.clear(from); // send failed — allow a retry next time
    }
    return;
  }

  const code = codeMatch[1].toUpperCase();

  if (recentlySentCodes.has(code)) {
    console.log(`Duplicate for sent code: ${code} — ignored!`);
    return;
  }

  const session = sessions.get(code);

  if (!session) {
    try {
      await sendWhatsAppMessage(from,
        '✗ Invalid or expired code!' +
        'Please compress your video again at our website.'
      );
    } catch (err) {
      console.error('Failed to send expired message:', err.message);
    }
    return;
  }

  // On the first successful use, this is the point where the website visitor
  // becomes a real account: the WhatsApp sender number is now known. Store the
  // association and start the 7-day trial from this first-use completion.
  if (session.firstUse && session.visitorId) {
    const linkedUser = markFirstUseCompleted(session.visitorId, senderPhone, senderUserId);
    session.authPhone = linkedUser?.phone || senderPhone;
    session.firstUse = false;
    sessions.set(code, session);
    console.log(`✓ First use linked to WhatsApp number ${session.authPhone}`);
  }

  if (session.status === 'sent') {
    console.log(`Duplicate webhook for code: ${code} — ignoring!`);
    return;
  }
  if (session.status === 'processing') {
    // Normally a duplicate webhook (WhatsApp re-delivers the same message a few
    // times); ignore it while a send is genuinely in progress. But if the
    // attempt has been "in progress" longer than any real send could take, treat
    // it as wedged and allow this webhook to re-attempt delivery (self-healing
    // so a stuck send can never permanently block the code).
    const since = session.processingSince || 0;
    if (Date.now() - since < PROCESSING_STALE_MS) {
      console.log(`Duplicate webhook for code: ${code} while processing - ignoring!`);
      return;
    }
    console.log(`Stale processing for code: ${code} (${Math.round((Date.now() - since) / 1000)}s) — re-attempting delivery`);
  }
  if (session.status === 'failed') {
    try {
      await sendWhatsAppMessage(from,
        'Failed to send your video.' +
        'Please compress your video again and try with a new code.'
      );
    } catch (err) {
      console.error('Failed retry message:', err.message);
    }
    return;
  }

  session.status = 'processing';
  session.processingSince = Date.now();
  setDeliveryStatus(code, 'processing');
  sessions.set(code, session);

  try {
    await sendWhatsAppMessage(from, buildVerifiedMessage(session.files.length));
  } catch (err) {
    console.error('Failed code verified message:', err.message);
  }

  const waStartTime = Date.now();
  try {
    for (let i = 0; i < session.files.length; i++) {
      const file = session.files[i];
      const isMultiple = session.files.length > 1;
      const videoSendStart = Date.now();

      // Build caption: user caption on FIRST part only, nothing on subsequent parts
      let videoCaption = '';
      if (i === 0 && typeof session.caption === 'string' && session.caption.trim()) {
        // First video/part — use the exact text entered on the website as the
        // native WhatsApp video caption (not as a separate message).
        videoCaption = session.caption.trim().slice(0, 1000);
        console.log(`📝 Sending WhatsApp caption: ${JSON.stringify(videoCaption)}`);
      }
      // All other parts (i > 0) — no caption at all (empty string)

      await sendWhatsAppVideo(from, file.url, videoCaption);

      console.log(`✓ Video ${i + 1}/${session.files.length} sent (${((Date.now() - videoSendStart) / 1000).toFixed(2)}s)`);
      // Spacing between parts is handled by humanPause() before each send.
    }
    console.log(`✓ All sends completed (${((Date.now() - waStartTime) / 1000).toFixed(2)}s)`);
    session.status = 'sent';
    setDeliveryStatus(code, 'sent');
  } catch (err) {
    // Delivery stalled/failed — but the video was ALREADY processed and is
    // sitting in R2. Do NOT discard it or brick the code: reset to a retryable
    // state and keep the R2 files so the user can simply resend the same code.
    // The session's existing expiry timer still bounds how long the files live.
    session.status = 'pending';
    session.processingSince = 0;
    setDeliveryStatus(code, 'pending');
    sessions.set(code, session);
    console.error('Video send failed:', err.message);
    try {
      await sendWhatsAppMessage(from,
        "Un problème temporaire est survenu lors de l'envoi de votre vidéo. Veuillez renvoyer votre code dans un instant pour réessayer. 🎬"
      );
    } catch (messageErr) {
      console.error('Failed send-failure message:', messageErr.message);
    }
    return; // keep files + existing expiry timer so the retry can deliver
  }

  // SUCCESS only — purge the delivered files and any uploaded music/sticker R2
  // assets (Req 14.2), then schedule session cleanup. (On failure we returned
  // above WITHOUT deleting, so a resend can still find the files.)
  for (const file of session.files) {
    try {
      await deleteFromR2(file.fileName);
      console.log(`R2 deleted after send: ${file.fileName}`);
    } catch (err) {
      console.error('R2 delete error:', err.message);
    }
  }
  if (Array.isArray(session.assetKeys)) {
    for (const assetKey of session.assetKeys) {
      try {
        await deleteFromR2(assetKey);
        console.log(`R2 asset deleted after send: ${assetKey}`);
      } catch (err) {
        console.error('R2 asset delete error:', err.message);
      }
    }
  }

  if (session.expiryTimer) {
    clearTimeout(session.expiryTimer);
    console.log(`Original timer cancelled for: ${code}`);
  }
  const newTimer = setTimeout(() => {
    sessions.delete(code);
    console.log(`Session cleaned after ${session.status}: ${code}`);
  }, 300000);
  session.files = [];
  session.assetKeys = [];
  session.createdAt = Date.now();
  session.expiryTimer = newTimer;
  sessions.set(code, session);

  recentlySentCodes.add(code);
  setTimeout(() => recentlySentCodes.delete(code), 600000);
}

// ========================
// PAYMENT / SUBSCRIPTION CONFIG
// ========================
app.get('/api/subscription/config', (req,res)=>res.json({
  trialDays: TRIAL_DAYS,
  monthlyAmountXof: MOMO_MONTHLY_AMOUNT_XOF,
  momoEnabled: MOMO_ENABLED,
  currency:'XOF',
  provider:'MoMo',
  authenticationRequiredAfterFirstUse:true,
  firstUseRequiresLogin:false
}));

// ========================
// EXPRESS ROUTES
// ========================
app.use((req, res, next) => {
  if (req.path === '/api/process') {
    req.setTimeout(900000);
    res.setTimeout(900000);
  }
  next();
});

app.get('/api/health', (req, res) => {
  // `ffmpegOnPath` matters for delivery quality, not just encoding: Baileys
  // shells out to a BARE `ffmpeg` to build the video thumbnail and derive
  // width/height/duration. Without it, outgoing videoMessages carry no
  // metadata and WhatsApp re-encodes them when forwarded to Status instead of
  // passing them through. Surfaced here so it can be verified, not assumed.
  res.json({
    status: '✓ Server Running!',
    baileys: baileysConnected ? 'connected' : 'disconnected',
    ffmpegOnPath: ffmpegOnPath(),
    fpsCap: FPS_CAP,
  });
});

app.get('/privacy', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'privacy.html'));
});

// ========================
// DELIVERY STATUS (FRONTEND POLLING)
// ========================
// Keep the final delivery state independently for a short time so the
// frontend can still detect `sent` even if the main session is cleaned up.
const deliveryStatusCache = new Map();
const DELIVERY_STATUS_TTL_MS = 10 * 60 * 1000;

function setDeliveryStatus(code, status) {
  const key = String(code || '').trim().toUpperCase();
  if (!key) return;
  deliveryStatusCache.set(key, { status, expiresAt: Date.now() + DELIVERY_STATUS_TTL_MS });
  setTimeout(() => {
    const item = deliveryStatusCache.get(key);
    if (item && item.expiresAt <= Date.now()) deliveryStatusCache.delete(key);
  }, DELIVERY_STATUS_TTL_MS + 1000);
}
// The browser polls this endpoint after opening WhatsApp. It is intentionally
// read-only: it only exposes the status of the user's own short-lived activation
// code so the frontend can refresh once delivery has completed.
app.get('/api/delivery-status/:code', (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  if (!/^[A-Z0-9]{9}$/.test(code)) {
    return res.status(400).json({ status: 'invalid' });
  }

  const cached = deliveryStatusCache.get(code);
  if (cached) {
    if (cached.expiresAt <= Date.now()) {
      deliveryStatusCache.delete(code);
    } else {
      return res.json({ status: cached.status });
    }
  }

  const session = sessions.get(code);
  if (!session) {
    return res.status(404).json({ status: 'not_found' });
  }

  return res.json({
    status: session.status || 'pending'
  });
});

// ========================
// META WEBHOOK (WhatsApp Cloud API / coexistence)
// ========================
// Required by Meta's "Configure Webhooks" production-setup step. Two behaviours:
//   GET  /webhook -> echo hub.challenge when hub.verify_token matches ours.
//   POST /webhook -> accept events and answer 200 fast, or Meta retries.
//
// This endpoint is RECEIVE-ONLY and has NO side effects: it does not send
// messages, touch sessions, or trigger delivery. Message delivery stays on
// Baileys, which is what preserves HD quality. It is intentionally public
// (Meta must reach it unauthenticated), so it is protected by the verify token
// on GET and, when META_APP_SECRET is set, an HMAC signature check on POST.
const WEBHOOK_VERIFY_TOKEN = process.env.WEBHOOK_VERIFY_TOKEN || '';
const META_APP_SECRET = process.env.META_APP_SECRET || '';

app.get('/webhook', (req, res) => {
  const result = verifyChallenge({
    mode: req.query['hub.mode'],
    token: req.query['hub.verify_token'],
    challenge: req.query['hub.challenge'],
  }, WEBHOOK_VERIFY_TOKEN);

  if (!result.ok) {
    console.warn(`Webhook verification refused: ${result.reason}`);
    return res.sendStatus(result.status);
  }
  console.log('✓ Webhook verified by Meta');
  res.status(200).send(result.challenge);
});

app.post('/webhook', (req, res) => {
  if (!isValidSignature(req.rawBody, req.get('x-hub-signature-256'), META_APP_SECRET)) {
    console.warn('Webhook POST rejected: bad X-Hub-Signature-256');
    return res.sendStatus(403);
  }
  // Acknowledge immediately — Meta retries if we are slow.
  res.sendStatus(200);
  try {
    console.log(`📩 Webhook event: ${summarizeEvent(req.body)}`);
  } catch (err) {
    console.error('Webhook logging error:', err.message);
  }
});

app.post('/api/upload-url', limiter, async (req, res) => {
  try {
    const { filename, contentType, fileSize } = req.body;
    const gate = validateUploadRequest({ filename, contentType, fileSize });
    if (!gate.ok) return res.status(gate.status).json({ error: gate.error });
    const ext = path.extname(filename).toLowerCase();
    const key = `uploads/${uuidv4()}${ext}`;
    res.json({ uploadUrl: `${req.protocol}://${req.get('host')}/api/upload/${encodeURIComponent(key)}`, key });
  } catch (error) {
    console.error('Upload URL error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Background job store for /api/process. See src/server/jobStore.js for why.
const jobStore = new JobStore();
setInterval(() => jobStore.sweep(), 5 * 60 * 1000).unref?.();

/**
 * Poll a background job. The client hits this after /api/process hands back a
 * jobId, so a killed connection (Azure's ~240s cap, a backgrounded mobile tab,
 * flaky network) no longer loses the activation code.
 */
app.get('/api/job/:jobId', (req, res) => {
  const job = jobStore.get(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found or expired. Please compress again.' });
  }
  if (job.status === 'processing') return res.json({ status: 'processing' });
  if (job.status === 'done') return res.json({ status: 'done', ...job.result });
  return res.json({ status: 'error', ...job.error, httpStatus: job.httpStatus });
});

app.post('/api/process', limiter, async (req, res) => {
  // Server-side access gate:
  // - first use: allowed without login;
  // - subsequent use: valid account session required;
  // - expired trial/subscription: processing blocked.
  const accessGate = processingAccess(req, res);
  if (!accessGate.ok) return res.status(accessGate.status).json(accessGate.body);

  let releaseProcessingPrioritySlot = null;

  // Async mode: answer immediately with a jobId and finish the work in the
  // background. The client opts in with `async: true`, so an older cached
  // frontend still gets the original synchronous response and keeps working
  // during the window where Vercel and Azure are on different versions.
  const asyncMode = req.body && req.body.async === true;
  let jobId = null;
  if (asyncMode) {
  // WhatsApp activation flow: no website login is required. The user uploads and
  // processes the video first; the activation code returned below links the
  // already-processed files to the WhatsApp conversation opened by the user.
    jobId = jobStore.create();
    res.status(202).json({ jobId });
  }

  // Routes each of the handler's responses to either the live HTTP response
  // (sync mode) or the job record (async mode), so the logic below is identical
  // in both modes.
  const reply = {
    json: (body) => {
      if (asyncMode) jobStore.resolve(jobId, body);
      else res.json(body);
    },
    status: (code) => ({
      json: (body) => {
        if (asyncMode) jobStore.reject(jobId, code, body);
        else res.status(code).json(body);
      },
    }),
  };

  try {
    const { files } = req.body;

    // Input-limit gate: runs BEFORE any download/encode (Req 1.6, 13.5, 13.6).
    // Added-audio size per video comes from the recipe's Music_Track asset size
    // when present in the request body (forward-compatible with the recipes map).
    const recipes = req.body.recipes || null;
    const gate = enforceInputLimits(files, {
      getAudioBytes: recipes
        ? (file) => {
            const music = file && file.key && recipes[file.key]
              && recipes[file.key].audio && recipes[file.key].audio.music;
            return music ? Number(music.sizeBytes) : undefined;
          }
        : undefined,
    });
    if (!gate.ok) {
      return reply.status(400).json({ error: gate.error, limit: gate.limit });
    }

    // Process only the retained set (first 3 videos in upload order).
    const acceptedFiles = gate.retained;
    const totalSizeMB = acceptedFiles.reduce((sum, f) => sum + f.size, 0) / (1024 * 1024);
    console.log(`📥 Processing ${acceptedFiles.length} file(s) — Total: ${totalSizeMB.toFixed(1)}MB`);

    const activationCode = generateCode();
    const r2Files = [];
    const uploadedKeys = acceptedFiles.map(f => f.key);

    // Per-request artifact ledger. Every download, output, and chunk is
    // registered here so the cleanup wiring (task 10.6) can purge the request
    // from disk and R2 on success or any failure. We populate it now.
    const requestContext = new RequestContext(activationCode);
    // Track this request as in-progress so the Startup_Sweep retains its live
    // temp files; removed in `finally`.
    activeRequests.add(requestContext);

    // Output clips/chunks uploaded to R2 are DELIVERY artifacts: they must
    // survive the request `finally` so handleIncomingMessage can send them, then
    // delete them within 60s of delivery (Req 14.2). They are tracked here
    // (NOT in the cleanup ledger) on success; on ANY failure before delivery
    // hand-off they are added to the ledger so partial outputs are purged too
    // (Req 14.3).
    const outputR2Keys = new Set();
    // Uploaded Music_Track / Sticker R2 asset keys for the whole request. Stored
    // on the session so delivery-time cleanup re-attempts their removal (Req 14.2).
    const assetR2Keys = [];
    // Set true once outputs have been handed to the session for delivery.
    let deliveryHandedOff = false;
    // Set when a Render_Engine failure occurs so its source upload is retained
    // for the no-editor compression flow (Req 15.7).
    let renderFailureSourcePath = null;

    // Route each retained upload to its OWN recipe by upload key (Property 2 /
    // Req 1.3, 1.4). A file without a matching key is routed as skipped
    // (recipe === null → byte-identical legacy path, Req 1.2/1.5).
    const routed = routeRecipes(acceptedFiles, recipes);
    // Optional set of client-validated asset ids (music/sticker uploads that
    // passed the /api/music/validate boundary). Full asset wiring lands later.
    const validatedAssets = req.body.assets;

    try {
      // ---- Phase A: download, probe, route, validate, and PLAN each video. ----
      // No encode (Compression_Pass) is started in this phase, so a recipe that
      // fails validation rejects the whole request before any encode runs
      // (Req 2.7, 3.8).
      const prepared = await Promise.all(
        acceptedFiles.map(async (fileInfo, index) => {
          const { key, originalName, size } = fileInfo;
          const localInputPath = safeObjectPath(key);
          console.log(`📹 [${index + 1}/${acceptedFiles.length}] Processing: ${originalName} (${(size / 1024 / 1024).toFixed(1)}MB)`);
          if (!fs.existsSync(localInputPath)) throw new Error(`Uploaded file not found: ${key}`);
          requestContext.addLocalPath(localInputPath);

          const duration = await getVideoDuration(localInputPath, fileInfo.durationHint);
          const dimensions = await getVideoDimensions(localInputPath);

          // Look up this video's recipe (null => skipped / legacy path).
          const recipe = routed[index] ? routed[index].recipe : null;

          // Register every uploaded Music_Track / Sticker R2 asset referenced by
          // the recipe so the request `finally` purges them (Req 14.4), and carry
          // them onto the session for delivery-time re-attempt (Req 14.2).
          for (const assetKey of collectAssetR2Keys(recipe)) {
            requestContext.addR2Key(assetKey);
            assetR2Keys.push(assetKey);
          }

          if (recipe) {
            // Validate BEFORE any encode. On the first fault, reject with the
            // offending field + permitted bound and start NO Compression_Pass
            // (Req 2.7, 3.8). The downloaded source stays on local disk.
            const availableAssets = collectAvailableAssets(recipe, validatedAssets);
            const validation = validateRecipe(recipe, {
              sourceDuration: duration,
              availableAssets,
            });
            if (!validation.ok) {
              const err = new Error(
                `Recipe validation failed for "${originalName}": ${validation.error.reason}`
              );
              err.isRecipeValidationError = true;
              err.validationError = validation.error;
              err.fileName = originalName;
              throw err;
            }
          }

          // ---- Stage 1: resolve overlay/music asset files for the graph. ----
          // Built-in stickers (/stickers/...) and library audio (/library/...)
          // resolve to local public files; uploaded assets (carrying an R2 key)
          // are downloaded to temp and registered for request cleanup. The
          // resulting map is consumed by planRender via meta.assetPaths.
          let assetPaths;
          if (recipe) {
            const { localPaths, remotes } = planRecipeAssets(recipe, {
              publicDir: path.join(__dirname, 'public'),
              tmpDir: 'uploads',
              genId: uuidv4,
            });
            assetPaths = Object.assign({}, localPaths);
            for (const r of remotes) {
              requestContext.addLocalPath(r.tmpPath);
              const assetSource = safeObjectPath(r.key);
              if (!fs.existsSync(assetSource)) throw new Error(`Uploaded asset not found: ${r.key}`);
              await fs.promises.copyFile(assetSource, r.tmpPath);
              assetPaths[r.ref] = r.tmpPath;
            }

            // ---- Stage 3: convert animated .tgs stickers to alpha .webp. ----
            // ffmpeg can't render Lottie, so each .tgs is rasterized to an
            // animated webp (reusing the Stage 1 webp overlay path). Built-in
            // stickers are cached on disk and rendered once; uploaded ones go to
            // a request temp file. A render failure leaves the .tgs unmapped so
            // only that sticker is dropped — the rest of the edit still renders.
            if (tgsRaster.available()) {
              const stickerList = Array.isArray(recipe.stickers) ? recipe.stickers : [];
              const dropped = [];
              for (const s of stickerList) {
                const ref = s && s.assetRef;
                if (typeof ref !== 'string' || !/\.tgs$/i.test(ref) || !assetPaths[ref]) continue;
                try {
                  const srcTgs = assetPaths[ref];
                  const isStatic = ref.indexOf('/stickers/') === 0 || ref.indexOf('/library/') === 0;
                  if (isStatic) {
                    const cached = path.join('tgs-cache', path.basename(ref).replace(/\.tgs$/i, '') + '.apng');
                    if (!fs.existsSync(cached)) await tgsRaster.renderTgsToApng(srcTgs, cached);
                    assetPaths[ref] = cached;
                  } else {
                    const apngOut = path.join('uploads', `asset_${uuidv4()}.apng`);
                    await tgsRaster.renderTgsToApng(srcTgs, apngOut);
                    requestContext.addLocalPath(apngOut);
                    assetPaths[ref] = apngOut;
                  }
                } catch (tgsErr) {
                  console.warn(`tgs render failed for ${ref}: ${tgsErr.message}`);
                  delete assetPaths[ref];
                  dropped.push(s); // drop just this sticker; keep the rest of the edit
                }
              }
              if (dropped.length) recipe.stickers = stickerList.filter((s) => dropped.indexOf(s) === -1);
            }

            // Drop any overlay/music asset we could NOT resolve to a local file
            // (e.g. an upload that didn't finish, leaving a client-local ref) so
            // a missing input never fails the whole encode — the rest of the
            // edits still render. Mirrors the dev-render harness behaviour.
            if (Array.isArray(recipe.stickers)) {
              recipe.stickers = recipe.stickers.filter((s) => s && assetPaths[s.assetRef]);
            }
            if (recipe.audio && recipe.audio.music && !assetPaths[recipe.audio.music.assetRef]) {
              console.warn(`Dropping unresolved music asset "${recipe.audio.music.assetRef}" for "${originalName}"`);
              recipe.audio.music = null;
            } else if (recipe.audio && recipe.audio.music) {
              console.log(`🎵 Music resolved for "${originalName}": ${recipe.audio.music.assetRef} -> ${assetPaths[recipe.audio.music.assetRef]}`);
            }
          }

          // Rasterize each Text_Overlay to a transparent PNG (Stage 2) and map
          // it by overlay id for the render graph. Files are registered for
          // request cleanup. Skipped when the recipe has no text overlays.
          let textRasterPaths;
          if (recipe && Array.isArray(recipe.textOverlays) && recipe.textOverlays.length) {
            textRasterPaths = {};
            for (const t of recipe.textOverlays) {
              const textPng = path.join('uploads', `text_${uuidv4()}.png`);
              await rasterizeTextOverlay(t, textPng);
              requestContext.addLocalPath(textPng);
              textRasterPaths[t.id] = textPng;
            }
          }

          // Build the single-encode RenderPlan. recipe === null yields the
          // byte-identical legacy plan (Property 1); a recipe folds its edits
          // into one Compression_Pass (Req 2.6, 12.3).
          const plan = planRender(recipe, {
            width: dimensions.width,
            height: dimensions.height,
            duration,
            hasSourceAudio: dimensions.hasAudio !== false,
            key,
            path: localInputPath,
            assetPaths,
            textRasterPaths,
          });
          // Loop animated .webp sticker inputs for the full clip (Stage 1),
          // bounded to the planned output duration so the encode terminates.
          markLoopingImageInputs(plan, Number.isFinite(plan.plannedDuration) ? plan.plannedDuration : duration);

          return { fileInfo, localInputPath, duration, dimensions, recipe, plan };
        })
      );

      // Optional "Longer clips — 60s per part" mode (non-edited path only):
      // split at ~59s and encode 720×1280 so each part still fits under 16MB.
      // Default (off) keeps the byte-identical 1080×1920 / ~29s behavior.
      const longClips = req.body.longClips === true || req.body.longClips === 'true';
      const skipClipLen = longClips ? LONG_CLIP_SECONDS : 29;
      const skipEnc = longClips ? LONG_CLIP_ENC : {};
      if (longClips) console.log('🎚️ Longer-clips mode ON: 720p, ~59s parts');

      // Premium means a currently active paid subscription. An active free
      // trial or a paid plan queued for a future date stays in the normal queue.
      const isPremiumProcessing = accessGate.access?.status === 'SUBSCRIBED';
      releaseProcessingPrioritySlot = await acquireProcessingPrioritySlot(isPremiumProcessing);
      console.log(
        `⏱️ Encoding queue: ${isPremiumProcessing ? 'PREMIUM' : 'NORMAL'} ` +
        `(${activeProcessingJobs}/${PROCESSING_JOB_CONCURRENCY} active; ` +
        `${processingPriorityQueue.length} waiting)`
      );

      // ---- Phase B: encode each prepared video through the EncodeSemaphore. ----
      // Skip/legacy plans (filterComplex === '') run the unchanged
      // compressVideo/splitVideo path so output stays byte-for-byte identical to
      // the no-editor flow. Recipe plans execute the -filter_complex graph from
      // the RenderPlan / per-chunk ChunkRenderPlan. Every ffmpeg invocation is
      // gated by the shared semaphore, so concurrent encodes across all files and
      // chunks never exceed the CPU-derived Concurrency_Limit (Req 13.1/13.2).
      const allGroupResults = await Promise.all(
        prepared.map(async ({ fileInfo, localInputPath, duration, dimensions, recipe, plan }) => {
          let retainSource = false;
          try {
            const isSkip = !plan.filterComplex; // legacy/skip plan
            const plannedDuration = Number.isFinite(plan.plannedDuration)
              ? plan.plannedDuration
              : duration;

            // ----- Skip / legacy path (byte-identical) -----
            if (isSkip) {
              // Longer-clips mode: ~59s parts at 720p (a <=59s video becomes a
              // single 720p part). Default: ~29s parts at full 1080p HD.
              const clipLen = longClips ? LONG_CLIP_SECONDS : 29;
              const enc = longClips ? LONG_CLIP_ENC : {};
              if (duration <= clipLen) {
                const outputFileName = `compressed_${uuidv4()}.mp4`;
                const outputPath = path.join('compressed', outputFileName);
                requestContext.addLocalPath(outputPath);
                await compressVideo(localInputPath, outputPath, duration, dimensions.height, enc);
                const url = await uploadToR2(outputPath, outputFileName);
                // Output is a delivery artifact — tracked separately so the
                // request `finally` does NOT purge it before delivery (Req 14.2).
                outputR2Keys.add(outputFileName);
                await fs.promises.unlink(outputPath).catch(() => { });
                return [{ fileName: outputFileName, url }];
              }
              const chunkPaths = await splitVideo(localInputPath, 'compressed', duration, clipLen, dimensions.height, enc);
              const chunkResults = [];
              for (let i = 0; i < chunkPaths.length; i++) {
                const chunkPath = chunkPaths[i];
                const chunkFileName = path.basename(chunkPath);
                requestContext.addLocalPath(chunkPath);
                const url = await uploadToR2(chunkPath, chunkFileName);
                outputR2Keys.add(chunkFileName);
                await fs.promises.unlink(chunkPath).catch(() => { });
                chunkResults.push({ fileName: chunkFileName, url });
              }
              return chunkResults;
            }

            // ----- Recipe path: execute the filter graph in one encode -----
            // A Render_Engine failure here is surfaced with the specific video
            // identified and its source retained for the no-editor flow (Req
            // 15.6/15.7). Cleanup of partial render artifacts still runs.
            try {
              if (plannedDuration <= CLIP_DURATION_LIMIT) {
                const outputFileName = `compressed_${uuidv4()}.mp4`;
                const outputPath = path.join('compressed', outputFileName);
                requestContext.addLocalPath(outputPath);
                await encodeRecipePlan(plan, outputPath, FULL_VIDEO_TIMEOUT_MS, `Edited video "${fileInfo.originalName}"`);
                const url = await uploadToR2(outputPath, outputFileName);
                outputR2Keys.add(outputFileName);
                await fs.promises.unlink(outputPath).catch(() => { });
                return [{ fileName: outputFileName, url }];
              }

              // Trimmed/edited duration exceeds the clip limit → split into chunks
              // and render overlays in every chunk via planChunk (Req 4.6, 11.x).
              const count = chunkCount(plannedDuration, CLIP_DURATION_LIMIT);
              const indexedResults = await Promise.all(
                Array.from({ length: count }, (_, i) => i).map(async (i) => {
                  const chunkPlan = planChunk(plan, i, CLIP_DURATION_LIMIT);
                  markLoopingImageInputs(chunkPlan, CLIP_DURATION_LIMIT);
                  const chunkFileName = `chunk_${uuidv4()}.mp4`;
                  const chunkPath = path.join('compressed', chunkFileName);
                  requestContext.addLocalPath(chunkPath);
                  await encodeRecipePlan(chunkPlan, chunkPath, CHUNK_TIMEOUT_MS, `Edited chunk ${i + 1} of "${fileInfo.originalName}"`);
                  const url = await uploadToR2(chunkPath, chunkFileName);
                  outputR2Keys.add(chunkFileName);
                  await fs.promises.unlink(chunkPath).catch(() => { });
                  return { index: i, fileName: chunkFileName, url };
                })
              );
              // Preserve chunk order regardless of completion order.
              indexedResults.sort((a, b) => a.index - b.index);
              return indexedResults.map(({ fileName, url }) => ({ fileName, url }));
            } catch (renderErr) {
              // Retain this source upload so the user can still post via the
              // no-editor compression flow (Req 15.7).
              retainSource = true;
              renderFailureSourcePath = localInputPath;
              const err = new Error(
                `Failed to apply edits to "${fileInfo.originalName}": ${renderErr.message}`
              );
              err.isRenderError = true;
              err.fileName = fileInfo.originalName;
              err.cause = renderErr.message;
              throw err;
            }
          } finally {
            // Remove the local source unless it must be retained for the
            // no-editor fallback after a Render_Engine failure (Req 15.7).
            if (!retainSource) {
              await fs.promises.unlink(localInputPath).catch(() => { });
            }
          }
        })
      );

      if (releaseProcessingPrioritySlot) {
        releaseProcessingPrioritySlot();
        releaseProcessingPrioritySlot = null;
      }

      for (const group of allGroupResults) {
        for (const { fileName, url } of group) {
          r2Files.push({ fileName, url });
        }
      }
      console.log(`🎉 All ${r2Files.length} file(s) ready!`);

      const expiryTimer = setTimeout(async () => {
        const session = sessions.get(activationCode);
        if (session) {
          for (const file of session.files) {
            try { await deleteFromR2(file.fileName); } catch (err) { console.error('R2 cleanup:', err.message); }
          }
          sessions.delete(activationCode);
          console.log(`⏰ Session expired: ${activationCode}`);
        }
      }, 300000);

      // Get user caption (optional)
      const userCaption = typeof req.body.caption === 'string' ? req.body.caption.trim().slice(0, 1000) : '';
      console.log(`📝 Caption received: ${userCaption ? JSON.stringify(userCaption) : '(none)'}`);

      setDeliveryStatus(activationCode, 'pending');
      sessions.set(activationCode, {
        files: r2Files,
        assetKeys: assetR2Keys,  // music/sticker R2 assets for delivery-time cleanup (Req 14.2)
        visitorId: accessGate.visitorId || null,
        authPhone: accessGate.user?.phone || null,
        firstUse: accessGate.firstUse === true,
        createdAt: Date.now(),
        status: 'pending',
        expiryTimer: expiryTimer,
        caption: userCaption,  // 🆕 store user's caption
      });
      // Outputs are now owned by the session for delivery; the request `finally`
      // must NOT purge the R2 output clips (Req 14.2).
      deliveryHandedOff = true;

      const cleanNumber = (process.env.WHATSAPP_BUSINESS_NUMBER || '').replace(/\D/g, '');
      const waText = buildInboundText(activationCode);
      const waLink = `https://wa.me/${cleanNumber}?text=${encodeURIComponent(waText)}`;
      reply.json({
        success: true,
        activationCode,
        waLink,
        fileCount: r2Files.length,
        firstUse: accessGate.firstUse === true,
        authenticationRequiredNextUse: accessGate.firstUse === true,
      });

    } catch (processingError) {
      // A recipe that failed validation rejects the request with HTTP 400,
      // naming the offending field and its permitted bound. No Compression_Pass
      // was started (validation runs in Phase A, before any encode) — Req 3.8.
      if (processingError && processingError.isRecipeValidationError) {
        console.warn(`✗ Recipe validation failed: ${processingError.message}`);
        for (const key of uploadedKeys) {
          try { await deleteFromR2(key); } catch (cleanupErr) { console.error(`Failed cleanup ${key}:`, cleanupErr.message); }
        }
        const ve = processingError.validationError || {};
        return reply.status(400).json({
          error: processingError.message,
          field: ve.field,
          bound: ve.bound,
          fileName: processingError.fileName,
        });
      }
      // A Render_Engine failure identifies the specific video and the cause, and
      // leaves that video's source available for the no-editor compression flow
      // (Req 15.6/15.7). Partial render artifacts are purged by the `finally`
      // Cleanup_Process below.
      if (processingError && processingError.isRenderError) {
        console.error(`✗ Render failed: ${processingError.message}`);
        return reply.status(422).json({
          error: processingError.message,
          fileName: processingError.fileName,
          cause: processingError.cause,
        });
      }
      console.error('✗ Processing error, cleaning up R2 uploads...');
      for (const key of uploadedKeys) {
        try { await deleteFromR2(key); } catch (cleanupErr) { console.error(`Failed cleanup ${key}:`, cleanupErr.message); }
      }
      throw processingError;
    } finally {
      if (releaseProcessingPrioritySlot) {
        releaseProcessingPrioritySlot();
        releaseProcessingPrioritySlot = null;
      }

      // Cleanup_Process (Req 14.1/14.3/14.4): purge every registered local path
      // and non-output R2 asset (music/sticker), logging per-item failures and
      // continuing (Req 14.6), then verifying the request prefix is empty in R2.
      //   - On SUCCESS (deliveryHandedOff): the R2 output clips are owned by the
      //     session and are NOT purged here; delivery cleanup removes them later.
      //   - On ANY FAILURE before hand-off: partial output clips already uploaded
      //     to R2 are added to the ledger so nothing leaks (Req 14.3).
      //   - On a Render_Engine failure: the failed video's source is retained for
      //     the no-editor flow (Req 15.7).
      if (!deliveryHandedOff) {
        for (const key of outputR2Keys) {
          requestContext.addR2Key(key);
        }
      }
      if (renderFailureSourcePath) {
        requestContext.localPaths.delete(renderFailureSourcePath);
      }
      try {
        await cleanupRequest(requestContext, cleanupDeps);
      } catch (cleanupErr) {
        console.error(`Cleanup failed for ${activationCode}:`, cleanupErr.message);
      }
      activeRequests.delete(requestContext);
    }
  } catch (error) {
    console.error('✗ Process error:', error);
    reply.status(500).json({ error: error.message });
  }
});

// ========================
// ERROR HANDLER
// ========================
app.use((err, req, res, next) => {
  if (err?.message === 'Request aborted' || err?.code === 'ECONNABORTED' || err?.type === 'request.aborted') {
    console.warn('Client aborted - cleaning up');
    if (!res.headersSent) res.status(499).end();
    return;
  }
  console.error('Unhandled error:', err);
  if (!res.headersSent) res.status(500).json({ error: err.message });
});

// ========================
// START SERVER
// ========================
const server = app.listen(PORT, () => {
  console.log(`
================================
→ MIABE STATUS HD Server Running!
================================
Local: http://localhost:${PORT}
================================
  `);
  // One-time WhatsApp re-link: when RESET_BAILEYS=true, wipe the saved session
  // on boot so Baileys starts fresh and prints a new pairing code / QR for the
  // number in WHATSAPP_BUSINESS_NUMBER. Do this ONCE at process start (never on
  // reconnect, which would break an in-progress link). Set RESET_BAILEYS back to
  // false after linking, or every restart will unlink the bot.
  if (process.env.RESET_BAILEYS === 'true') {
    try {
      // Delete the CONTENTS of the auth dir, not the folder itself: on a mounted
      // volume, removing the mount point fails with EBUSY.
      const dir = BAILEYS_AUTH_DIR;
      let removed = 0;
      if (fs.existsSync(dir)) {
        for (const entry of fs.readdirSync(dir)) {
          fs.rmSync(path.join(dir, entry), { recursive: true, force: true });
          removed++;
        }
      }
      console.log(`🔑 RESET_BAILEYS=true → cleared ${removed} saved session file(s). A new pairing code / QR will appear below. Set RESET_BAILEYS=false after linking.`);
    } catch (e) {
      console.error('Failed to clear baileys_auth:', e.message);
    }
  }

  // Surface the Baileys media dependency explicitly. Without a bare `ffmpeg` on
  // PATH, Baileys silently ships videoMessages with no thumbnail/dimensions/
  // duration, and WhatsApp re-encodes them at the Status step instead of
  // forwarding them through.
  if (ffmpegOnPath()) {
    console.log('✓ ffmpeg is on PATH — Baileys can attach video thumbnail + metadata.');
  } else {
    console.warn('!!! ffmpeg is NOT on PATH. Baileys cannot build video thumbnails, so sent videos will lack metadata and WhatsApp will re-encode them on Status. Check the Dockerfile symlink.');
  }

  // Start Baileys after Express is up so QR shows in logs
  startBaileys().catch(err => console.error('Baileys startup failed:', err));

  // Startup_Sweep: reclaim disk by deleting orphan temp files in uploads/,
  // compressed/, and assets/ that are not tied to an active in-progress request
  // (Req 14.5). At boot nothing is in-flight, so leftovers from a prior run are
  // removed; live work is retained via the activeRequests ledger set.
  startupSweep(activeRequests, makeSweepDeps({ fs, baseDir: process.cwd() }))
    .then((sweep) => {
      console.log(`🧹 Startup sweep: removed ${sweep.deleted.length} orphan file(s), retained ${sweep.retained.length}.`);
    })
    .catch((err) => console.error('Startup sweep failed:', err.message));

  // R2 orphan sweep: reclaim never-redeemed outputs/inputs/music left in R2 by a
  // prior run that restarted before its in-memory expiry timers fired. Runs once
  // at boot and every 15 minutes thereafter. Only ephemeral prefixes are listed
  // and only objects older than 30 min and not owned by a live session are
  // removed, so in-flight work and permanent assets are never affected.
  sweepOrphanR2Objects().catch((err) => console.error('R2 orphan sweep failed:', err.message));
  setInterval(() => {
    sweepOrphanR2Objects().catch((err) => console.error('R2 orphan sweep failed:', err.message));
  }, 15 * 60 * 1000);
});

server.requestTimeout = 0;
server.headersTimeout = 620000;
server.keepAliveTimeout = 610000;
