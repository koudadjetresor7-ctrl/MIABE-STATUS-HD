# POWERD BY TRESOR — V2

Version R2-free de l'application WhatsApp Status HD.

## Ce qui change
- Stockage vidéo/audio local sur le serveur : aucun Cloudflare R2 requis.
- Upload direct navigateur → Node.js avec progression.
- FFmpeg + FFprobe pour le traitement vidéo.
- Baileys pour la livraison WhatsApp.
- Interface modernisée POWERD BY TRESOR.
- Paiement CinetPay prêt à activer (XOF, Mobile Money TMoney/Flooz selon les moyens activés sur votre compte).

## Installation locale
1. Installer Node.js.
2. Installer FFmpeg/FFprobe et vérifier `ffmpeg -version` / `ffprobe -version`.
3. Dans ce dossier : `npm ci`
4. Copier `.env.example` vers `.env`.
5. Laisser `PAYMENT_REQUIRED=false` pour tester gratuitement en local.
6. Lancer `npm start`.
7. Associer le numéro `+22872204628` à WhatsApp avec le code de liaison affiché.
8. Après association, passer `RESET_BAILEYS=false`.

## Paiement CinetPay
Pour activer le paiement :
- `PAYMENT_REQUIRED=true`
- renseigner `CINETPAY_API_KEY`
- renseigner `CINETPAY_SITE_ID`
- définir `PUBLIC_BASE_URL` sur l'URL HTTPS publique du serveur
- choisir le montant avec `PAYMENT_AMOUNT_XOF` (multiple de 5)

Les secrets ne doivent jamais être placés dans le frontend ou commités dans Git.

## Stockage
Les objets temporaires sont stockés sous `storage/objects/`. Le serveur les nettoie après traitement/livraison et effectue un sweep des objets orphelins.

### Important about the “0 €” goal
The application no longer needs Cloudflare R2 or an upload Worker. The code itself can therefore run locally without an R2 subscription. A public 24/7 deployment can still have hosting/provider costs, and CinetPay charges transaction fees when real payments are collected.
