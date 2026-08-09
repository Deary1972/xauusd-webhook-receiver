"""
Webhook Receiver — Jembatan TradingView Alert -> Pipeline Otomatis
======================================================================
Ini komponen yang bikin sistem BENERAN otomatis konek ke TradingView, lewat
jalur RESMI TradingView (bukan scraping): Pine Script alert -> webhook HTTP.

Alur lengkap:
    1. `pinescript_webhook_feed.pine` dipasang di 6 chart TradingView
           (satu per timeframe: D1, H4, H1, M30, M15, M5), masing-masing dengan
                  1 alert ("Any alert() function call") yang webhook URL-nya menunjuk
                         ke server ini: https://<server-kamu>/webhook/<WEBHOOK_SECRET>
                             2. Tiap kali candle di salah satu chart itu close, TradingView kirim
                                    POST JSON ke endpoint /webhook/<secret> di server ini.
                                        3. Server ini simpan candle itu ke CSV lewat
                                               tradingview_data_connector.append_candle() (upsert, bukan duplikat).
                                                   4. Kalau candle yang masuk adalah timeframe utama (default M15, bisa
                                                          diganti lewat env PRIMARY_TIMEFRAME), otomatis jalankan
                                                                 main_orchestrator.run_pipeline() sekali lalu log hasilnya sebagai
                                                                        ALERT (bukan auto-execute trade — tetap butuh review manual).

                                                                        BATASAN PENTING — WAJIB DIBACA SEBELUM DEPLOY:
                                                                          1. Webhook alert TradingView BUTUH plan berbayar (Essential ke atas).
                                                                               Free plan tidak bisa kirim webhook sama sekali (dicek Agustus 2026).
                                                                                 2. Endpoint ini HARUS bisa diakses dari internet publik (server
                                                                                      TradingView yang kirim POST ke sini), jadi TIDAK BISA dijalankan di
                                                                                           sandbox/computer lokal biasa yang tidak public-facing. Deploy ke
                                                                                                VPS/cloud kecil (Railway, Render, Fly.io, VPS pribadi, dst) dengan
                                                                                                     HTTPS aktif (TradingView mengharuskan webhook URL pakai HTTPS).
                                                                                                       3. WEBHOOK_SECRET WAJIB di-set ke string acak yang susah ditebak, dan
                                                                                                            JANGAN publish URL webhook-mu (termasuk secret-nya) di tempat umum —
                                                                                                                 endpoint ini menerima data mentah yang langsung dipakai untuk analisa,
                                                                                                                      jadi siapa pun yang tahu URL-nya bisa suntik data candle palsu kalau
                                                                                                                           tidak dilindungi.
                                                                                                                             4. Ini BUKAN auto-execute trade. Sesuai main_orchestrator.py, output
                                                                                                                                  tetap berupa ALERT (probabilitas + trading plan) untuk review manual
                                                                                                                                       sebelum entry — bukan perintah eksekusi otomatis ke broker.
                                                                                                                                         5. Karena TradingView tidak expose history candle lewat webhook (cuma
                                                                                                                                              bar yang baru close), CSV di ./tradingview_data/ awalnya kosong
                                                                                                                                                   sampai ada beberapa bar masuk. Untuk cold-start, tetap lakukan
                                                                                                                                                        export manual (lihat tradingview_data_connector.py) sekali di awal
                                                                                                                                                             supaya ada histori cukup buat ATR/swing detection sebelum webhook
                                                                                                                                                                  mulai mengalir.
                                                                                                                                                                  
                                                                                                                                                                  Setup:
                                                                                                                                                                      export ANTHROPIC_API_KEY="sk-ant-..."
    export WEBHOOK_SECRET="ganti-dengan-string-acak-panjang"
    export PRIMARY_TIMEFRAME="M15"     # opsional, default M15
        pip install flask pandas numpy pytz anthropic requests --break-system-packages
            python webhook_receiver.py
                # server jalan di 0.0.0.0:8080 -- taruh di belakang reverse proxy HTTPS
                    # (Caddy/nginx/Cloudflare Tunnel/dll) kalau deploy sendiri di VPS.

                    Format payload yang dikirim Pine Script (lihat pinescript_webhook_feed.pine):
                        {
                              "symbol": "XAUUSD", "timeframe": "M15", "time": 1754611200,
                                    "open": 2340.5, "high": 2345.1, "low": 2338.2, "close": 2341.9,
                                          "volume": 15234
                                              }
                                              """

from __future__ import annotations
import os
import logging

from flask import Flask, request, jsonify

import tradingview_data_connector as tvc

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("webhook_receiver")

app = Flask(__name__)

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
PRIMARY_TIMEFRAME = os.environ.get("PRIMARY_TIMEFRAME", "M15")
VALID_TIMEFRAMES = set(tvc.TIMEFRAME_KEYS)

if not WEBHOOK_SECRET:
    log.warning(
        "WEBHOOK_SECRET belum di-set! Endpoint webhook TIDAK terproteksi. "
        "JANGAN deploy ke publik seperti ini -- set env WEBHOOK_SECRET dulu."
    )


def _run_pipeline_safely() -> str | None:
    """Jalankan main_orchestrator.run_pipeline() (import lazy supaya modul ini
    tetap bisa dipakai sekadar nampung data candle walau ANTHROPIC_API_KEY
    atau koneksi kalender berita sedang bermasalah)."""
    try:
        import main_orchestrator as orch
        result = orch.run_pipeline()
        message = orch.format_alert_message(result)
        log.info("=== HASIL PIPELINE (trigger otomatis dari webhook) ===\n%s", message)
        return message
    except Exception as e:
        log.error(f"Pipeline gagal jalan setelah webhook trigger: {e}")
        return None


@app.route("/webhook/<secret>", methods=["POST"])
def receive_webhook(secret: str):
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        log.warning("Percobaan akses webhook dengan secret salah -- ditolak.")
        return jsonify({"error": "unauthorized"}), 403

    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "body harus JSON"}), 400

    symbol = payload.get("symbol", "XAUUSD")
    timeframe = payload.get("timeframe")
    if timeframe not in VALID_TIMEFRAMES:
        return jsonify({
            "error": f"timeframe '{timeframe}' tidak dikenali, harus salah satu dari {sorted(VALID_TIMEFRAMES)}"
        }), 400

    required = ["time", "open", "high", "low", "close"]
    missing = [k for k in required if k not in payload]
    if missing:
        return jsonify({"error": f"payload kurang field: {missing}"}), 400

    candle = {
        "time": payload["time"],
        "open": payload["open"],
        "high": payload["high"],
        "low": payload["low"],
        "close": payload["close"],
        "volume": payload.get("volume", 0),
    }

    tvc.append_candle(symbol, timeframe, candle)
    log.info(f"Candle diterima & disimpan: {symbol} {timeframe} @ {candle['time']}")

    triggered = False
    if timeframe == PRIMARY_TIMEFRAME:
        _run_pipeline_safely()
        triggered = True

    return jsonify({"status": "ok", "pipeline_triggered": triggered}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "alive"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log.info(f"Webhook receiver jalan di 0.0.0.0:{port} (primary timeframe: {PRIMARY_TIMEFRAME})")
    log.info("INGAT: endpoint ini baru bisa dipakai TradingView kalau sudah public-facing (HTTPS).")
    app.run(host="0.0.0.0", port=port)
