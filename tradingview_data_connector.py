"""
TradingView Data Connector — Penghubung Data Manual dari TradingView
====================================================================
PENTING: TradingView TIDAK menyediakan API Python publik untuk menarik data
candle OHLCV secara otomatis (beda dengan MetaTrader5 yang punya connector
Python resmi). Jadi modul ini BUKAN koneksi live ke terminal — ini alur
MANUAL berbasis file:

  1. Buka chart XAUUSD di TradingView, set timeframe yang dibutuhkan
     (D1, H4, H1, M30, M15, M5).
  2. Export data candle:
       - Kalau kamu subscriber berbayar (Pro/Premium ke atas), klik kanan
         di chart -> "Export chart data..." -> simpan sebagai CSV.
       - Kalau akun gratis (tidak ada tombol export), catat candle manual
         dari chart (OHLC per bar) lalu masukkan lewat manual_candles() di
         bawah, atau ketik langsung ke CSV dengan format yang sama.
  3. Taruh file CSV di folder data (default: ./tradingview_data/) dengan
     nama <SYMBOL>_<TIMEFRAME>.csv, misal: XAUUSD_D1.csv, XAUUSD_H1.csv, dst.
  4. Panggil fetch_all_timeframes() seperti biasa — fungsi ini baca CSV-CSV
     itu dan format ke df_by_tf dict yang IDENTIK dengan versi MT5 lama,
     jadi xauusd_detection_engine.py dkk TIDAK perlu diubah sama sekali.

Install:
    pip install pandas pytz --break-system-packages
    (dependency MetaTrader5 sudah dihapus total dari modul ini)

Format CSV yang diharapkan (header wajib ada, urutan kolom bebas):
    time,open,high,low,close,Volume
    2026-08-08 00:00:00,2340.5,2345.1,2338.2,2341.9,15234
    ...
    Kolom `time` boleh UNIX timestamp (detik, seperti hasil export asli
    TradingView) ATAU string tanggal yang bisa di-parse pandas.

BATASAN YANG PERLU DISADARI:
  - Ini bukan data real-time otomatis — kamu (atau scheduler.py yang cuma
    mengingatkan) yang harus rutin export/update CSV-nya secara manual.
  - Kalau butuh update tiap M15 close secara benar-benar otomatis tanpa
    campur tangan manual, MetaTrader5 (via broker) atau data vendor
    berbayar (mis. lewat API resmi) tetap satu-satunya opsi otomatis —
    TradingView memang tidak menyediakan itu untuk akun retail biasa.
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

WITA = ZoneInfo("Asia/Makassar")

DEFAULT_DATA_DIR = Path("./tradingview_data")

TIMEFRAME_KEYS = ["D1", "H4", "H1", "M30", "M15", "M5"]


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Samakan nama kolom & index datetime UTC, apa pun variasi header CSV TradingView."""
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    rename_map = {"vol": "volume", "vol.": "volume", "tick_volume": "volume"}
    df = df.rename(columns=rename_map)

    if "time" not in df.columns:
        raise ValueError("CSV harus punya kolom 'time'.")

    # time bisa UNIX timestamp (angka) atau string tanggal
    if pd.api.types.is_numeric_dtype(df["time"]):
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    else:
        df["time"] = pd.to_datetime(df["time"], utc=True)

    df = df.set_index("time").sort_index()

    required = ["open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV kurang kolom wajib: {missing}")

    if "volume" not in df.columns:
        df["volume"] = 0

    return df[["open", "high", "low", "close", "volume"]]


def fetch_ohlcv(symbol: str, timeframe_key: str, n_bars: int = 500,
    data_dir: "Path | str" = DEFAULT_DATA_DIR) -> pd.DataFrame:
    """
    Baca file CSV hasil export TradingView untuk symbol & timeframe tertentu.
    Nama file yang dicari: <data_dir>/<symbol>_<timeframe_key>.csv
    Ambil n_bars candle TERAKHIR dari file itu.
    """
    data_dir = Path(data_dir)
    path = data_dir / f"{symbol}_{timeframe_key}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {path}\n"
            f"Export dulu candle {symbol} timeframe {timeframe_key} dari TradingView "
            f"dan simpan dengan nama file itu (lihat docstring modul ini)."
        )

    raw = pd.read_csv(path)
    df = _normalize_ohlcv(raw)
    return df.tail(n_bars)


def fetch_all_timeframes(symbol: str = "XAUUSD", n_bars: int = 500,
    data_dir: "Path | str" = DEFAULT_DATA_DIR) -> dict[str, pd.DataFrame]:
    """
    Baca semua timeframe (D1 -> H4 -> H1 -> M30 -> M15 -> M5) dari CSV export
    TradingView di data_dir. Return dict {"D1": df, ...} siap dipakai
    xauusd_detection_engine.build_analysis_snapshot() — format IDENTIK
    dengan versi mt5_data_connector lama.
    """
    result = {}
    for tf_key in TIMEFRAME_KEYS:
        try:
            result[tf_key] = fetch_ohlcv(symbol, tf_key, n_bars, data_dir)
            print(f"  {tf_key}: {len(result[tf_key])} candle dibaca dari CSV, terakhir {result[tf_key].index[-1]}")
        except Exception as e:
            print(f"  {tf_key}: GAGAL - {e}")
    return result


def fetch_dxy(n_bars: int = 500, timeframe_key: str = "H1",
    data_dir: "Path | str" = DEFAULT_DATA_DIR) -> Optional[pd.DataFrame]:
    """
    Coba baca data DXY dari CSV export TradingView. Nama file yang dicoba:
    DXY_<timeframe_key>.csv, USDX_<timeframe_key>.csv, TVC-DXY_<timeframe_key>.csv
    Export chart "U.S. Dollar Index" dari TradingView (simbol umum: TVC:DXY).
    """
    for candidate_symbol in ["DXY", "USDX", "TVC-DXY"]:
        try:
            df = fetch_ohlcv(candidate_symbol, timeframe_key, n_bars, data_dir)
            print(f"DXY ditemukan lewat file: {candidate_symbol}_{timeframe_key}.csv")
            return df
        except FileNotFoundError:
            continue
    print("DXY tidak ditemukan. Export chart DXY (simbol TVC:DXY) dari TradingView dan "
          "simpan sebagai DXY_<timeframe>.csv di folder data.")
    return None


def manual_candles(rows: list[dict], symbol: str, timeframe_key: str,
    data_dir: "Path | str" = DEFAULT_DATA_DIR, save: bool = True) -> pd.DataFrame:
    """
    Alternatif kalau akun TradingView kamu tidak punya fitur export (free plan):
    ketik manual candle yang kamu baca dari chart sebagai list of dict, misal:

        manual_candles([
            {"time": "2026-08-08 00:00:00", "open": 2340.5, "high": 2345.1,
             "low": 2338.2, "close": 2341.9, "volume": 15234},
            ...,
        ], symbol="XAUUSD", timeframe_key="H1")

    Kalau save=True, otomatis disimpan jadi CSV di data_dir supaya bisa dibaca lagi
    lewat fetch_ohlcv()/fetch_all_timeframes() tanpa ketik ulang tiap kali.
    """
    df = _normalize_ohlcv(pd.DataFrame(rows))
    if save:
        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        out_path = data_dir / f"{symbol}_{timeframe_key}.csv"
        df.reset_index().to_csv(out_path, index=False)
        print(f"Tersimpan: {out_path}")
    return df


def append_candle(symbol: str, timeframe_key: str, candle: dict,
    data_dir: "Path | str" = DEFAULT_DATA_DIR, max_rows: int = 1000) -> pd.DataFrame:
    """
    Upsert SATU candle ke CSV <symbol>_<timeframe_key>.csv — dipakai oleh
    webhook_receiver.py setiap kali TradingView (via Pine Script alert)
    mengirim data candle baru. Kalau `time` sudah ada di file, baris lama
    di-replace (upsert), bukan duplikat — berguna kalau TradingView sempat
    kirim ulang bar yang sama sebelum benar-benar close.

    candle: dict dengan key time/open/high/low/close/volume (lihat
    _normalize_ohlcv untuk format `time` yang diterima).
    """
    if timeframe_key not in TIMEFRAME_KEYS:
        raise ValueError(f"Timeframe '{timeframe_key}' tidak dikenali. Pilihan: {TIMEFRAME_KEYS}")

    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f"{symbol}_{timeframe_key}.csv"

    new_row = _normalize_ohlcv(pd.DataFrame([candle]))

    if path.exists():
        existing = _normalize_ohlcv(pd.read_csv(path))
        existing = existing[~existing.index.isin(new_row.index)]
        combined = pd.concat([existing, new_row])
    else:
        combined = new_row

    combined = combined.sort_index()
    if len(combined) > max_rows:
        combined = combined.tail(max_rows)

    combined.reset_index().to_csv(path, index=False)
    return combined


def list_expected_files(symbol: str = "XAUUSD", data_dir: "Path | str" = DEFAULT_DATA_DIR) -> None:
    """Utility: cetak daftar file CSV yang perlu disiapkan sebelum jalanin pipeline."""
    data_dir = Path(data_dir)
    print(f"Siapkan file-file ini di folder '{data_dir}/' (export dari TradingView, chart {symbol}):")
    for tf in TIMEFRAME_KEYS:
        path = data_dir / f"{symbol}_{tf}.csv"
        status = "OK" if path.exists() else "BELUM ADA"
        print(f"  - {path.name}  [{status}]")


# ============================================================
# CONTOH PEMAKAIAN LENGKAP
# ============================================================

if __name__ == "__main__":
    print("--- Cek file CSV yang dibutuhkan ---")
    list_expected_files("XAUUSD")

    print("\n--- Membaca data XAUUSD semua timeframe dari CSV TradingView ---")
    df_by_tf = fetch_all_timeframes("XAUUSD", n_bars=500)

    print("\n--- Membaca DXY (opsional) ---")
    dxy_df = fetch_dxy()

    print("\nSelesai. df_by_tf siap dipakai ke xauusd_detection_engine.build_analysis_snapshot()")
