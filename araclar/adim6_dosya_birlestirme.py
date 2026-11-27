
import csv
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
from ortak import s3

istemci = s3()
con = duckdb.connect()

kayit_yolu = "/kit/kanit/z4_birlestirme_olcumleri.csv"
hedef_klasor = "z4_birlestirme/G211210575/"

try:
    # Eski deney sonuclarini koru.
    if Path(kayit_yolu).exists():
        raise SystemExit("Olcum CSV dosyasi zaten var.")

    kontrol = istemci.list_objects_v2(
        Bucket="islenmis",
        Prefix=hedef_klasor,
        MaxKeys=1
    )

    if kontrol.get("Contents"):
        raise SystemExit("Hedef klasorde dosya var. Islem durduruldu.")

    con.execute("INSTALL httpfs; LOAD httpfs;")

    con.execute("""
        SET s3_endpoint='s3:8333';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_access_key_id='bsm461';
        SET s3_secret_access_key='bsm461-gizli';
        SET s3_region='us-east-1';
    """)

    con.execute("SET threads=2")

    sonuclar = []
    baslangic = time.perf_counter()

    for i in range(14):
        tarih = (date(2026, 11, 12) + timedelta(days=i)).isoformat()

        kaynak_prefix = f"satis/tarih={tarih}/"
        kaynak = f"s3://islenmis/{kaynak_prefix}*.parquet"

        hedef_anahtar = (
            f"{hedef_klasor}tarih={tarih}/tek.parquet"
        )
        hedef = f"s3://islenmis/{hedef_anahtar}"

        cevap = istemci.list_objects_v2(
            Bucket="islenmis",
            Prefix=kaynak_prefix
        )

        eski_dosyalar = [
            d for d in cevap.get("Contents", [])
            if d["Key"].endswith(".parquet")
        ]

        if len(eski_dosyalar) != 7:
            raise RuntimeError("Beklenmeyen dosya sayisi: " + tarih)

        eski_boyut = sum(d["Size"] for d in eski_dosyalar)

        # Kayit sayisi ve toplam adedi kontrol et.
        eski_sonuc = con.execute(f"""
            SELECT COUNT(*), SUM(adet)
            FROM read_parquet('{kaynak}')
        """).fetchone()

        # 7 dosyayi bir dosyaya yaz.
        con.execute(f"""
            COPY (
                SELECT
                    olay_zamani, siparis_id, magaza_id,
                    urun_id, adet, birim_fiyat, tur
                FROM read_parquet('{kaynak}')
            )
            TO '{hedef}'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

        yeni_sonuc = con.execute(f"""
            SELECT COUNT(*), SUM(adet)
            FROM read_parquet('{hedef}')
        """).fetchone()

        if eski_sonuc != yeni_sonuc:
            raise RuntimeError("Veri kontrolu basarisiz: " + tarih)

        yeni_boyut = istemci.head_object(
            Bucket="islenmis",
            Key=hedef_anahtar
        )["ContentLength"]

        sonuclar.append([
            tarih,
            len(eski_dosyalar),
            1,
            eski_sonuc[0],
            eski_boyut,
            yeni_boyut
        ])

        print(
            tarih,
            "| Dosya: 7 -> 1",
            "| Kayit:", eski_sonuc[0]
        )

    with open(
        kayit_yolu, "x", newline="", encoding="utf-8"
    ) as dosya:
        yazici = csv.writer(dosya)
        yazici.writerow([
            "tarih", "once_dosya", "sonra_dosya",
            "kayit_sayisi", "once_byte", "sonra_byte"
        ])
        yazici.writerows(sonuclar)

    print("\n=== Z4 BIRLESTIRME SONUCU ===")
    print("Once dosya:", sum(s[1] for s in sonuclar))
    print("Sonra dosya:", sum(s[2] for s in sonuclar))
    print("Toplam kayit:", sum(s[3] for s in sonuclar))
    print("Sure (s):", round(time.perf_counter() - baslangic, 2))
    print("Olcumler kaydedildi.")

finally:
    con.close()
