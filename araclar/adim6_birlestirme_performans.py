
import csv
import time
import statistics
from pathlib import Path

import duckdb
from ortak import s3

# Deney ayarlari
kaynaklar = {
    "once_98": "s3://islenmis/satis/*/*.parquet",
    "sonra_14": (
        "s3://islenmis/z4_birlestirme/"
        "G211210575/tarih=*/*.parquet"
    )
}

prefixler = {
    "once_98": "satis/",
    "sonra_14": "z4_birlestirme/G211210575/"
}

hedef_csv = "/kit/kanit/z4_performans_olcumleri.csv"

# Eski olcumleri koru
if Path(hedef_csv).exists():
    raise SystemExit("Olcum dosyasi zaten var!")

istemci = s3()
con = duckdb.connect()

try:
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

    # Dosya sayisi ve toplam boyut
    dosya_bilgileri = {}

    for ad, prefix in prefixler.items():
        sayfalar = istemci.get_paginator(
            "list_objects_v2"
        ).paginate(
            Bucket="islenmis",
            Prefix=prefix
        )

        dosyalar = [
            obj
            for sayfa in sayfalar
            for obj in sayfa.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]

        dosya_bilgileri[ad] = (
            len(dosyalar),
            sum(obj["Size"] for obj in dosyalar)
        )

    # Iki durumda ayni SQL sorgusu
    sorgu = """
        SELECT tur, COUNT(*), SUM(adet)
        FROM read_parquet(?)
        GROUP BY tur
        ORDER BY tur
    """

    # Sonuclarin ayni oldugunu kontrol et
    kontrol = {}

    for ad, yol in kaynaklar.items():
        sonuc = con.execute(sorgu, [yol]).fetchall()
        kontrol[ad] = sonuc

        toplam_kayit = sum(satir[1] for satir in sonuc)

        if toplam_kayit != 2994056:
            raise RuntimeError("Kayit sayisi hatali: " + ad)

    if kontrol["once_98"] != kontrol["sonra_14"]:
        raise RuntimeError("Sorgu sonuclari farkli!")

    print("Sorgu sonuclari ayni.")
    print("Toplam kayit: 2994056")

    # Her iki durum icin 5 olcum
    sureler = {
        "once_98": [],
        "sonra_14": []
    }

    for tekrar in range(5):
        sira = ["once_98", "sonra_14"]

        if tekrar % 2 == 1:
            sira.reverse()

        for ad in sira:
            baslangic = time.perf_counter()

            sonuc = con.execute(
                sorgu,
                [kaynaklar[ad]]
            ).fetchall()

            sure = time.perf_counter() - baslangic

            if sonuc != kontrol[ad]:
                raise RuntimeError("Sonuc degisti!")

            sureler[ad].append(sure)

            print(
                ad,
                "Tekrar:", tekrar + 1,
                "Sure:", round(sure, 3), "s"
            )

    # Sonuclari CSV dosyasina kaydet
    with open(
        hedef_csv, "x",
        newline="", encoding="utf-8"
    ) as dosya:

        yazici = csv.writer(dosya)

        yazici.writerow([
            "durum", "dosya_sayisi", "toplam_byte",
            "toplam_MiB", "okuma1_s", "okuma2_s",
            "okuma3_s", "okuma4_s", "okuma5_s",
            "ortalama_s", "medyan_s"
        ])

        for ad in kaynaklar:
            sayi, boyut = dosya_bilgileri[ad]
            s = sureler[ad]

            yazici.writerow([
                ad,
                sayi,
                boyut,
                round(boyut / 1048576, 2),
                *[round(x, 3) for x in s],
                round(statistics.mean(s), 3),
                round(statistics.median(s), 3)
            ])

            print("\nDurum:", ad)
            print("Dosya sayisi:", sayi)
            print("Boyut MiB:", round(boyut / 1048576, 2))
            print("Ortalama sure:", round(statistics.mean(s), 3))
            print("Medyan sure:", round(statistics.median(s), 3))

    print("\nZ4 performans olcumleri kaydedildi.")

finally:
    con.close()
