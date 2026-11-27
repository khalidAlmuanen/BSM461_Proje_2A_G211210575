
import csv
import time
import duckdb
from ortak import s3

grup = "G211210575"
kaynak = "s3://islenmis/satis/*/*.parquet"
kodekler = ["snappy", "zstd", "gzip"]
beklenen_sayi = 2994056

istemci = s3()
con = duckdb.connect()

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

try:
    # Eski deney dosyalarinin uzerine yazmayalim.
    for kodek in kodekler:
        anahtar = f"z2_kodek_deney/{grup}/{kodek}.parquet"

        cevap = istemci.list_objects_v2(
            Bucket="islenmis",
            Prefix=anahtar
        )

        dosyalar = cevap.get("Contents", [])

        if any(d["Key"] == anahtar for d in dosyalar):
            raise SystemExit(
                f"Dosya zaten var: {anahtar}\n"
                "Veriyi korumak icin deney durduruldu."
            )

    for kodek in kodekler:
        anahtar = f"z2_kodek_deney/{grup}/{kodek}.parquet"
        hedef = f"s3://islenmis/{anahtar}"

        print("\nKodek:", kodek)

        baslangic = time.perf_counter()

        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{kaynak}')
            )
            TO '{hedef}'
            (FORMAT PARQUET, COMPRESSION {kodek})
        """)

        yazma_suresi = time.perf_counter() - baslangic

        boyut = istemci.head_object(
            Bucket="islenmis",
            Key=anahtar
        )["ContentLength"]

        sayi = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [hedef]
        ).fetchone()[0]

        if sayi != beklenen_sayi:
            raise RuntimeError("Kayit sayisi farkli!")

        okuma_sureleri = []
        sorgu_sonucu = None

        for tekrar in range(3):
            baslangic = time.perf_counter()

            sonuc = con.execute(f"""
                SELECT tur, COUNT(*), SUM(adet)
                FROM read_parquet('{hedef}')
                GROUP BY tur
                ORDER BY tur
            """).fetchall()

            sure = time.perf_counter() - baslangic

            if sorgu_sonucu is not None:
                if sonuc != sorgu_sonucu:
                    raise RuntimeError("Sorgu sonucu degisti!")

            sorgu_sonucu = sonuc
            okuma_sureleri.append(sure)

        ortalama = sum(okuma_sureleri) / 3

        sonuclar.append([
            kodek,
            sayi,
            boyut,
            round(boyut / 1048576, 2),
            round(yazma_suresi, 2),
            *[round(s, 2) for s in okuma_sureleri],
            round(ortalama, 2)
        ])

        print("Kayit sayisi:", sayi)
        print("Boyut (MiB):", round(boyut / 1048576, 2))
        print("Yazma (s):", round(yazma_suresi, 2))
        print("Okuma ortalama (s):", round(ortalama, 2))

    with open("/kit/kanit/z2_kodek_olcumleri.csv",
              "w", newline="", encoding="utf-8") as dosya:

        yazici = csv.writer(dosya)

        yazici.writerow([
            "kodek", "kayit_sayisi", "byte", "MiB",
            "yazma_s", "okuma1_s", "okuma2_s",
            "okuma3_s", "okuma_ortalama_s"
        ])

        yazici.writerows(sonuclar)

    print("\nOlcumler kaydedildi.")

finally:
    con.close()
