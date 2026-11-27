
import csv
import time
import duckdb
from ortak import s3

con = duckdb.connect()
istemci = s3()

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
con.execute("SET memory_limit='3GB'")

kaynak = "s3://islenmis/satis/*/*.parquet"
klasor = "z2_format_deney/G211210575/"

json_anahtar = klasor + "temiz.jsonl"
parquet_anahtar = klasor + "temiz.parquet"

json_yolu = "s3://islenmis/" + json_anahtar
parquet_yolu = "s3://islenmis/" + parquet_anahtar

try:
    # Once eski deney dosyasi var mi kontrol et.
    for anahtar in [json_anahtar, parquet_anahtar]:
        cevap = istemci.list_objects_v2(
            Bucket="islenmis", Prefix=anahtar
        )

        for dosya in cevap.get("Contents", []):
            if dosya["Key"] == anahtar:
                raise SystemExit(
                    "Dosya zaten var: " + anahtar
                )

    # Her iki bicim icin ayni kayitlari kullan.
    alanlar = """
        olay_zamani, siparis_id, magaza_id,
        urun_id, adet, birim_fiyat, tur
    """

    print("JSONL yaziliyor...")

    con.execute(f"""
        COPY (
            SELECT {alanlar}
            FROM read_parquet('{kaynak}')
        )
        TO '{json_yolu}'
        (FORMAT JSON)
    """)

    print("Parquet ZSTD yaziliyor...")

    con.execute(f"""
        COPY (
            SELECT {alanlar}
            FROM read_parquet('{kaynak}')
        )
        TO '{parquet_yolu}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    dosyalar = {
        "JSONL": (
            json_yolu,
            json_anahtar,
            "read_json_auto"
        ),
        "Parquet_ZSTD": (
            parquet_yolu,
            parquet_anahtar,
            "read_parquet"
        )
    }

    sonuclar = {}

    for bicim, (yol, anahtar, okuyucu) in dosyalar.items():
        adet = con.execute(
            f"SELECT COUNT(*) FROM {okuyucu}(?)",
            [yol]
        ).fetchone()[0]

        if adet != 2994056:
            raise RuntimeError("Kayit sayisi hatali!")

        boyut = istemci.head_object(
            Bucket="islenmis", Key=anahtar
        )["ContentLength"]

        print(bicim, "kayit:", adet)
        print(bicim, "MiB:", round(boyut / 1048576, 2))

        sonuclar[bicim] = {
            "boyut": boyut,
            "sureler": [],
            "sonuc": None
        }

    # Iki bicimde de ayni sorgu.
    # Olcum sirasini degistirerek 3 tekrar yap.
    for tekrar in range(3):
        sira = ["JSONL", "Parquet_ZSTD"]

        if tekrar == 1:
            sira.reverse()

        for bicim in sira:
            yol, _, okuyucu = dosyalar[bicim]

            baslangic = time.perf_counter()

            sonuc = con.execute(f"""
                SELECT tur, COUNT(*), SUM(adet)
                FROM {okuyucu}(?)
                GROUP BY tur
                ORDER BY tur
            """, [yol]).fetchall()

            sure = time.perf_counter() - baslangic

            sonuclar[bicim]["sureler"].append(sure)
            sonuclar[bicim]["sonuc"] = sonuc

            print(bicim, "okuma:", round(sure, 2), "s")

    # Sorgu sonucunu karsilastir.
    if sonuclar["JSONL"]["sonuc"] != sonuclar["Parquet_ZSTD"]["sonuc"]:
        raise RuntimeError("Sorgu sonuclari farkli!")

    with open(
        "/kit/kanit/z2_format_olcumleri.csv",
        "w",
        newline="",
        encoding="utf-8"
    ) as dosya:

        yazici = csv.writer(dosya)
        yazici.writerow([
            "format", "byte", "MiB",
            "okuma1_s", "okuma2_s",
            "okuma3_s", "ortalama_s"
        ])

        for bicim, bilgi in sonuclar.items():
            sureler = bilgi["sureler"]

            yazici.writerow([
                bicim,
                bilgi["boyut"],
                round(bilgi["boyut"] / 1048576, 2),
                *[round(s, 2) for s in sureler],
                round(sum(sureler) / 3, 2)
            ])

    print("\nSonuclar ayni. Olcumler kaydedildi.")

finally:
    con.close()
