
import csv
from collections import defaultdict
from ortak import s3

istemci = s3()
gunler = defaultdict(list)

sayfalar = istemci.get_paginator("list_objects_v2").paginate(
    Bucket="islenmis",
    Prefix="satis/"
)

for sayfa in sayfalar:
    for dosya in sayfa.get("Contents", []):
        anahtar = dosya["Key"]

        if not anahtar.endswith(".parquet"):
            continue

        tarih = "bilinmiyor"

        for parca in anahtar.split("/"):
            if parca.startswith("tarih="):
                tarih = parca.split("=", 1)[1]

        gunler[tarih].append(dosya["Size"])

print("\n=== Z4 KUCUK DOSYA KONTROLU ===")

sonuclar = []

for tarih in sorted(gunler):
    boyutlar = gunler[tarih]

    dosya_sayisi = len(boyutlar)
    toplam = sum(boyutlar)
    ortalama_kib = toplam / dosya_sayisi / 1024

    print(
        tarih,
        "| Dosya:", dosya_sayisi,
        "| Ortalama KiB:", round(ortalama_kib, 2)
    )

    sonuclar.append([
        tarih,
        dosya_sayisi,
        toplam,
        round(ortalama_kib, 2)
    ])

toplam_dosya = sum(len(v) for v in gunler.values())
toplam_byte = sum(sum(v) for v in gunler.values())

print("\nToplam dosya:", toplam_dosya)
print("Toplam byte:", toplam_byte)
print("Toplam MiB:", round(toplam_byte / 1048576, 2))

yol = "/kit/kanit/z4_kucuk_dosya_olcumleri.csv"

with open(yol, "x", newline="", encoding="utf-8") as dosya:
    yazici = csv.writer(dosya)
    yazici.writerow([
        "tarih",
        "dosya_sayisi",
        "toplam_byte",
        "ortalama_KiB"
    ])
    yazici.writerows(sonuclar)

print("\nOlcumler kaydedildi.")
