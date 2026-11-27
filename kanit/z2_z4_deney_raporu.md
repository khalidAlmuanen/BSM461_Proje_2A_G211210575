# BSM 461 — Z2 ve Z4 Deney Raporu

**Şube:** 2. Öğretim A (2A)\
**Grup:** G211210575\
**Şablon:** 2 — Gerçek Zamanlı Satış ve Stok Takibi\
**Deney tarihi:** 26.11.2026\
**Durum:** Aşağıdaki bulgular ölçülmüştür. Spark hata araştırması ve diğer Z maddeleri devam etmektedir.

## 1. Amaç ve deney ortamı

Bu çalışmada, satış olaylarının JSONL ve Parquet olarak depolanması, Parquet sıkıştırma kodekleri ve çok sayıda küçük dosyanın birleştirilmesinin sorgu süresine etkisi incelendi.

Deney ortamı Windows, Docker Desktop, SeaweedFS (S3 uyumlu nesne deposu), Apache Spark ve DuckDB'den oluşmaktadır. Spark ile ilk JSONL → Parquet/Delta iş hattı çalıştırıldı; biçim, sıkıştırma ve dosya birleştirme ölçümlerinin devamında DuckDB kullanıldı. Aynı bilgisayarda alınan sonuçlar farklı donanımlara genellenmemelidir.

## 2. Ham veri ve ilk Parquet çıktısı

14 günlük geçmiş satış/stok verisi aşağıdaki komutla üretildi:

```powershell
docker compose exec araclar python araclar/uretici.py --grup G211210575 --sablon satis --toplu 3000000 --gun 14
```

| Ölçüm | Ham JSONL | İlk Spark Parquet |
|---|---:|---:|
| Kayıt sayısı | 3.000.000 | 2.994.056 |
| Dosya sayısı | 14 | 98 |
| Toplam boyut (bayt) | 461.823.430 | 52.036.601 |
| Toplam boyut (MiB) | 440,43 | 49,63 |

Üretici çıktısında **5.944 tekrar** bildirildi. Ham ve Parquet kayıt sayısı farkı da **5.944** olarak ölçüldü. Bu eşitlik, tekrarların temizlendiğini güçlü biçimde destekler; ancak `is/toplu_is.py` ayrıca `dropna` kullandığı için hangi işlemle kaç satırın çıkarıldığını ayrı bir test olmadan kesin olarak söyleyemeyiz.

İlk boyut karşılaştırmasında Parquet çıktısı ham JSONL'den %88,73 küçük göründü. **Bu değer yalnızca sıkıştırma oranı değildir:** Karşılaştırılan kayıt sayıları aynı değildir.

İlk Spark işinde `s3a://islenmis/satis/` Parquet ve `s3a://tablolar/satis` Delta çıktıları oluşturuldu. Spark sorgusunun sonuçları ve `explain` planı ayrı kanıt dosyasında bulunmaktadır. Daha sonraki ayrı Spark sayım denemesinde `BlockManager`/executor iletişim hatası görüldüğü için bu hata ayrıca incelenecektir.

## 3. Parquet kodek deneyi — Z2

Kaynak olarak aynı **2.994.056** temiz kayıt kullanıldı. DuckDB her kodek için bir Parquet dosyası yazdı. Yazma süresi bir kez, aynı `GROUP BY tur` sorgusunun okuma süresi üç kez ölçüldü.

| Kodek | Dosya boyutu (MiB) | Yazma (s) | Okuma 1 (s) | Okuma 2 (s) | Okuma 3 (s) | Okuma ort. (s) |
|---|---:|---:|---:|---:|---:|---:|
| Snappy | 49,24 | 8,29 | 0,45 | 0,60 | 0,53 | 0,53 |
| ZSTD | 28,16 | 7,25 | 0,31 | 0,40 | 0,30 | 0,34 |
| Gzip | 28,89 | 18,86 | 0,30 | 0,51 | 0,31 | 0,37 |

Bu çalıştırmada **ZSTD** en küçük dosyayı ve en düşük ortalama sorgu süresini sağladı. Bu nedenle ilk tercih olarak ZSTD belirlendi. Ölçümler tek yazma çalıştırmasına ve üç okuma tekrarına aittir; önbellek ve çalışma sırası sonuçları etkileyebilir. Kodekler arasında evrensel performans üstünlüğü iddia edilmemektedir.

**Kanıt:** `kanit/z2_kodek_olcumleri.csv`\
**Çalıştırma dosyası:** `araclar/adim6_kodek_deneyi.py`

## 4. Aynı kayıtlarla JSONL–Parquet karşılaştırması — Z2

Temizlenmiş aynı **2.994.056** kayıt hem JSONL hem de ZSTD sıkıştırmalı Parquet dosyasına yazıldı. DuckDB ile aynı SQL sorgusu her biçimde üç kez çalıştırıldı; program sorgu sonuçlarının eşitliğini denetledi.

| Ölçüm | JSONL | Parquet ZSTD |
|---|---:|---:|
| Kayıt sayısı | 2.994.056 | 2.994.056 |
| Boyut (MiB) | 399,31 | 28,16 |
| Okuma 1 (s) | 3,15 | 0,26 |
| Okuma 2 (s) | 2,88 | 0,38 |
| Okuma 3 (s) | 3,27 | 0,30 |
| Ortalama sorgu süresi (s) | 3,10 | yaklaşık 0,31 |

Bu deneyde Parquet ZSTD yaklaşık **%92,95 daha az depolama alanı** kullandı ve sorgu yaklaşık **9,9 kat daha kısa sürede** tamamlandı. Bu ölçümde biçim, kodlama ve depolama yapısı birlikte farklıdır; hız farkı tek başına kodekten kaynaklanıyor diye yorumlanmaz.

**Kanıt:** `kanit/z2_format_olcumleri.csv`\
**Çalıştırma dosyası:** `araclar/adim6_format_deneyi.py`

## 5. Küçük dosya incelemesi — Z4

İlk Spark çıktısında 14 tarih bölümünün her birinde **7 Parquet dosyası** vardı. Toplam **98 dosya**, **52.036.601 bayt** ve **49,63 MiB** ölçüldü. Ortalama dosya boyutu yaklaşık **0,51 MiB** idi.

Küçük dosya sayısının sorgu üzerindeki etkisini değerlendirmek için, kaynak dosyalar silinmeden ayrı bir S3 yoluna her tarih için birer dosya yazıldı:

- **Önce:** `s3://islenmis/satis/` — 98 Parquet dosyası.
- **Sonra:** `s3://islenmis/z4_birlestirme/G211210575/` — 14 Parquet dosyası.

Birleştirme işlemi **9,17 saniye** sürdü. Gün bazında kayıt sayısı ve toplam `adet` eşitliği kontrol edildi. Toplam kayıt sayısı değişmedi: **2.994.056**. Bu kontrol satırların tamamının birebir aynı sırada veya tüm sütunlarda karşılaştırıldığını göstermez.

**Kanıtlar:** `kanit/z4_kucuk_dosya_olcumleri.csv`, `kanit/z4_birlestirme_olcumleri.csv`\
**Çalıştırma dosyaları:** `araclar/adim6_kucuk_dosya_kontrol.py`, `araclar/adim6_dosya_birlestirme.py`

## 6. Birleştirme öncesi/sonrası performans — Z4

DuckDB'de aynı `SELECT tur, COUNT(*), SUM(adet) ... GROUP BY tur ORDER BY tur` sorgusu, önceki 98 dosyalı ve sonraki 14 dosyalı veri üzerinde beşer kez çalıştırıldı. Sorgu sonuçları eşit bulundu.

| Ölçüm | Önce (98 dosya) | Sonra (14 dosya) |
|---|---:|---:|
| Toplam boyut (MiB) | 49,63 | 48,70 |
| Okuma 1 (s) | 1,342 | 0,829 |
| Okuma 2 (s) | 1,517 | 0,602 |
| Okuma 3 (s) | 1,300 | 0,729 |
| Okuma 4 (s) | 1,447 | 0,991 |
| Okuma 5 (s) | 1,215 | 0,599 |
| Ortalama (s) | 1,364 | 0,750 |
| Medyan (s) | 1,342 | 0,729 |
| Kayıt sayısı | 2.994.056 | 2.994.056 |

Dosya sayısı **98'den 14'e**, yani **%85,71** azaldı. Ortalama sorgu süresi **1,364 saniyeden 0,750 saniyeye** indi; bu yaklaşık **%45,0 daha kısa süre** ve **1,82 kat hız** anlamına gelir. Toplam dosya boyutu da yaklaşık %1,87 azaldı.

**Yorum sınırı:** Birleştirme aynı zamanda Parquet içindeki `row group` yapısını, dosya düzenini ve sıkıştırma sonucunu değiştirebilir. İşletim sistemi/DuckDB önbelleği de süreyi etkileyebilir. Dolayısıyla gözlenen iyileşmenin tamamı yalnızca dosya sayısına atfedilmez.

**Kanıt:** `kanit/z4_performans_olcumleri.csv`\
**Çalıştırma dosyası:** `araclar/adim6_birlestirme_performans.py`

## 7. Kısa sonuç ve açık işler

Bu örnek veri kümesinde ZSTD, Parquet kodekleri arasında en küçük boyutu verdi; temiz aynı kayıtlarla JSONL–Parquet karşılaştırmasında Parquet çok daha küçük ve sorguda daha hızlı çıktı. Z4 deneyinde tarih başına yedi küçük dosyanın birleştirilmesiyle sorgu süresi bu test koşullarında azaldı. **Orijinal ham veri ve ilk işlenmiş Parquet/Delta yolları değiştirilmedi.**

Sonraki işler:

1. `kanit/` ölçüm CSV dosyalarının ve çalıştırma kodlarının GitHub'da `z2-z4-z5-z7` dalına aktarılması.
2. Proje yönergesinin istediği ortak `olcumler.csv` / rapor tablosuyla ölçümlerin eşleştirilmesi.
3. Parquet ölçümünü Z1'in 1.095 günlük saklama hesabıyla ilişkilendirirken veri temizleme ve biçim farklarını açıkça belirtme.
4. Spark `BlockManager` hatasının ayrı bir testle değerlendirilmesi; Z5 ve Z7'yi tamamlanmış saymama.
5. Gerekirse farklı çalıştırma sıralarıyla tekrar ölçüm yaparak önbellek etkisini azaltma.

**Not:** Bu rapordaki sayılar sağlanan terminal ölçüm çıktılarından alınmıştır; bağımsız bir üretim sistemi performans garantisi değildir.
