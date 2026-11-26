# Z1 — Ölçek ve Eşik Kararı (Adım 4)

**Ders:** BSM 461 — Büyük Veriye Giriş\
**Şablon:** 2 — Gerçek Zamanlı Satış ve Stok Takibi\
**Şube:** 2A\
**Grup Kodu:** G211210575\
**Tarih:** 24.11.2026\
**Durum:** Ölçümlere dayalı çalışma; Parquet ölçümü ve H1 dersindeki eşik tablosu ile karşılaştırma henüz tamamlanmadı.

## 1. Amaç

Bu bölümde senaryo kartındaki olay hızı ve saklama süresi kullanılarak ham veri hacmi hesaplanmış, kullanılan bilgisayarın kaynakları ölçülmüş ve tek makine ile dağıtık mimari seçenekleri değerlendirilmiştir.

## 2. Ölçüm Kaynakları

- **Senaryo kartı:** `docker compose exec araclar python araclar/senaryo.py --grup G211210575 --sablon satis`
- **Veri örneği:** `kanit/adim3_ornek100.jsonl` (100 satır)
- **Olay boyutu ölçümü:** PowerShell `Get-Content` + `[System.Text.Encoding]::UTF8.GetByteCount()` + `Measure-Object`
- **Donanım:** PowerShell `Get-CimInstance Win32_ComputerSystem` ve `Get-Volume -DriveLetter E`
- **Docker kaynakları:** `docker info --format 'CPUs: {{.NCPU}} | MemoryBytes: {{.MemTotal}}'` ve `docker stats --no-stream`

## 3. Senaryo ve Olay Boyutu

| Parametre | Değer | Kaynak |
|---|---:|---|
| Ortalama olay hızı | 200 olay/s | Senaryo kartı |
| Tepe yük katsayısı | 4 | Senaryo kartı |
| Tepe olay hızı | 800 olay/s | 200 × 4 |
| Ham veri saklama süresi | 1095 gün | Senaryo kartı |
| İncelenen olay sayısı | 100 | JSONL dosyası |
| Ortalama olay boyutu | 153,02 bayt/olay | PowerShell ölçümü |
| En küçük olay | 150 bayt | PowerShell ölçümü |
| En büyük olay | 160 bayt | PowerShell ölçümü |
| Örnek toplamı | 15.302 bayt | PowerShell ölçümü |

**Ölçüm sınırı:** Olay başına bayt sayısı yalnızca JSON metin satırının UTF-8 uzunluğudur. Satır sonları, depolama üstverisi, dosya sistemi yükü, Kafka kaydı ve replikasyon maliyeti dahil değildir. 100 kayıtlık örnek tüm veri dağılımını temsil etmeyebilir.

## 4. Ham Veri Hacmi Hesabı

Bir gün = 86.400 saniye.

- Günlük olay sayısı: `200 × 86.400 = 17.280.000 olay/gün`
- Günlük ham JSON metni: `17.280.000 × 153,02 = 2.644.185.600 bayt ≈ 2,644 GB/gün`
- Saklama süresindeki olay sayısı: `17.280.000 × 1.095 = 18.921.600.000 olay`
- 1.095 günlük ham JSON metni: `2.644.185.600 × 1.095 = 2.895.383.232.000 bayt ≈ 2,895 TB ≈ 2,633 TiB`

GB ve TB için 10^9 ve 10^12; GiB ve TiB için 2^30 ve 2^40 kullanılmıştır.

**Tepe yük (800 olay/s) tüm gün devam etseydi:**

- Günlük olay: `800 × 86.400 = 69.120.000 olay`
- Günlük ham JSON: `10.576.742.400 bayt ≈ 10,577 GB`

Bu, karşılaştırma senaryosudur; tepe yükün her gün 24 saat sürdüğü varsayılmamaktadır.

## 5. Ölçek Artışı

Aynı kayıt boyutu ve saklama süresi varsayımıyla:

| Senaryo | Ortalama hız | Tepe hız | 1.095 gün ham veri |
|---|---:|---:|---:|
| Mevcut | 200 olay/s | 800 olay/s | ≈ 2,895 TB |
| 10 kat | 2.000 olay/s | 8.000 olay/s | ≈ 28,954 TB |
| 100 kat | 20.000 olay/s | 80.000 olay/s | ≈ 289,538 TB |

Bu değerler ölçülmüş depolama kullanımı değil, senaryo kartı ile örnek olay boyutundan **hesaplanmış tahminlerdir**.

## 6. Test Bilgisayarı ve Docker Kaynakları

24.11.2026 tarihinde alınan komut çıktıları:

| Kaynak | Gözlenen değer |
|---|---:|
| Fiziksel RAM | 15,88 GiB (PowerShell'de `1GB` ile hesaplandı) |
| Mantıksal işlemci | 4 |
| E: toplam kapasite | 368,14 GiB |
| E: boş alan | 279,71 GiB |
| Docker'a görünen CPU | 4 |
| Docker'a görünen RAM | 8.267.550.720 bayt ≈ 7,70 GiB |

`docker stats --no-stream` tek anlık gözlemi:

| Konteyner | CPU kullanımı | Bellek kullanımı |
|---|---:|---:|
| `bsm461-kafka` | %221,28 | 790,3 MiB |
| `bsm461-s3` | %0,72 | 116,8 MiB |
| `bsm461-spark` | %0,00 | 2,484 MiB |
| `bsm461-araclar` | %0,00 | 920 KiB |

Docker CPU yüzdesi birden fazla mantıksal işlemci kullanıldığında %100'ün üzerine çıkabilir. Kafka için görülen %221,28, tek anlık kullanım değeridir; kalıcı darboğaz veya tepe işleme kapasitesi ölçümü olarak yorumlanamaz. Docker konteynerlerinin bellek üst sınırı da kullanılabilir boş RAM ile aynı şey değildir.

## 7. Tek Makine mi, Dağıtık Sistem mi?

**Eğitim ve geliştirme ortamı:** Başlangıç kiti aynı bilgisayarda Docker ile çalıştırılabilir; Kafka, S3 uyumlu nesne deposu ve Spark hizmetleri doğrulama testlerinden geçmiştir. Bu, gerçek üretim yükünün aynı bilgisayarda sorunsuz taşınacağını kanıtlamaz. Büyük veriyi tek seferde RAM'e çekmek yerine Spark üzerinde parçalı işlemek gerekir.

**Üretim ortamı ve saklama:** Ham JSON için hesaplanan yaklaşık **2.895 TB**, E: sürücüsünün mevcut **279,71 GiB** boş kapasitesinden yaklaşık **9,64 kat** fazladır. Diğer veri türleri ve kopyalar eklenmeden bile E: sürücüsünde 1.095 günlük verinin tümünü saklamak mümkün görünmemektedir. Yalnızca E: diskinin mevcut boş alanı ham JSON'a ayrılsaydı yaklaşık **113,6 gün** teorik kapasite elde edilirdi. Bu tahmin diğer dosyaları, satır sonlarını ve ek yükleri içermez.

**Önemli sınır:** Docker Desktop'ın verileri E: yerine başka bir fiziksel diskte veya WSL disk görüntüsünde olabilir; bu ölçümler Docker'ın gerçek veri konumunu göstermemektedir. Bu nedenle "Docker diski doldu" iddiasında bulunulmamalıdır.

**Giriş aktarım hızı:** Ortalama ham metin veri hızı `200 × 153,02 = 30.604 bayt/s ≈ 0,0306 MB/s`; tepe değer `800 × 153,02 = 122.416 bayt/s ≈ 0,1224 MB/s` olarak tahmin edilir. Bu sayılardan tek başına ağın ya da Kafka'nın birinci darboğaz olduğu sonucu çıkarılamaz. Protokol, paralellik, disk ve işlem yükü ayrıca ölçülmelidir.

**Mimari kararı (öneri):** Projenin çalışır prototipi Docker üzerinde tek bilgisayarda kurulacaktır. Gerçek saklama ve süreklilik gereksinimleri için ölçeklenebilir nesne deposu, kalıcı/veri kaybına dayanıklı kuyruk altyapısı, kaynaklara göre ölçeklendirilebilen Spark işleme katmanı ve izleme mekanizması planlanmalıdır. Bu tercih yalnızca depolama miktarına değil, hata toleransı ve 1.095 günlük saklama gereksinimine de dayanmaktadır.

## 8. İlk Darboğaz — Ölçüm ile Tahmini Ayırma

- **Somut kapasite kısıtı:** E: sürücüsünün boş alanı, bütün ham verinin 1.095 gün saklanması için yetersizdir.
- **İncelenecek işlem riski:** 4 mantıksal CPU ve Docker'a ayrılan yaklaşık 7,70 GiB RAM, Spark iş yüklerinin aynı anda çalışmasında sınırlayıcı olabilir. Henüz yük testiyle doğrulanmadı.
- **İncelenecek Kafka riski:** Tek anlık CPU ölçümündeki %221,28 kullanımın sürüp sürmediği tekrar ölçülmelidir. Bu tek değerle kapasite kararı verilmez.
- **Henüz bilinmeyen:** Fiziksel Docker veri yolu, disk I/O hızı, gerçek Kafka/Spark tepe kapasitesi ve sürekli p99 servis gecikmesi.

## 9. Açık İşler

1. **H1 Eşik Tablosu:** Dersin H1 sunumundaki eşik tablosu ile karar, ilgili tablodan doğrudan karşılaştırılıp rapora eklenmelidir; tablo şu an elde doğrulanmış değildir.
2. **Parquet tahmini:** Adım 6'da aynı verinin JSON/Parquet boyutları gerçek olarak ölçüldükten sonra saklama tahmini güncellenecektir. Varsayımsal sıkıştırma oranı gerçek ölçüm gibi yazılmayacaktır.
3. **Docker veri yolu:** Gerektiğinde Docker Desktop'ın disk görüntüsünün fiziksel konumu kontrol edilmelidir.
4. **Performans:** Adım 6–8 sırasında Spark ve depolama için gerçek süreler/işlem ölçümleri rapora eklenecektir.

## 10. Kısa Sonuç

100 olaylık örnekte ortalama JSON olay boyutu **153,02 bayt** ölçülmüştür. Senaryo kartındaki **200 olay/s** ortalama hız ve **1.095 gün** saklama süresi ile ham veri hacmi yaklaşık **2,895 TB** hesaplanmıştır. **800 olay/s** tepe yük, kapasite tasarımında dikkate alınacaktır. Kullanılan bilgisayarın **4 mantıksal işlemcisi**, **15,88 GiB RAM'i**, Docker'ın **7,70 GiB bellek sınırı** ve E: sürücüsünde **279,71 GiB boş alanı** bulunmaktadır. Mevcut E: boş alanı tüm ham veriyi saklamaya yetmediğinden gerçek ölçek için genişleyebilir depolama gerekir; öğretim amaçlı daha küçük veri kümesiyle prototip tek bilgisayarda çalıştırılacaktır.

**Kanıt durumu:** Senaryo, JSON örneği ve ekran görüntülerindeki kaynak ölçümleri mevcut. H1 eşik tablosunun birebir uygulanması ve Parquet oranının gerçek ölçümü henüz tamamlanmadı.
