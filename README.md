# BSM 461 — Büyük Veriye Giriş Dönem Projesi

## Gerçek Zamanlı Satış ve Stok Takibi

**Şube:** 2. Öğretim A (`2A`)  
**Grup kodu:** `G211210575`  
**Şablon:** 2 — `satis`  
**Kaynak kit:** [BSM 461 Başlangıç Kiti](https://github.com/sumeyyekaynak/bsm461-proje-kit)

Bu projede bir mağaza zincirindeki satış, iade ve stok girişi olaylarını işleyen bir veri hattı kurulmaktadır. Anlık veriler Kafka üzerinden, geçmiş veriler ise S3 uyumlu nesne deposu üzerinden işlenir. Toplu analiz için Spark, sonuçların dosya ve tablo biçiminde saklanması için Parquet ve Delta Lake kullanılır. Akış analizi, servis deposu ve pano çalışmaları proje kapsamında geliştirilecektir.

> **Durum:** Bu depo geliştirme aşamasındadır. Aşağıda "tamamlandı" olarak belirtilmeyen özellikler henüz doğrulanmış kabul edilmez. GitHub kodu ve küçük kanıt dosyalarını tutar; yerel Docker hacimlerindeki veriler GitHub'a otomatik aktarılmaz.

## 1. Grup ve iş bölümü

| Üye | Öğrenci numarası | Çalışma alanı |
|---|---|---|
| Üye 1 | `G211210575` | Z1 ilk hesaplar, Z2, Z4, Z5, Z7: depolama, Parquet, Batch, Delta |
| Üye 2 | `G231210048` | Z3, Z6, Z8, Z9: Kafka bölümleme, servis deposu, Streaming, pano |
| Üye 3 | `B231210559` | Z10: veri kalitesi, grafikler, raporun birleştirilmesi |

**Ortak çalışmalar:** Z0 mimari, Z11 kırılma deneyleri, bütünleştirme ve sunum. Z1 ilk çalışması zaman nedeniyle Üye 1 tarafından yapılmıştır; Parquet oranıyla güncelleme beklemektedir.

## 2. Senaryo koşulları

Aşağıdaki değerler `G211210575` grup koduyla üretilen senaryo kartındandır.

| Koşul | Değer |
|---|---:|
| Ortalama olay hızı | 200 olay/s |
| Tepe yük | 800 olay/s (4 kat) |
| Akış uyarı hedefi | En fazla 120 saniye |
| Geç gelen olaylar | %15, en fazla 30 dakika |
| Geç veri nedeniyle kabul edilen kayıp | 0 olay |
| Ham veri saklama | 1095 gün |
| Servis sorgusu hedefi | p99 en fazla 200 ms |

**Dikkat:** 120 saniyede sonuç verme, 30 dakikaya kadar gecikme ve sıfır kayıp koşulları birlikte değerlendirilecektir. İlk sonucu hızlı üretip geç veri geldiğinde düzeltme yaklaşımı bir tasarım seçeneğidir; uygulanmış veya ölçülmüş bir garanti değildir.

## 3. Kullanılan bileşenler

| Bileşen | Kullanım | Durum |
|---|---|---|
| Python veri üreticisi | `araclar/uretici.py` | Çalıştırıldı |
| Apache Kafka | Anlık olaylar, konu: `satislar` | Temel doğrulama yapıldı |
| SeaweedFS / S3 | Ham JSONL, boyut tabloları, işlenmiş veri | Temel doğrulama ve veri yazımı yapıldı |
| Apache Spark 3.5 | Batch ve Streaming | Temel doğrulama, ilk Batch denemesi yapıldı |
| Parquet | Sütunlu dosya biçimi | İlk yazma denemesi yapıldı; karşılaştırma ölçümleri bekliyor |
| Delta Lake | Sürümlü tablo | İlk tablo yazımı görüldü; Z7 deneyleri bekliyor |
| MongoDB / pano | Servis ve görselleştirme için önerilen bileşenler | Henüz bütünleştirilmedi |

Başlangıç kiti eğitim amaçlı tek bilgisayarda çalışır. Kafka ve S3 tek düğümlüdür; üretim ortamındaki dayanıklılık garantileri bu kurulumla sağlanmış sayılmaz.

## 4. Kurulum — Windows PowerShell

### Gereksinimler

- Git, Docker Desktop ve Docker Compose.
- Docker Desktop için çalışan WSL 2 altyapısı.
- Çekirdek kit için en az 8 GB sistem RAM'i ve Docker tarafında en az 5 GB bellek; 16 GB sistem RAM'i ve yaklaşık 7 GB Docker belleği daha rahattır.
- İlk çalıştırmada Docker imajları ve Spark kütüphanelerini indirmek için internet.

### Yeni bir ekip üyesinin ilk kurulumu

```powershell
git clone https://github.com/khalidAlmuanen/BSM461_Proje_2A_G211210575.git
cd BSM461_Proje_2A_G211210575

docker compose up -d
docker compose ps

docker compose exec araclar python araclar/dogrula.py
docker compose exec spark spark-submit is/dogrula_spark.py
```

İlk kurulumun ardından `docker compose ps` çıktısında çekirdek servislerin durumları kontrol edilmelidir. PowerShell'de `dogrula.sh` yerine yukarıdaki iki Python/Spark doğrulaması kullanılır.

**Yerel düzeltme:** Bu depoda SeaweedFS `s3` healthcheck adresi `127.0.0.1:9333/cluster/status` olarak düzenlenmiştir. İlgili değişiklik `docker-compose.yml` dosyasındadır.

**Veri paylaşımı:** Her ekip üyesi kendi Docker ortamını ve S3 depolamasını kullanır. Bir üyede üretilmiş 3 milyon satır, diğer üyenin Docker ortamında kendiliğinden görünmez.

## 5. Veri kaynakları ve S3 yolları

### Kafka'daki anlık olaylar

- Konu: `satislar`
- Alanlar: `olay_zamani`, `siparis_id`, `magaza_id`, `urun_id`, `adet`, `birim_fiyat`, `tur`
- Türler: `satis`, `iade`, `stok_girisi`

Kısa bir test üretimi:

```powershell
docker compose exec araclar python araclar/uretici.py --grup G211210575 --sablon satis --hiz 5 --sure 10
```

Kafka konu bilgisini okumak:

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic satislar
```

İlk incelemede `satislar` konusunun 3 partition ve 1 replication factor ile çalıştığı gözlemlendi. Üç bölümün lideri aynı broker'dır; bölüm içi sıra vardır, bölümler arası genel sıra garantisi yoktur. Nihai bölüm anahtarı ve dayanıklılık tasarımı Z3/Z8'de değerlendirilecektir.

### Geçmiş veri ve referans dosyaları

```text
s3://ham/satis/tarih=YYYY-MM-DD/parca-....jsonl
s3://ham/satis_baslangic_stok.csv
s3://ham/boyut/satis/urunler.csv
s3://ham/boyut/satis/magazalar.csv
```

- Başlangıç stok dosyası: 500 ürün için stok bilgisi.
- `urunler.csv`: 500 ürün, kategori/marka/maliyet bilgileri.
- `magazalar.csv`: 20 mağaza, şehir ve metrekare bilgileri.

**Önemli — aşağıdaki komut mevcut ham tarihsel veriyi değiştirebilir!**

```powershell
docker compose exec araclar python araclar/uretici.py --grup G211210575 --sablon satis --toplu 3000000 --gun 14
```

Bu komut `s3://ham/satis/` altındaki önceki tarihsel dosyaları yeniden oluşturarak **siler ve değiştirir**. Aynı S3 ortamında başka bir kişi çalışıyorsa yeniden çalıştırmadan önce koordinasyon gerekir. Üreticide `--ekle` seçeneği vardır; bu seçenek de tekrar kayıtları ve veri hacmini etkileyebileceği için plansız kullanılmamalıdır. İlk kurulumda büyük veri üretimi zorunlu değildir; yalnızca ilgili deneyi çalıştıracak üye ihtiyaç duyduğunda uygulanır.

## 6. Batch çalışması ve çıktılar

İskelet Batch işi:

```powershell
docker compose exec -T spark spark-submit is/toplu_is.py --sablon satis
```

Girdi ve çıktı yolları:

```text
Girdi JSONL   : s3a://ham/satis/
Parquet       : s3a://islenmis/satis/
Delta Lake    : s3a://tablolar/satis
```

**Dikkat:** Mevcut `is/toplu_is.py` hem Parquet hem Delta çıktısında `mode("overwrite")` kullanır. Bu komut mevcut çıktıları değiştirebilir; başka üyenin deneyleri devam ederken rastgele tekrar çalıştırılmamalıdır. İskeletteki `dropDuplicates()` mantıksal tekrar tespiti için henüz doğrulanmış son yöntem değildir.

İlk denemede kaynak veri sayısı 3.000.000 olarak ekrana yazıldı, Parquet çıktısı ve Delta sürüm 0 görüldü. Çalıştırma günlüğü `kanit/adim6_spark_ilk_calistirma.txt` içindedir. Bu tek deneme, JSON/Parquet performans karşılaştırmalarının veya Z7'nin tamamlandığı anlamına gelmez.

Streaming iskeleti: `is/akis_iskelet.py`. İki akış sorusuna uyarlanması ve geç veri, checkpoint, stok ve teslim garantisi deneyleri henüz beklemektedir.

## 7. Analiz soruları

**Toplu (Batch):**

1. Son 14 günde en çok satılan ürünler ve kategoriler hangileridir?
2. Mağazalara göre iade oranları nasıl değişmektedir?

**Akış (Streaming):**

1. Hangi mağazalarda hangi ürünlerin stokları kritik seviyenin altına düşmüştür?
2. Mağaza bazında son bir saatlik net satış cirosu nasıl değişmektedir?

Soruların ayrıntısı: [`kanit/analiz_sorulari.md`](kanit/analiz_sorulari.md). Kritik stok eşiği, iade/stok iş kuralı ve tekrar giderme anahtarı henüz kesinleştirilmemiştir.

## 8. Yapılan ölçümler ve kanıtlar

**Aşağıdaki sayılar gruba ait deneylerden alınmıştır; tamamlanmamış deneyler tamamlandı olarak işaretlenmemiştir.**

| Ölçüm | Sonuç | Kanıt |
|---|---:|---|
| Tarihsel kayıt sayısı | 3.000.000, 14 gün | Üretici konsol çıktısı |
| Üreticinin bildirdiği tekrar | 5.944 | Üretici konsol çıktısı |
| Ham JSONL sayısı | 14 dosya | [`adim6_ham_boyut.txt`](kanit/adim6_ham_boyut.txt) |
| Ham JSONL toplam boyutu | 461.823.430 bayt = 440,43 MiB | [`adim6_ham_boyut.txt`](kanit/adim6_ham_boyut.txt) |
| İlk 100 JSON olayında ortalama satır uzunluğu | 153,02 bayt | [`z1_olcek_raporu.md`](kanit/z1_olcek_raporu.md) |
| Kafka mevcut konu ayarları | 3 partition, RF=1 | [`kafka_topic_satislar.txt`](kanit/kafka_topic_satislar.txt) |
| İlk Batch işi | Parquet yazma ve Delta sürüm 0 görüldü | [`adim6_spark_ilk_calistirma.txt`](kanit/adim6_spark_ilk_calistirma.txt) |

İlk Spark işinin tam çıkış kodu, temizlenmiş satır sayısı, Parquet toplam boyutu, codec karşılaştırması ve tekrarlı okuma süreleri ayrıca doğrulanacaktır. Çalıştırma günlüğünde Windows PowerShell kaynaklı uyarı/karakter kodlaması sorunları görülebilir; gerçek sonuç ve uyarılar birlikte değerlendirilmelidir.

Diğer dosyalar:

- [`kanit/senaryo.txt`](kanit/senaryo.txt) — senaryo kartı.
- [`kanit/veri_sozlugu.md`](kanit/veri_sozlugu.md) — veri sözlüğü ve 100 kayıtlık ilk inceleme.
- [`kanit/kvkk_notu.md`](kanit/kvkk_notu.md) — KVKK değerlendirmesi.
- [`kanit/mimari_taslak.md`](kanit/mimari_taslak.md) — ilk mimari ve garanti tablosu.
- [`kanit/z1_olcek_raporu.md`](kanit/z1_olcek_raporu.md) — ölçek hesabı; Parquet oranıyla güncellenecek.

## 9. GitHub üzerinde birlikte çalışma

Herkes projeyi kendi bilgisayarına klonlar ve **ayrı bir branch** üzerinde değişiklik yapar. Değişiklikleri Pull Request ile `main` dalına birleştirmeden önce kontrol ederiz.

Yeni klon yapan üyeler için örnek:

```powershell
git switch -c z3-z6-z8-z9
git add is/ kanit/
git commit -m "Streaming calismalari"
git push -u origin z3-z6-z8-z9
```

Bu örnekte `origin`, **grup deposunu klonlayan üyelerin** uzak bağlantısıdır. İlk kurulumunu doğrudan öğretim üyesinin kit deposundan yapan Üye 1'in bilgisayarında ise grup deposunun uzak bağlantısı `team` adını taşır. Üye 1 kendi dallarını `team` bağlantısına göndermelidir; öğretim üyesinin `origin` deposuna push yapılmaz.

- Birbirimizin dosyalarını haber vermeden değiştirmeyelim.
- S3 ve Kafka verileri GitHub'dan paylaşılmaz; aynı grup koduyla yerel olarak üretilebilir.
- Commit'lerde neyin yapıldığını anlaşılır şekilde yazalım.
- Ölçüm dosyalarında komut, kullanılan veri boyutu ve sonuç ayrı ayrı belirtilecek.
- Kamuya açık depoya şifre, özel anahtar veya kişisel veri yüklenmeyecek.

## 10. Güvenli durdurma ve dikkat edilecekler

Çekirdek hizmetleri durdurmak için:

```powershell
docker compose stop
```

Bu işlem konteynerleri durdurur. `docker compose down -v` **Docker hacimlerini ve yerel verileri siler**; veri silme deneyinin bilinçli bir parçası değilse kullanılmamalıdır. Kafka topic silme komutları da önceden veri koruma planı olmadan çalıştırılmamalıdır.

Bu depo başlangıç kitinden türetilmiştir. Genel altyapı açıklamaları ve sorun giderme adımları için [öğretim üyesinin başlangıç kiti README dosyasına](https://github.com/sumeyyekaynak/bsm461-proje-kit/blob/main/README.md) bakılabilir.

## 11. Sonraki işler

- [ ] Parquet dosya boyutu ve codec (`snappy`, `zstd`, `gzip`) karşılaştırmaları (Z2).
- [ ] S3 dosya düzeni, küçük dosya sayısı ve listeleme maliyeti ölçümleri (Z4).
- [ ] İki Batch sorusunun join ve performans ölçümleriyle uygulanması (Z5).
- [ ] Delta Lake time travel ve yarım kalan yazma deneyi (Z7).
- [ ] Kafka bölümleme, akış soruları, geç veri ve servis/pano deneyleri (Z3, Z6, Z8, Z9).
- [ ] Veri kalitesi, iki grafik, kırılma deneyleri ve raporun bütünleştirilmesi (Z10, Z11).

**Not:** Proje tamamlandığında README, gerçek çıktı yolları, tekrar sayıları, başarı/başarısızlık durumu ve kanıtlarla güncellenecektir.
