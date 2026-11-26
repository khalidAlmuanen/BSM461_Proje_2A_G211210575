# VERİ SÖZLÜĞÜ — VERİ İNCELEME RAPORU

**Ders:** BSM 461 — Büyük Veriye Giriş\
**Proje:** Şablon 2 — Gerçek Zamanlı Satış ve Stok Takibi\
**Şube:** 2A\
**Grup Kodu:** G211210575\
**İnceleme Tarihi:** 24.11.2026

---

## 1. Amaç

Bu çalışmada, Kafka üzerinden üretilen satış ve stok verilerini tanımak amacıyla 100 kayıtlık bir örnek veri kümesi incelenmiştir.

Veriler `satislar` konusundan okunarak `kanit/adim3_ornek100.jsonl` dosyasına kaydedilmiştir.

Amaç; veri alanlarını, veri tiplerini, olay türlerini ve veri kalitesiyle ilgili temel noktaları anlamaktır.

## 2. Veri Sözlüğü

Aşağıdaki tablolar, küçük ekranlarda da rahat okunabilmesi için alan tanımları ve örnek değerler olarak ayrılmıştır.

### 2.1. Alanlar ve Veri Tipleri

| Alan            | JSON Tipi      | Açıklama             |
| :---            | :---:          | :---                 |
| `olay_zamani`   | String         | Olay tarihi ve saati |
| `siparis_id`    | String         | Sipariş kimliği      |
| `magaza_id`     | Integer        | Mağaza numarası      |
| `urun_id`       | Integer        | Ürün numarası        |
| `adet`          | Integer        | İşlem miktarı        |
| `birim_fiyat`   | Number         | Birim fiyat          |
| `tur`           | String         | Olay türü            |

### 2.2. Örnek Alan Değerleri

| Alan            | Örnek Değer                |
| :---            | :---                       |
| `olay_zamani`   | `2026-11-24T10:09:46.654Z` |
| `siparis_id`    | `S000000063`               |
| `magaza_id`     | `11`                       |
| `urun_id`       | `56`                       |
| `adet`          | `5`                        |
| `birim_fiyat`   | `28.85`                    |
| `tur`           | `iade`                     |

### 2.3. Veri Tipleri Hakkında Notlar

- `olay_zamani` JSON içinde metindir; Spark analizinde `Timestamp` tipine dönüştürülecektir.
- `birim_fiyat` sayısal değerdir; finansal hesaplamalarda uygun hassasiyetle `Decimal` kullanılması değerlendirilecektir.
- `tur` alanında `satis`, `iade` ve `stok_girisi` değerleri bulunabilir.
- `siparis_id` farklı üretici çalıştırmalarında tekrar edebildiği için tek başına eşsiz olay anahtarı kabul edilmemektedir.

## 3. Olay Türlerinin İncelenmesi

İncelenen 100 kaydın olay türlerine göre dağılımı:

| Olay Türü     | Kayıt   | Oran     |
| ------------  | ----:   | ---:     |
| `satis`       | 93      | %93      |
| `stok_girisi` | 6       | %6       |
| `iade`        | 1       | %1       |
| **Toplam**    | **100** | **%100** |

Satış işlemleri stok miktarını azaltır. Stok girişleri stok miktarını artırır. İade işlemleri ise belirlenen iş kurallarına göre stok ve net satış tutarını etkiler.

## 4. Veri Kalitesi İncelemesi

### 4.1. Eksik ve Boş Değerler

Yedi temel alan üzerinde eksik, null ve boş değer kontrolü yapılmıştır.

- İncelenen kayıt sayısı: 100
- Eksik veya boş alan sayısı: 0

Bu sonuç yalnızca incelenen örnek veri için geçerlidir. Bütün veri kümesinde hatalı kayıt bulunmadığı anlamına gelmez.

### 4.2. Tekrar Eden Kayıtlar

JSON satırlarının metinsel olarak birebir aynı olup olmadığı kontrol edilmiştir.

- İncelenen kayıt sayısı: 100
- Birebir aynı satır grubu sayısı: 0
- Fazladan metinsel tekrar sayısı: 0

Ancak farklı üretici çalıştırmalarında bazı `siparis_id` değerlerinin tekrarlandığı görülmüştür.

Bu nedenle `siparis_id` alanına göre doğrudan kayıt silinmeyecektir. Mantıksal tekrar kontrolü daha sonra Spark Batch çalışmaları kapsamında ayrıntılı olarak değerlendirilecektir.

### 4.3. Mağaza Dağılımı

100 kayıt üzerinde mağaza kimliklerine göre dağılım incelenmiştir.

En fazla görülen mağazalar:

| Mağaza ID | Olay Sayısı |
| :-------: | ----------: |
| 3         | 14          |
| 14        | 10          |
| 10        | 9           |
| 5         | 9           |
| 12        | 7           |

Mağaza 3, örnek veri içinde en fazla olaya sahip mağazadır. Ancak bu sonuç tek başına büyük veri ölçeğinde performans sorunu olduğunu göstermez.

### 4.4. Ürün Dağılımı

Ürün kimliklerine göre yapılan incelemede:

| Ürün ID | Olay Sayısı |
| :-----: | ----------: |
| 2       | 13          |
| 7       | 10          |
| 23      | 6           |
| 9       | 5           |
| 11      | 5           |

Ürün 2 ve ürün 7, incelenen örnekte en sık görülen ürünlerdir.

Veri dağılımının Spark performansı üzerindeki etkisi Z5 kapsamında daha büyük veri kullanılarak değerlendirilecektir.

### 4.5. Geç Gelen Veriler

Senaryo kartına göre olayların %15'i en fazla 30 dakika gecikmeli gelebilmektedir.

Örnek verilerde farklı olay zamanları gözlemlenmiştir. Ancak kesin gecikme süresi analizi henüz tamamlanmamıştır.

Bu konu Z8 — Spark Streaming ve ortak mimari çalışmaları kapsamında ayrıca incelenecektir.

## 5. Veri İşleme İçin Temel Kurallar

- Olay zamanları Spark üzerinde uygun zaman veri tipine dönüştürülecektir.
- Stok hesaplamalarında `magaza_id` ve `urun_id` birlikte kullanılacaktır.
- `satis`, `iade` ve `stok_girisi` olayları farklı iş kurallarına göre işlenecektir.
- Stok girişleri satış geliri olarak hesaplanmayacaktır.
- Tekrar eden kimlikler, ek doğrulama yapılmadan silinmeyecektir.
- Hatalı ve eksik kayıtların nasıl ele alınacağı daha büyük veri üzerinde belirlenecektir.
- Yapılan performans ölçümlerinde gerçek sonuçlar, kullanılan komutlar ve kanıt dosyaları kaydedilecektir.

## 6. Sonuç

Bu çalışmada Kafka üzerinden alınan 100 olaylık veri örneği incelenmiştir. Yedi temel alan tanımlanmış, olay türleri sınıflandırılmış ve mağaza ile ürün dağılımları analiz edilmiştir.

Örnek veri üzerinde eksik veya boş alan bulunmamıştır. Metinsel olarak tamamen aynı kayıt da tespit edilmemiştir. Bununla birlikte, farklı çalıştırmalarda sipariş kimliklerinin tekrar edebildiği görülmüştür.

Mantıksal tekrarlar, veri gecikmeleri ve performans üzerindeki etkiler sonraki aşamalarda ayrıntılı olarak incelenecektir.

**Veri Kaynağı:** `kanit/adim3_ornek100.jsonl`

**Not:** Bu rapordaki ölçümler yalnızca incelenen 100 kayıtlık örnek veri kümesine aittir.
