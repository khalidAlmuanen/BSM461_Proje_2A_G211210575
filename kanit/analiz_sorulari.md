ANALİZ SORULARI — TASLAK
Ders: BSM 461 — Büyük Veriye Giriş
Proje: Şablon 2 — Gerçek Zamanlı Satış ve Stok Takibi
Şube: 2A
Grup Kodu: G211210575
Durum: Grup onayı bekleyen taslak
Kaynak: Dönem Projesi Yönergesi, Adım 3 / Şablon 2; G211210575 senaryo kartı
1. Amaç
Proje için iki toplu (Batch) ve iki akış (Streaming) sorusu seçilmiştir. Sorular birbirinden farklı iş ihtiyaçlarını karşılar. Sonuçlar gerçek veri, çalışan kod, ölçüm ve kanıt dosyalarıyla desteklenecektir. Bu dosya taslaktır; grup kararı verilmeden sorular kesinleşmiş sayılmaz.
2. Toplu İşleme (Batch) — Z5
B1. Son 14 günde en çok satılan ürünler ve kategoriler hangileridir?
- Amaç: Ürün ve kategori bazında satılan birim miktarını karşılaştırmak.
- Veri: S3 üzerindeki geçmiş satış olayları; urunler.csv boyut tablosu.
- İşlem: Veri temizleme -> tur == 'satis' filtresi -> urun_id ile JOIN -> groupBy ve sum(adet) -> azalan sıralama.
- Çıktı: En çok satılan ilk 10 ürün ve kategorilere göre toplam satış adedi.
- Grafik: En çok satılan ürünler için sütun grafik (Z10).
- Doğrulama: Kullanılan olay sayısı, bozuk/tekrar kayıt politikası ve hesaplanan toplamlar kaydedilecek.
- Önemli not: Bu soruda "en çok satan" ölçütü satılan adet olarak tanımlanmıştır; ciro ile karıştırılmayacaktır.
B2. Mağazalara göre iade oranları nasıl değişmektedir?
- Amaç: Mağazaları iade edilen ürün adedinin satılan ürün adedine oranına göre karşılaştırmak.
- Veri: S3 üzerindeki geçmiş satış/iade olayları; magazalar.csv boyut tablosu.
- İşlem: Temizleme -> mağaza ve olay türüne göre sum(adet) -> mağaza bazında oran -> magaza_id ile JOIN.
- Ölçüt: iade_orani_yuzde = 100 * toplam_iade_adedi / toplam_satis_adedi.
- Çıktı: Mağaza bazında satış adedi, iade adedi, iade oranı; uygunsa şehir karşılaştırması.
- Grafik: Mağazalara göre iade oranı sütun grafiği (Z10).
- Doğrulama: toplam_satis_adedi = 0 olan mağazalar için oran tanımsız olarak raporlanacak; sıfıra bölme yapılmayacak.
- Önemli not: Bu metrik, aynı siparişe bağlanmış gerçek iade yüzdesi değildir. Üreticide iade olaylarının önceki satışlarla bağlantısı ayrıca incelenecektir. Aynı tarih aralığı kullanılacaktır.
3. Akış İşleme (Streaming) — Z8
S1. Hangi mağazalarda hangi ürünlerin stokları kritik seviyenin altına düşmüştür?
- Amaç: Ürün/mağaza bazında kritik stok uyarısı oluşturmak.
- Veri: Kafka satislar olayları; başlangıç stok tablosu.
- İşlem: magaza_id + urun_id anahtarıyla stok durumunu güncelleme; satış, stok girişi ve iade türlerine uygun kurallar.
- Çıktı: Mağaza, ürün, güncel tahmini stok, kritik eşik, uyarı zamanı.
- Karar bekliyor: Kritik stok eşiğinin kaynağı ve iadenin yeniden satılabilir stoğa eklenip eklenmeyeceği.
- Kalite: Tekrar olayları iki kez işlenmemeli; geç olaylar için düzeltme/yeniden işleme stratejisi tanımlanmalı.
S2. Mağaza bazında son bir saatlik net satış cirosu nasıl değişmektedir?
- Amaç: Mağazaların yakın dönem net cirosunu zaman içinde izlemek.
- Veri: Kafka satislar olayları.
- İşlem: Olay zamanına dayalı kayan/zaman penceresi; satış tutarı adet * birim_fiyat, iadeler için iş kuralına bağlı negatif düzeltme; stok girişi ciroya dahil edilmez.
- Çıktı: Mağaza, pencere başlangıcı/sonu, geçici net ciro, güncelleme zamanı.
- Karar bekliyor: Pencere kaydırma aralığı, watermark, sonuç güncelleme ve sonucun kesinleşme yöntemi.
- Önemli not: 120 saniye uyarı süresi, olayların %15inin 30 dakika gecikmesi ve geç kaldığı için sıfır olay kaybı koşulları birbiriyle çelişebilir. İlk sonuç / güncellenmiş sonuç ayrımı açıkça tanımlanacaktır. Hocadan 120 saniyenin başlangıç noktası sorulmuştur; resmi yanıt henüz kaydedilmemiştir.
4. Senaryo Koşulları (Sabit)
- Ortalama hız: 200 olay/saniye.
- Tepe yük: ortalamanın 4 katı = 800 olay/saniye.
- Akış uyarı süresi: en fazla 120 saniye.
- Geç olaylar: %15, en fazla 30 dakika gecikme.
- Geç kaldığı için sayıma girmeyen olay: 0 tolerans.
- Ham veri saklama: 1095 gün.
- Servis sorgusu: p99 <= 200 ms.
Bu değerler senaryo kartından alınmıştır. Ölçülmüş sistem başarımı olarak sunulmaz.
5. Sorumluluklar
- Üye 1: Batch soruları B1, B2 — Z2, Z4, Z5, Z7.
- Üye 2: Streaming soruları S1, S2 — Z3, Z6, Z8, Z9.
- Üye 3: Z1, Z10; Batch grafiklerinin hazırlanmasında sonuçları Üye 1'den alır.
- Ortak: Z0 mimari ve Z11 kırılma deneyi; entegrasyon ve rapor kontrolü.
6. Kabul ve Kanıt Kriterleri
1. Dört soru grup tarafından onaylanmış ve raporun özetinde açıkça yazılmış olmalı.
2. B1 ve B2 gerçek Batch sorgularıyla cevaplanmalı; en az iki grafik ve bir cümlelik bulgu hazırlanmalı.
3. S1 ve S2 gerçek Streaming hattıyla cevaplanmalı; senaryo koşulları ve geç veri davranışı test edilmeli.
4. Veri temizleme, tekrar kayıt politikası ve ölçüm yöntemleri belgelendirilmeli.
5. Kullanılan komut, veri boyutu, gerçek sonuç, tekrar sayısı ve kanıt dosyası kaydedilmeli. Ölçüm yapılmadan başarı iddia edilmeyecek.
6. GitHub'a entegrasyon öncesinde ekipçe değişiklikler gözden geçirilecek.
7. Henüz Kesinleştirilmemiş Kararlar
- [ ] Grup, dört soruyu onayladı.
- [ ] Kritik stok eşiği belirlendi ve gerekçelendirildi.
- [ ] İade ve stok güncelleme iş kuralları belirlendi.
- [ ] Ciro penceresi, kaydırma aralığı ve geç veri düzeltmesi kararlaştırıldı.
- [ ] 120 saniye ölçüm başlangıcı için hocanın yanıtı kaydedildi (gelirse).
- [ ] Batch ve Streaming sonuç şemaları ekipçe paylaşıldı.
Not: Bu dosyada uygulama sonuçları, ölçüm süreleri veya grafik bulguları uydurulmamıştır.