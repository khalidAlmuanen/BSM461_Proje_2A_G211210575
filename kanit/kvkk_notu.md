KVKK NOTU — KİŞİSEL VERİ DEĞERLENDİRMESİ
Ders: BSM 461 — Büyük Veriye Giriş
Proje: Şablon 2 — Gerçek Zamanlı Satış ve Stok Takibi
Şube: 2A
Grup Kodu: G211210575
İnceleme Tarihi: 24.11.2026
1. Amaç ve İncelenen Veri
Bu not, Adım 3 ve Z10 kapsamında proje verisinde kişiyi doğrudan veya dolaylı olarak tanımlayabilecek alanlar bulunup bulunmadığını değerlendirmek için hazırlanmıştır.

İncelenen kaynak, ders kitinin sentetik veri üreticisinden Kafka satislar konusu üzerinden alınarak kanit/adim3_ornek100.jsonl dosyasına kaydedilen 100 olaylık örnektir.
2. Alanların Değerlendirilmesi
Alan	Değerlendirme
olay_zamani	Olay zamanı; tek başına doğrudan kişi kimliği değildir.
siparis_id	Sipariş kimliği; başka sistemlerde müşteri kaydıyla eşleştirilirse dolaylı tanımlama riski taşıyabilir.
magaza_id	Mağaza kimliği.
urun_id	Ürün kimliği.
adet	İşlem miktarı.
birim_fiyat	Birim fiyat.
tur	Olay türü (satis, iade, stok_girisi).


İncelenen yedi alan içinde ad-soyad, telefon numarası, e-posta adresi, T.C. kimlik numarası veya IP adresi gibi doğrudan kişiyi tanımlayan bir alan gözlemlenmemiştir. Bu tespit yalnızca incelenen veri yapısı ve örnek için geçerlidir.
3. Önerilen Koruma Yaklaşımı
1. Analiz için gerekli olmayan müşteri kimlik bilgileri veri hattına eklenmeyecektir (veri minimizasyonu).
2. Gerçek müşteri bilgileri, ham olaylar veya kimliği belirleyebilecek bağlantı tabloları herkese açık GitHub deposuna yüklenmeyecektir.
3. Gerçek kişiyle bağlantı kurulabilen bir sipariş kaynağı sonradan eklenirse siparis_id ve ilgili alanlar yeniden değerlendirilecek; gerekli olduğunda maskeleme veya takma adlandırma (pseudonymization) uygulanacaktır.
4. Rapor, grafik ve ekran görüntülerinde mümkün olduğunca toplulaştırılmış sonuçlar kullanılacaktır.
5. Senaryo kartındaki 1095 günlük saklama süresi, bu eğitim senaryosunda ham olay verisi için teknik kapasite gereksinimidir; gerçek kişisel verilerin aynı süreyle otomatik olarak saklanması gerektiği anlamına gelmez.
4. Sonuç ve Kapsam Sınırı
İncelenen sentetik satış verisinin yedi temel alanında doğrudan kişi tanımlayıcı bilgi görülmemiştir. Ancak siparis_id başka kayıtlarla ilişkilendirilirse dolaylı tanımlama ihtimali bulunduğundan gereksiz kişisel veri toplanmaması ve gerçek müşteri kayıtlarının projeye veya herkese açık depoya eklenmemesi benimsenmiştir.

Bu not, proje yönergesinin Adım 3 / Z10 gereksinimi için yapılan ön değerlendirmedir. İleride yeni alanlar veya veri kaynakları eklendiğinde güncellenmelidir.

Dayanak veri: kanit/adim3_ornek100.jsonl
İlgili belge: kanit/veri_sozlugu.md