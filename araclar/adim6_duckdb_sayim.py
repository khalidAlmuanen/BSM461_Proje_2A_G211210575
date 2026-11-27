
import duckdb

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

    yol = "s3://islenmis/satis/*/*.parquet"

    sayi = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [yol]
    ).fetchone()[0]

    print("=== ADIM 6 KAYIT KONTROLU ===")
    print("Ham veri kayit sayisi:", 3000000)
    print("Parquet kayit sayisi:", sayi)
    print("Kayit farki:", 3000000 - sayi)

finally:
    con.close()
