import csv
import os
from datetime import datetime, timedelta


def generate_big_stock_csv(
    output_path: str,
    row_count: int = 200_000,
    start_date: str = "2025-01-01",
) -> None:
    """Generate a large CSV for testing stock import.

    Columns follow the default `stocks` config:
    - sku (str, mandatory)
    - warehouse_code (str, mandatory)
    - stock_pcs (int, mandatory)
    - stock_box (int)
    - stock_cs (int)
    - date (date, optional)
    """

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    header = [
        "sku",
        "warehouse_code",
        "stock_pcs",
        "stock_box",
        "stock_cs",
        "date",
    ]

    base_date = datetime.strptime(start_date, "%Y-%m-%d")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for i in range(1, row_count + 1):
            sku = f"SKU{i:06d}"
            warehouse_code = f"WH{i % 10:03d}"  # 10 gudang
            stock_pcs = (i * 3) % 1000
            stock_box = (i * 2) % 200
            stock_cs = i % 50
            date_val = base_date + timedelta(days=i % 365)

            writer.writerow([
                sku,
                warehouse_code,
                stock_pcs,
                stock_box,
                stock_cs,
                date_val.strftime("%Y-%m-%d"),
            ])

    print(f"Generated {row_count} rows to {output_path}")


if __name__ == "__main__":
    # Default: buat file besar untuk tabel `stocks`.
    # Nama file bisa kamu sesuaikan dengan `allowed_filename` di Master Config.
    output_file = "pv_inventory_big.csv"  # ganti kalau perlu
    generate_big_stock_csv(output_file, row_count=200_000)
