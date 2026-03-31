import pymysql
import csv
import os

# === CONFIGURATION ===
host = "localhost"
user = "root"
password = ""

databases = ["db_tms", "db_tms_mlb"]  # 👈 both databases here
output_folder = "generated_exports"

# Connect to MySQL (no default DB needed)
conn = pymysql.connect(
    host=host,
    user=user,
    password=password
)
cursor = conn.cursor()

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

for database in databases:
    print(f"\n🔄 Processing database: {database}")

    # Select database
    cursor.execute(f"USE `{database}`;")

    output_file = f"{database}_last_ids_per_table.csv"
    output_path = os.path.join(output_folder, output_file)

    # Get list of tables
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    results = []

    for table in tables:
        table_name = table[0]
        try:
            cursor.execute(f"SELECT MAX(id) FROM `{table_name}`;")
            result = cursor.fetchone()
            last_id = result[0] if result[0] is not None else 0
            results.append({
                "table_name": table_name,
                "last_id": last_id
            })
        except Exception as e:
            print(f"⚠️ Error in {database}.{table_name}: {e}")
            results.append({
                "table_name": table_name,
                "last_id": None
            })

    # Write CSV
    with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["table_name", "last_id"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ Saved: {output_path}")

# Close connection
cursor.close()
conn.close()