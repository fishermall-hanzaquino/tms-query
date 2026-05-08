import os
import csv
import re

init_file = "init_ref_qav.csv"
new_file = "generated_exports\\db_tms_last_ids_per_table.csv"

init_ids = {}
new_ids = {}

def clean(x):
    return x.strip().lower()

# Load init
with open(init_file, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        init_ids[clean(row["table_name"])] = row["last_id"].strip()

# Load new
with open(new_file, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        new_ids[clean(row["table_name"])] = row["last_id"].strip()

# Build mapping + debug missing ones
id_map = {}
missing = []

for table, old_id in init_ids.items():
    if table in new_ids:
        new_id = new_ids[table]

        if old_id and new_id:
            id_map[old_id] = new_id
    else:
        missing.append(table)

print("Mapped IDs:", len(id_map))
print("Missing tables:", missing)


def refactorfile(path, id_map):
    temp_path = path + ".tmp"

    with open(path, "r", encoding="utf-8", errors="ignore") as infile, \
         open(temp_path, "w", encoding="utf-8") as outfile:

        for line in infile:

            # replace OLD -> NEW using whole-word match
            for old_id, new_id in id_map.items():
                line = re.sub(rf"\b{re.escape(old_id)}\b", new_id, line)

            # optional: fix escaping issue
            line = line.replace("\\'", "''")

            outfile.write(line)

    os.replace(temp_path, path)
    print(f"Updated: {path}")


folders = ["acc_queries","leasing_queries","ops_queries"]

for folder in folders:
    for filename in os.listdir(folder):
        full_path = os.path.join(folder, filename)
        
        refactorfile(full_path, id_map)
           





