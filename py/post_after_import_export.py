import pymysql
import pymysql.cursors
from collections import Counter
import csv
import pandas as pd
import re

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "fishershub_tms_migration_uat4_3",  # ← Change to your target database
    "charset": "utf8mb4",
}

conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor(pymysql.cursors.DictCursor)

# Disable strict mode for this session only
cursor.execute("SET SESSION sql_mode = ''")



update = []
update2 = []

cursor.execute(
"""
    SELECT
        id,
        tms_contract_id,
        tms_awardnotice_id
    FROM
        t_m_s_a_n_other_charges
"""
)

t_m_s_a_n_other_charges = cursor.fetchall()

for chr in t_m_s_a_n_other_charges:
        chr_id = chr["id"]
        chr_tms_contract_no = chr["tms_contract_id"]
        chr_tms_awardnotice_no = chr["tms_awardnotice_id"]

        cursor.execute(
            """
            SELECT id, contracts_tmsawardnoticeid
            FROM t_m_s_contracts
            WHERE contracts_contractid = %s
            AND contracts_status = 10
            LIMIT 1
            """,
            (chr_tms_contract_no,)
        )

        t_m_s_contract = cursor.fetchone()

        if t_m_s_contract:
            update.append((chr_id, t_m_s_contract["id"]))
            update2.append((chr_id, t_m_s_contract["contracts_tmsawardnoticeid"]))
        


sql = "UPDATE t_m_s_a_n_other_charges SET tms_contract_id = %s WHERE id = %s"

# note: order must match query (value first, then id)
data = [(contractid, id_) for id_, contractid in update]

cursor.executemany(sql, data)
conn.commit()
print("updated:", len(data))

sql2 = "UPDATE t_m_s_a_n_other_charges SET tms_awardnotice_id = %s WHERE id = %s"
data2 = [(anid, id_) for id_, anid in update2]

cursor.executemany(sql2, data2)
conn.commit()
print("updated:", len(data2))