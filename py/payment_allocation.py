import pymysql
import pymysql.cursors
from collections import Counter
import csv
import pandas as pd
import re
from decimal import Decimal

BRANCHES = [
    {
        "DB_CONFIG" : {
            "host": "localhost",
            "user": "root",
            "password": "",
            "database": "db_tms",
            "charset": "utf8mb4"
        },
        "bsn": "QAV"
    },
    {
        "DB_CONFIG" : {
            "host": "localhost",
            "user": "root",
            "password": "",
            "database": "db_tms_mlb",
            "charset": "utf8mb4"
        },
        "bsn": "MLB"
    },
]
SI_TYPE = 2


for BRANCH in BRANCHES:
    conn = pymysql.connect(**BRANCH["DB_CONFIG"])
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # Disable strict mode for this session only
    cursor.execute("SET SESSION sql_mode = ''")

    # Fix AN other charges
    cursor.execute("""
        SELECT
            id,
            termsofleasecde
        FROM
            clientofferfile
        WHERE termsofleasecde IS NOT NULL
        AND termsofleasecde <> ''
        ORDER BY termsofleasecde DESC;
    """)
    award_notices = cursor.fetchall()

    for award_notice in award_notices:
        cursor.execute("""
            SELECT
                id,
                amt
            FROM
                or1
            WHERE posted = 1
            AND tc = %s
            AND bsn = %s
            AND snt = %s          
        """, (award_notice["termsofleasecde"], BRANCH["bsn"], SI_TYPE))
        or1 = cursor.fetchall()


        cursor.execute("""
            SELECT
                id,
                chargeamt
            FROM
                single_soa1
            WHERE stat = 1
            AND tc = %s
            AND bsn = %s
            AND snt = %s  
        """, (award_notice["termsofleasecde"], BRANCH["bsn"], SI_TYPE))
        single_soa1 = cursor.fetchall()

        gtotal_chrg = sum(
            si["chargeamt"] if si["chargeamt"] is not None else Decimal("0")
            for si in single_soa1
        )
        gtotal_pmt = sum(
            py["amt"] if py["amt"] is not None else Decimal("0")
            for py in or1
        )

        print((gtotal_chrg-gtotal_pmt), award_notice["termsofleasecde"], gtotal_chrg, gtotal_pmt)

    # Iterate on all tenants required
    # Iterate on or1 linked to the tenant
    # Iterate or2 linked to or1
