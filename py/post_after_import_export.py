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
    "database": "fishershub_tms_migration_uat4",  # ← Change to your target database
    "charset": "utf8mb4",
}

DB_CONFIG_TMS = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "db_tms",  # ← Change to your target database
    "charset": "utf8mb4",
}

conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor(pymysql.cursors.DictCursor)
# Disable strict mode for this session only
cursor.execute("SET SESSION sql_mode = ''")

run_leasing = False
run_ebm = True

if run_leasing:
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



if run_ebm:
    update = []
    ignore = []
    no_match = []

    cursor.execute(
        """
            SELECT
                e_b_m_events_other_services.id,
                e_b_m_events_other_services.service_id,
                e_b_m_events_epf_information.branch as mallid
            FROM
                e_b_m_events_other_services
            LEFT JOIN 
                e_b_m_events_epf_information ON e_b_m_events_epf_information.id = e_b_m_events_other_services.epf_id 
            WHERE 
                e_b_m_events_epf_information.branch IS NOT NULL
        
        """
    )
    e_b_m_events_other_services = cursor.fetchall()

    cursor.execute("SELECT * FROM t_m_s_charges WHERE document = %s", ("moa"))
    new_chargeslist = cursor.fetchall()


    conn = pymysql.connect(**DB_CONFIG_TMS)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    # Disable strict mode for this session only
    cursor.execute("SET SESSION sql_mode = ''")
    cursor.execute(
        """
            SELECT
                id,
                chargescde,
                chargesdesc
            FROM
                chargesfile
        """
    )
    old_chargeslist = cursor.fetchall()
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    # Disable strict mode for this session only
    cursor.execute("SET SESSION sql_mode = ''")



    for chrg in e_b_m_events_other_services:
        chrg_id = int(chrg["id"])
        chrg_code = str(chrg["service_id"]).strip()
        chrg_mallid = int(chrg["mallid"])


        old_tms_charge = next(
            (
                row
                for row in old_chargeslist
                if str(row["chargescde"]) == str(chrg_code)
            ),
            None,
        )

        if old_tms_charge:
            chr_chg_desc = str(old_tms_charge["chargesdesc"])

            if (
                chr_chg_desc == "ADVANCE RENT"
            ):
                chr_chg_desc = "ADVANCE RENT EXHIBIT"

            if (
                chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA"
                or chr_chg_desc
                == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA - STRAIGHT PERCENTAGE"
                or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA (PERCENTAGE)"
                or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES - AFFI"
                or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES INDOOR"
                or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES INDOOR (PERCENTAGE)"
                or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES (KINETIC QA)"
                or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES OUTDOOR"
            ):
                chr_chg_desc = "COMMON AREA MAINTENANCE CHARGES PER UNIT"

            if (
                chr_chg_desc == "NEW JANITORIAL POSTING (WITHIN MALL HOURS)"
            ):
                chr_chg_desc = "JANITORIAL POSTING (WITHIN MALL HOURS)"

            if (
                chr_chg_desc == "NEW JANITORIAL POSTING (BEYOND MALL HOURS)"
                or chr_chg_desc == "NEW JANITORIAL POSTING (BEYOND MALL HOURS) "
            ):
                chr_chg_desc = "JANITORIAL POSTING (BEYOND MALL HOURS)"

            if (
                chr_chg_desc == "NEW SECURITY POSTING (PER HOUR)"
            ):
                chr_chg_desc = "SECURITY POSTING ( PER HOUR)"

            if (
                chr_chg_desc == "NEW SECURITY POSTING (3 HOURS)"
            ):
                chr_chg_desc = "SECURITY POSTING (3 HOURS)"


            if (
                chr_chg_desc == "NEW ENGINEERING POSTING" or chr_chg_desc == "NEW ENGINEERING POSTING "
            ):
                chr_chg_desc = "ENGINEERING POSTING"

            if (
                chr_chg_desc == "RENTAL RATE"
            ):
                chr_chg_desc = "BASIC/BASE RENT EXHIBIT"

            
                

            new_tms_charge = next(
                (
                    row
                    for row in new_chargeslist
                    if str(row["description"]) == chr_chg_desc
                    and int(row["mall_id"]) == int(chrg_mallid)
                ),
                None,
            )

            if new_tms_charge:
                update.append((chrg_id, new_tms_charge["id"]))
            else:
                ignore.append(chr_chg_desc)
                no_match.append(chr_chg_desc)
        else:
            no_match.append(str(chrg_code))


    print("No DB Match: ", set(no_match))
    sql = "UPDATE e_b_m_events_other_services SET service_id = %s WHERE id = %s"

    data = [(val, id_) for id_, val in update]

    cursor.executemany(sql, data)
    conn.commit()

    print("Total items updated:", len(data))

