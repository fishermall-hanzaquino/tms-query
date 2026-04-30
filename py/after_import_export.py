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
    "database": "fishershub_tms_migration",  # ← Change to your target database
    "charset": "utf8mb4",
}

conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor(pymysql.cursors.DictCursor)

# Disable strict mode for this session only
cursor.execute("SET SESSION sql_mode = ''")

# Fix AN other charges
cursor.execute("SELECT * FROM t_m_s_charges WHERE document = %s", ("an"))

# rental scheme

t_m_s_charges = cursor.fetchall()


cursor.execute("SELECT * FROM t_m_s_charges WHERE document = %s", ("moa"))

# rental scheme

t_m_s_charges_moa = cursor.fetchall()

run_leasing = True

run_accounting = True

run_treasury = True


isFood = {
    "Affiliate": False,
    "Amusement/Recreational": False,
    "Bakeshop": True,
    "Bank/ATM": False,
    "Bar": True,
    "Beauty and Wellness Services": False,
    "Beauty and Wellness Products": False,
    "Beauty Salon": False,
    "Coffee Shop": True,
    "Donuts": True,
    "Drug Store/Convenience Store": False,
    "Fast Food": True,
    "Fashion Apparel": False,
    "Footwear/Shoes Store": False,
    "Gadgets/Cellphones": False,
    "Gym": False,
    "Office": False,
    "Radio Room/Telecommunications": False,
    "Restaurant": True,
    "Retail Shop/Store": False,
    "Specialty Food/Beverages": True,
    "Specialty Shop/Store": True,
    "Services/Service Center": False,
    "Appliance/Furniture Store": False,
    "Optical Shop/Services": False,
    "Jewelry Shop": False,
    "Books/School/Office Supplies": False,
    "Casual Dining": True,
    "Storage": False,
    "Hardware/Homeware": False,
    "Canteen": True,
    "Education/Learning Center": False,
    "Medical Services": False,
    "Textile/Tailoring": False,
    "Laptops, Cellphones & Gadgets": False,
    "Services": False,
    "CAKES & PASTRIES": True,
    "CELLSITES": False,
    "MEDICAL CLINIC": False,
    "TATTOO SHOP": False,
    "RECREATION": False,
    "Food & Beverages": True,
    "Laptops, Desktop and Gadgets": False,
    "Car/Auto/Bicycle/Motorcycle Accessories": False,
    "Convenience Store": False,
    "Mobile Phones & Accessories": False,
    "Watches": False,
    "Gadget Accessories": False,
    "Buffet": True,
    "Breads/Pastries": True,
    "Clinics & Laboratories": False,
    "Photography Services": False,
    "Regional Delicacies": False,
    "Other Services": False,
    "Logistic Services": False,
    "Pet Shop": False,
    "Massage Chairs": False,
}
lookup_qv = {}
with open("generated_exports\\sn_map_qav.csv", newline="") as csvfile1:
    reader1 = csv.reader(csvfile1)
    for row in reader1:
        # strip spaces in case CSV has them
        a_sn1 = row[1].strip()
        a_id1 = row[0].strip()
        lookup_qv[a_sn1] = a_id1
lookup_mb = {}
with open("generated_exports\\sn_map_mlb.csv", newline="") as csvfile2:
    reader2 = csv.reader(csvfile2)
    for row in reader2:
        # strip spaces in case CSV has them
        a_sn = row[1].strip()
        a_id = row[0].strip()
        lookup_mb[a_sn] = a_id


################################## LEASING POST MIGRATION SCRIPT ##################################
if run_leasing:
    cursor.execute(
        """
    SELECT
        t_m_s_a_n_other_charges.id,
        t_m_s_a_n_other_charges.remarks as chg_desc,
        (
                SELECT
                    mallid
                FROM
                t_m_s_award_notices
                WHERE
                    t_m_s_award_notices.id = t_m_s_a_n_other_charges.tms_awardnotice_id
                LIMIT
                    1
            ) AS mallid,
        (
                SELECT
                    t_m_s_business_categories.description
                FROM
                t_m_s_award_notices
                LEFT JOIN t_m_s_business_categories ON t_m_s_business_categories.id = t_m_s_award_notices.categoryid
                WHERE
                    t_m_s_award_notices.id = t_m_s_a_n_other_charges.tms_awardnotice_id
                LIMIT
                    1
            ) AS buss_category,
        COALESCE(
            (
                SELECT
                    rentalschemeid
                FROM
                    t_m_s_a_n_rental_charges
                WHERE
                    t_m_s_a_n_rental_charges.tms_contract_id = t_m_s_a_n_other_charges.tms_contract_id
                    AND CURDATE() BETWEEN t_m_s_a_n_rental_charges.fromdate
                    AND t_m_s_a_n_rental_charges.todate
                ORDER BY
                    id DESC
                LIMIT
                    1
            ), (
                SELECT
                    rentalschemeid
                FROM
                    t_m_s_a_n_rental_charges
                WHERE
                    t_m_s_a_n_rental_charges.tms_contract_id = t_m_s_a_n_other_charges.tms_contract_id
                ORDER BY
                    id DESC
                LIMIT
                    1
            )
        ) AS rentalschemeid
    FROM
        t_m_s_a_n_other_charges
    WHERE NOT t_m_s_a_n_other_charges.tms_contract_id IS NULL
    AND NOT t_m_s_a_n_other_charges.tms_contract_id = ''
    ORDER BY rentalschemeid asc
    """
    )

    t_m_s_a_n_other_charges = cursor.fetchall()
    update = []
    ignored = []

    for chr in t_m_s_a_n_other_charges:
        chr_id = chr["id"]
        chr_chg_desc = chr["chg_desc"]
        chr_mallid = chr["mallid"]
        chr_rentschemeid = chr["rentalschemeid"]
        chr_buss_cat = chr["buss_category"]

        if (
            chr_chg_desc == ""
            or chr_chg_desc is None
            or chr_mallid is None
            or chr_rentschemeid is None
        ):
            continue

        if (
            chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA"
            or chr_chg_desc
            == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA - STRAIGHT PERCENTAGE"
            or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA (PERCENTAGE)"
            or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES - AFFI"
            or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES INDOOR"
            or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES INDOOR (PERCENTAGE)"
            or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES (KINETIC QA)"
            or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES (SSS QA)"
        ):
            if int(chr_rentschemeid) == 1 or int(chr_rentschemeid) == 2:
                chr_chg_desc = "COMMON AREA MAINTENANCE CHARGES TOTAL AREA (FIXED)"
            elif int(chr_rentschemeid) == 3 or int(chr_rentschemeid) == 4:
                chr_chg_desc = "COMMON AREA MAINTENANCE CHARGES TOTAL AREA (PERCENTAGE)"

        if (
            chr_chg_desc == "ADVERTISING FUND"
            or chr_chg_desc == "ADVERTISING FUND (KINETIC QA)"
        ):
            if int(chr_rentschemeid) == 1 or int(chr_rentschemeid) == 2:
                chr_chg_desc = "ADVERTISING FUND FIXED"
            elif int(chr_rentschemeid) == 3 or int(chr_rentschemeid) == 4:
                chr_chg_desc = "ADVERTISING FUND PERCENTAGE"

        if chr_chg_desc == "ADVERTISING FUND - SPECIAL":
            if int(chr_rentschemeid) == 1 or int(chr_rentschemeid) == 2:
                chr_chg_desc = "ADVERTISING FUND - SPECIAL FIXED"
            elif int(chr_rentschemeid) == 3 or int(chr_rentschemeid) == 4:
                chr_chg_desc = "ADVERTISING FUND - SPECIAL PERCENTAGE"

        if chr_chg_desc == "ADVERTISING FUND - FOOD HALL TENANT":
            if int(chr_rentschemeid) == 1 or int(chr_rentschemeid) == 2:
                chr_chg_desc = "ADVERTISING FUND - FOOD HALL TENANT FIXED"
            elif int(chr_rentschemeid) == 3 or int(chr_rentschemeid) == 4:
                chr_chg_desc = "ADVERTISING FUND - FOOD HALL TENANT PERCENTAGE"

        if (
            chr_chg_desc == "AIR CONDITION - AFFI"
            or chr_chg_desc == "AIR CONDITION (SSS QA)"
        ):
            chr_chg_desc = "AIR CONDITION"

        if (
            chr_chg_desc == "PEST CONTROL"
            or chr_chg_desc == "PEST CONTROL (DS)"
            or chr_chg_desc == "PEST CONTROL - BIR MALABON"
            or chr_chg_desc == "PEST CONTROL - FIXED PER UNIT"
            or chr_chg_desc == "PEST CONTROL (SSS QA)"
            or chr_chg_desc == "PEST CONTROL (FSM)"
        ):
            if chr_buss_cat is not None:
                if isFood[str(chr_buss_cat).strip()]:
                    chr_chg_desc = "PEST CONTROL - FOOD TENANT"
                else:
                    chr_chg_desc = "PEST CONTROL - NON-FOOD TENANT"
            else:
                chr_chg_desc = "PEST CONTROL - NON-FOOD TENANT"

        match = next(
            (
                row
                for row in t_m_s_charges
                if row["mall_id"] == chr_mallid
                and int(row["rental_scheme_id"]) == int(chr_rentschemeid)
                and str(row["description"]).strip().upper()
                == str(chr_chg_desc).strip().upper()
            ),
            None,
        )
        if match is not None:
            update.append((chr_id, match["id"]))
        else:
            ignored.append(chr_chg_desc)

        # print((chr_id, match))
        # break

    # count occurrences
    counts = Counter(ignored)

    print("Unique values:", list(counts.keys()))
    print("Total items:", len(ignored))
    print("\nCounts:")

    for value, count in counts.most_common():
        print(f"{value}: {count}")

    sql = "UPDATE t_m_s_a_n_other_charges SET applicable_charge_id = %s WHERE id = %s"

    # note: order must match query (value first, then id)
    data = [(val, id_) for id_, val in update]

    cursor.executemany(sql, data)
    conn.commit()

    sheets = pd.read_excel("py\\ref\\location-tagging.xlsx", sheet_name=None)

    # access a specific sheet
    tms_qav_locations = sheets["TMS QAV"]
    tms_mlb_locations = sheets["TMS MLB"]
    ebm_qav_locations = sheets["EBM QAV"]
    ebm_mlb_locations = sheets["EBM MLB"]

    for index, row in tms_qav_locations.iterrows():
        loccde = str(row.iloc[2])
        clsification = str(row.iloc[4]) # mall - 1, foodhall - 2, parkway - 3
        if clsification == "INACTIVE" or clsification == "":
            continue

        sql6 = "UPDATE locations SET location_type_id = %s WHERE locationcode = %s AND mallid = 2"
    
        cursor.execute(sql6, (clsification, loccde))
        conn.commit()

    for index, row in tms_mlb_locations.iterrows():
        loccde = str(row.iloc[3])
        clsification = str(row.iloc[6]) # mall - 1, foodhall - 2, parkway - 3
        if clsification == "INACTIVE" or clsification == "":
            continue

        sql7 = "UPDATE locations SET location_type_id = %s WHERE locationcode = %s AND mallid = 1"
    
        cursor.execute(sql7, (clsification, loccde))
        conn.commit()

################################## ACCOUNTING POST MIGRATION SCRIPT ##################################
if run_accounting:
    cursor.execute(
        """
        SELECT 
            t_m_s_service_invoice_charges.id,
            t_m_s_service_invoice_charges.charge_description,
            t_m_s_service_invoice_charges.t_m_s_service_invoice_id,
            t_m_s_service_invoice_charges.award_notice_no,
            t_m_s_service_invoice_charges.hash,
            t_m_s_service_invoice_charges.posting_date,
            t_m_s_service_invoice_charges.t_m_s_service_invoice_id,
            t_m_s_service_invoice_charges.amount,
            (
                SELECT
                    t_m_s_business_categories.description
                FROM
                t_m_s_award_notices
                LEFT JOIN t_m_s_business_categories ON t_m_s_business_categories.id = t_m_s_award_notices.categoryid
                WHERE
                    t_m_s_award_notices.anno = t_m_s_service_invoice_charges.award_notice_no
                LIMIT
                    1
            ) AS buss_category,
            COALESCE((SELECT 
                            rentalschemeid
                        FROM
                            t_m_s_a_n_rental_charges
                                LEFT JOIN
                            t_m_s_award_notices ON t_m_s_award_notices.id = t_m_s_a_n_rental_charges.awardnoticeid
                        WHERE
                            t_m_s_award_notices.anno = t_m_s_service_invoice_charges.award_notice_no
                                AND CURDATE() BETWEEN t_m_s_a_n_rental_charges.fromdate AND t_m_s_a_n_rental_charges.todate
                        ORDER BY t_m_s_a_n_rental_charges.id DESC
                        LIMIT 1),
                    (SELECT 
                            rentalschemeid
                        FROM
                            t_m_s_a_n_rental_charges
                                LEFT JOIN
                            t_m_s_award_notices ON t_m_s_award_notices.id = t_m_s_a_n_rental_charges.awardnoticeid
                        WHERE
                            t_m_s_award_notices.anno = t_m_s_service_invoice_charges.award_notice_no
                        ORDER BY t_m_s_a_n_rental_charges.id DESC
                        LIMIT 1)) AS rentalschemeid
        FROM
            t_m_s_service_invoice_charges
        """
    )
    t_m_s_service_invoice_charges = cursor.fetchall()

    update2 = []
    ignored2 = []
    payments2 = []

    # t_m_s_service_invoice_id, ewt_code, charge_id, charge_type, charge_code, non_vatable, priority_order

    for chrg in t_m_s_service_invoice_charges:
        chrg_id = chrg["id"]
        chrg_description = str(chrg["charge_description"]).strip()
        chrg_award_notice_no = str(chrg["award_notice_no"]).strip()
        chrg_rentschemeid = chrg["rentalschemeid"]
        chrg_soaid = str(chrg["t_m_s_service_invoice_id"])
        chrg_buss_category = chrg["buss_category"]
        chrg_posting_date = chrg["posting_date"]
        chrg_amount = chrg["amount"]
        chrg_sn = str(chrg["hash"]).strip()

        up_t_m_s_service_invoice_id = ""
        if chrg_sn == "SOAEXTFILE":
            up_t_m_s_service_invoice_id = chrg_soaid
        else:
            up_t_m_s_service_invoice_id = ""
            chrg_mallid = 1
            if "QAV" in str(chrg_award_notice_no).upper():
                chrg_mallid = 2
                up_t_m_s_service_invoice_id = lookup_qv.get(chrg_sn)
                if (
                    up_t_m_s_service_invoice_id is not None
                    and up_t_m_s_service_invoice_id != ""
                ):
                    up_t_m_s_service_invoice_id = int(up_t_m_s_service_invoice_id)
            elif "MLB" in str(chrg_award_notice_no).upper():
                chrg_mallid = 1
                up_t_m_s_service_invoice_id = lookup_mb.get(chrg_sn)
                if (
                    up_t_m_s_service_invoice_id is not None
                    and up_t_m_s_service_invoice_id != ""
                ):
                    up_t_m_s_service_invoice_id = int(up_t_m_s_service_invoice_id) + 132088 + 17422

        if chrg_description == "" or chrg_description is None or chrg_rentschemeid is None:
            continue

        if (
            chrg_description == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA"
            or chrg_description
            == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA - STRAIGHT PERCENTAGE"
            or chrg_description == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA (PERCENTAGE)"
            or chrg_description == "COMMON AREA MAINTENANCE CHARGES - AFFI"
            or chrg_description == "COMMON AREA MAINTENANCE CHARGES INDOOR"
            or chrg_description == "COMMON AREA MAINTENANCE CHARGES INDOOR (PERCENTAGE)"
            or chrg_description == "COMMON AREA MAINTENANCE CHARGES (KINETIC QA)"
            or chrg_description == "COMMON AREA MAINTENANCE CHARGES (SSS QA)"
        ):
            if int(chrg_rentschemeid) == 1 or int(chrg_rentschemeid) == 2:
                chrg_description = "COMMON AREA MAINTENANCE CHARGES TOTAL AREA (FIXED)"
            elif int(chrg_rentschemeid) == 3 or int(chrg_rentschemeid) == 4:
                chrg_description = "COMMON AREA MAINTENANCE CHARGES TOTAL AREA (PERCENTAGE)"

        if (
            chrg_description
            == "HAZARDOUS WASTE DISPOSAL - PARTICIPATION FEE (SHORT-TERM, NON-FOOD)"
        ):
            chrg_description = (
                "HAZARDOUS WASTE DISPOSAL - PARTICIPATION FEE (SHORT TERM, NON-FOOD)"
            )

        if (
            chrg_description
            == "HAZ. WASTE DISPOSAL - PARTICIPATION FEE (INLINE, FOOD)"
        ):
            chrg_description = (
                "HAZARDOUS WASTE DISPOSAL - PARTICIPATION FEE (INLINE, FOOD)"
            )

        if (
            chrg_description == "ADVERTISING FUND"
            or chrg_description == "ADVERTISING FUND (KINETIC QA)"
        ):
            if int(chrg_rentschemeid) == 1 or int(chrg_rentschemeid) == 2:
                chrg_description = "ADVERTISING FUND FIXED"
            elif int(chrg_rentschemeid) == 3 or int(chrg_rentschemeid) == 4:
                chrg_description = "ADVERTISING FUND PERCENTAGE"

        if chrg_description == "ADVERTISING FUND - SPECIAL":
            if int(chrg_rentschemeid) == 1 or int(chrg_rentschemeid) == 2:
                chrg_description = "ADVERTISING FUND - SPECIAL FIXED"
            elif int(chrg_rentschemeid) == 3 or int(chrg_rentschemeid) == 4:
                chrg_description = "ADVERTISING FUND - SPECIAL PERCENTAGE"

        if chrg_description == "ADVERTISING FUND - FOOD HALL TENANT":
            if int(chrg_rentschemeid) == 1 or int(chrg_rentschemeid) == 2:
                chrg_description = "ADVERTISING FUND - FOOD HALL TENANT FIXED"
            elif int(chrg_rentschemeid) == 3 or int(chrg_rentschemeid) == 4:
                chrg_description = "ADVERTISING FUND - FOOD HALL TENANT PERCENTAGE"

        if chrg_description == "NEW SECURITY POSTING (3 HOURS)":
            chrg_description = "SECURITY POSTING (3 HOURS)"

        if chrg_description == "BASIC/BASE RENT" or chrg_description == "PERCENTAGE RENT":
            if int(chrg_rentschemeid) == 1 or int(chrg_rentschemeid) == 2:
                chrg_description = "BASIC/BASE RENT FIXED"
            elif int(chrg_rentschemeid) == 3 or int(chrg_rentschemeid) == 4:
                chrg_description = "PERCENTAGE RENT"

        if chrg_description == "ADVANCE RENT":
            if int(chrg_rentschemeid) == 1 or int(chrg_rentschemeid) == 2:
                chrg_description = "ADVANCE RENT FIXED"
            elif int(chrg_rentschemeid) == 3 or int(chrg_rentschemeid) == 4:
                chrg_description = "ADVANCE RENT PERCENTAGE"

        if chrg_description == "MINIMUM RENT":
            chrg_description = "MINIMUM RENT PERCENTAGE"

        if chrg_description == "LOEC":
            chrg_description = "LATE OPENING AND EARLY CLOSING"

        if (
            chrg_description == "AIR CONDITION - AFFI"
            or chrg_description == "AIR CONDITION (SSS QA)"
        ):
            chrg_description = "AIR CONDITION"

        if (
            chrg_description == "PEST CONTROL"
            or chrg_description == "PEST CONTROL (DS)"
            or chrg_description == "PEST CONTROL - BIR MALABON"
            or chrg_description == "PEST CONTROL - FIXED PER UNIT"
            or chrg_description == "PEST CONTROL (SSS QA)"
            or chrg_description == "PEST CONTROL (FSM)"
        ):
            if chrg_buss_category is not None:
                try:
                    if isFood[str(chrg_buss_category).strip()]:
                        chrg_description = "PEST CONTROL - FOOD TENANT"
                    else:
                        chrg_description = "PEST CONTROL - NON-FOOD TENANT"
                except Exception:
                    chrg_description = "PEST CONTROL - NON-FOOD TENANT"
            else:
                chrg_description = "PEST CONTROL - NON-FOOD TENANT"


        rental_charge_pattern = r"(?i)RENTAL RATE\s*\((.*?)\)"
        if re.search(rental_charge_pattern, chrg_description.upper()):
            chrg_description = "BASIC/BASE RENT EXHIBIT"

        elec_charge_pattern = r"(?i)ELECTRICITY\s*\((.*?)\)"
        if re.search(rental_charge_pattern, chrg_description.upper()):
            chrg_description = "ELECTRICITY"
        

        if chrg_sn == "SOAEXTFILE":
            tms_charge = next(
                (
                    row
                    for row in t_m_s_charges_moa
                    if int(row["mall_id"]) == int(chrg_mallid)
                    and str(row["description"]).strip().upper()
                    == str(chrg_description).strip().upper()
                ),
                None,
            )
        else:
            tms_charge = next(
                (
                    row
                    for row in t_m_s_charges
                    if int(row["mall_id"]) == int(chrg_mallid)
                    and int(row["rental_scheme_id"]) == int(chrg_rentschemeid)
                    and str(row["description"]).strip().upper()
                    == str(chrg_description).strip().upper()
                ),
                None,
            )

        # t_m_s_service_invoice_id, ewt_code, charge_id, charge_type, charge_code, non_vatable, priority_order, id
        if tms_charge is not None or chrg_description == "TRANSFERRED PAYMENT" or chrg_description == "APPLICATION OF ADVANCE RENT":
            if chrg_description == "TRANSFERRED PAYMENT":
                payments2.append(
                    (
                        up_t_m_s_service_invoice_id,
                        chrg_award_notice_no,
                        chrg_posting_date,
                        "TRANSFERED PAYMENT" + str(up_t_m_s_service_invoice_id),
                        "TRANSFERED PAYMENT",
                        chrg_amount,
                        chrg_sn,
                        1,
                        1,
                        '1990-01-01 12:00:00',
                        '1990-01-01 12:00:00',
                    )
                )
            elif chrg_description == "APPLICATION OF ADVANCE RENT":
                payments2.append(
                    (
                        up_t_m_s_service_invoice_id,
                        chrg_award_notice_no,
                        chrg_posting_date,
                        "APPLICATION OF ADVANCE RENT" + str(up_t_m_s_service_invoice_id),
                        "APPLICATION OF ADVANCE RENT",
                        chrg_amount,
                        chrg_sn,
                        1,
                        1,
                        '1990-01-01 12:00:00',
                        '1990-01-01 12:00:00',
                    )
                )
            else:
                update2.append(
                    (
                        up_t_m_s_service_invoice_id,
                        tms_charge["ewt_code"],
                        tms_charge["id"],
                        tms_charge["charge_type"],
                        tms_charge["code"],
                        tms_charge["non_vatable"],
                        tms_charge["priority_order"],
                        chrg_id,
                    )
                )

        else:
            ignored2.append(chrg_description)


    counts2 = Counter(ignored2)
    print("ACC Unique values:", list(counts2.keys()))
    print("ACC Total items:", len(ignored2))
    print("\nACC Counts:")

    for value, count in counts2.most_common():
        print(f"{value}: {count}")

    sql2 = """
        UPDATE t_m_s_service_invoice_charges
        SET 
            t_m_s_service_invoice_id = %s,
            ewt_code = %s,
            charge_id = %s,
            charge_type = %s,
            charge_code = %s,
            non_vatable = %s,
            priority_order = %s
        WHERE id = %s
    """
    # note: order must match query (value first, then id)

    cursor.executemany(sql2, update2)
    conn.commit()

    ## SI Payments
    cursor.execute(
        """
        SELECT 
            t_m_s_service_invoice_payments.id,
            t_m_s_service_invoice_payments.award_notice_no,
            t_m_s_service_invoice_payments.hash
        FROM
            t_m_s_service_invoice_payments
        """
    )

    t_m_s_service_invoice_payments = cursor.fetchall()

    update3 = []
    ignore3 = []


    for pmt in t_m_s_service_invoice_payments:
        pyt_id = pmt["id"]
        pyt_sn = str(pmt["hash"]).strip()
        pyt_award_notice_no = str(pmt["award_notice_no"]).strip()

        up_t_m_s_service_invoice_id = ""
        pyt_mallid = 1
        if "QAV" in str(pyt_award_notice_no).upper():
            pyt_mallid = 2
            up_t_m_s_service_invoice_id = lookup_qv.get(pyt_sn)
            if (
                up_t_m_s_service_invoice_id is not None
                and up_t_m_s_service_invoice_id != ""
            ):
                up_t_m_s_service_invoice_id = int(up_t_m_s_service_invoice_id)
        elif "MLB" in str(pyt_award_notice_no).upper():
            pyt_mallid = 1
            up_t_m_s_service_invoice_id = lookup_mb.get(pyt_sn)
            if (
                up_t_m_s_service_invoice_id is not None
                and up_t_m_s_service_invoice_id != ""
            ):
                up_t_m_s_service_invoice_id = int(up_t_m_s_service_invoice_id) + 132088 + 17422

        if up_t_m_s_service_invoice_id != "":
            update3.append((up_t_m_s_service_invoice_id, pyt_id))

        else:
            ignore3.append(pyt_id)

    print(len(ignore3))

    sql3 = """
        UPDATE t_m_s_service_invoice_payments
        SET 
            t_m_s_service_invoice_id = %s
        WHERE id = %s
    """
    # note: order must match query (value first, then id)


    cursor.executemany(sql3, update3)
    conn.commit()

    sql2a = """
    INSERT INTO t_m_s_service_invoice_payments
        (
            t_m_s_service_invoice_id,
            award_notice_no,
            posting_date,
            reference_no,
            payment_description,
            amount,
            hash,
            updated_by,
            created_by,
            created_at,
            updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    # note: order must match query (value first, then id)


    cursor.executemany(sql2a, payments2)
    conn.commit()

################################## TREASURY POST MIGRATION SCRIPT ##################################
if run_treasury:
    cursor.execute(
        """
        SELECT 
            t_m_s_treasuries.id,
            t_m_s_treasuries.mall_id,
            t_m_s_treasuries.remarks
        FROM
            t_m_s_treasuries
        """
    )

    t_m_s_treasury = cursor.fetchall()
    update4 = []
    ignore4 = []

    for orar in t_m_s_treasury:
        orar_id = orar["id"]
        orar_sn = str(orar["remarks"]).strip()
        orar_mall_id = str(orar["mall_id"]).strip()

        up_t_m_s_service_invoice_id = ""
        if int(orar_mall_id) == 2:
            up_t_m_s_service_invoice_id = lookup_qv.get(orar_sn)
            if (
                up_t_m_s_service_invoice_id is not None
                and up_t_m_s_service_invoice_id != ""
            ):
                up_t_m_s_service_invoice_id = int(up_t_m_s_service_invoice_id)
        elif int(orar_mall_id) == 1:
            up_t_m_s_service_invoice_id = lookup_mb.get(orar_sn)
            if (
                up_t_m_s_service_invoice_id is not None
                and up_t_m_s_service_invoice_id != ""
            ):
                up_t_m_s_service_invoice_id = int(up_t_m_s_service_invoice_id) + 132088 + 17422

        if up_t_m_s_service_invoice_id != "":
            update4.append((up_t_m_s_service_invoice_id, orar_id))

        else:
            ignore4.append(orar_id)

    print(len(ignore4))

    sql4 = """
        UPDATE t_m_s_treasuries
        SET 
            service_invoice_id = %s
        WHERE id = %s AND service_invoice_id = 0
    """
    # note: order must match query (value first, then id)


    cursor.executemany(sql4, update4)
    conn.commit()







### RUN in MySQL workbench
# SET SQL_SAFE_UPDATES = 0;
# UPDATE t_m_s_service_invoices AS si
# LEFT JOIN e_b_m_events_moa_information AS moa ON moa.id = si.tin
# LEFT JOIN e_b_m_locations AS loc ON loc.id = moa.location
# SET 
# 	si.award_notice_no = moa.moa_id, 
# 	si.loc_code = COALESCE(loc.location_code, ""),
# 	si.`status` = CASE
# 		WHEN moa.moa_id IS NULL THEN 0
# 		ELSE 1
# 	END
# WHERE si.customer_type = 'event' 
# AND si.tin IS NOT NULL;
# SET SQL_SAFE_UPDATES = 1;



### FAILED
# try:
#     cursor.execute("SET SQL_SAFE_UPDATES = 0;")
#     sql5 = """
#         UPDATE t_m_s_service_invoices AS si
#         LEFT JOIN e_b_m_events_moa_information AS moa ON moa.id = si.tin
#         LEFT JOIN e_b_m_locations AS loc ON loc.id = moa.location
#         SET 
#             si.award_notice_no = moa.moa_id, 
#             si.loc_code = COALESCE(loc.location_code, ""),
#             si.`status` = CASE
#                 WHEN moa.moa_id IS NULL THEN 0
#                 ELSE 1
#             END
#         WHERE si.customer_type = 'event' 
#         AND si.tin IS NOT NULL;
#     """
#     cursor.execute(sql5)
#     cursor.execute("SET SQL_SAFE_UPDATES = 1;")
#     conn.commit()
#     print("Update successful")
# except Exception as e:
#     conn.rollback()
#     print("Error:", e)