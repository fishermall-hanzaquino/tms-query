import pymysql
import pymysql.cursors
from collections import Counter

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

isFood = {
    "Affiliate" : False,	
    "Amusement/Recreational" : False,	
    "Bakeshop" : True,	
    "Bank/ATM" : False,	
    "Bar" : True,	
    "Beauty and Wellness Services" : False,	
    "Beauty and Wellness Products" : False,	
    "Beauty Salon" : False,	
    "Coffee Shop" : True,	
    "Donuts" : True,	
    "Drug Store/Convenience Store" : False,	
    "Fast Food" : True,	
    "Fashion Apparel" : False,	
    "Footwear/Shoes Store" : False,	
    "Gadgets/Cellphones" : False,	
    "Gym" : False,	
    "Office" : False,	
    "Radio Room/Telecommunications" : False,	
    "Restaurant" : True,	
    "Retail Shop/Store" : False,
    "Specialty Food/Beverages" : True,	
    "Specialty Shop/Store" : True,	
    "Services/Service Center" : False,	
    "Appliance/Furniture Store" : False,	
    "Optical Shop/Services" : False,	
    "Jewelry Shop" : False,	
    "Books/School/Office Supplies" : False,	
    "Casual Dining" : True,	
    "Storage" : False,	
    "Hardware/Homeware" : False,
    "Canteen" : True,	
    "Education/Learning Center" : False,	
    "Medical Services" : False,	
    "Textile/Tailoring" : False,	
    "Laptops, Cellphones & Gadgets" : False,	
    "Services" : False,	
    "CAKES & PASTRIES" : True,	
    "CELLSITES" : False,	
    "MEDICAL CLINIC" : False,	
    "TATTOO SHOP" : False,
    "RECREATION" : False,	
    "Food & Beverages" : True,	
    "Laptops, Desktop and Gadgets" : False,	
    "Car/Auto/Bicycle/Motorcycle Accessories" : False,	
    "Convenience Store" : False,	
    "Mobile Phones & Accessories" : False,	
    "Watches" : False,	
    "Gadget Accessories" : False,	
    "Buffet" : True,	
    "Breads/Pastries" : True,
    "Clinics & Laboratories" : False,	
    "Photography Services" : False,	
    "Regional Delicacies" : False,	
    "Other Services" : False,	
    "Logistic Services" : False,	
    "Pet Shop" : False,	
    "Massage Chairs" : False
}

update = []
ignored = []

t_m_s_a_n_other_charges = cursor.fetchall()

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
        or chr_chg_desc == "COMMON AREA MAINTENANCE CHARGES TOTAL AREA - STRAIGHT PERCENTAGE"
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

    if chr_chg_desc == "ADVERTISING FUND" or chr_chg_desc == "ADVERTISING FUND (KINETIC QA)":
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

    if chr_chg_desc ==  'AIR CONDITION - AFFI' or chr_chg_desc == 'AIR CONDITION (SSS QA)':
        chr_chg_desc = 'AIR CONDITION'

    if (chr_chg_desc == "PEST CONTROL"
        or chr_chg_desc == "PEST CONTROL (DS)"
        or chr_chg_desc == "PEST CONTROL - BIR MALABON"
        or chr_chg_desc == "PEST CONTROL - FIXED PER UNIT"
        or chr_chg_desc == "PEST CONTROL (SSS QA)"
        or chr_chg_desc == "PEST CONTROL (FSM)"
    ):
        if chr_buss_cat is not None:
            if isFood[str(chr_buss_cat).strip()]:
                chr_chg_desc =  "PEST CONTROL - FOOD TENANT"
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
