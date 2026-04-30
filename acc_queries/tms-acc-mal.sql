SELECT
    t_m_s_service_invoices.id + 132088 + 17422 AS id,
    t_m_s_service_invoices.sn AS service_invoice_no,
    CASE
        WHEN t_m_s_service_invoices.snt = 2 THEN "regular"
        WHEN t_m_s_service_invoices.snt = 1 THEN "initial"
    END AS `type`,
    t_m_s_service_invoices.tc AS award_notice_no,
    "tenant" AS customer_type,
    COALESCE(
        (
            SELECT
                amendparticularsfile.valueto
            FROM
                amendmentfile
                LEFT JOIN amendparticularsfile ON amendmentfile.amendmentcde = amendparticularsfile.amendmentcde
            WHERE
                clientofferfile.termsofleasecde = amendmentfile.termsofleasecde
                AND amendmentfile.posted = 1
                AND amendparticularsfile.amendfield = 'Store Name'
                AND amendmentfile.effectivedte < t_m_s_service_invoices.sdt1
            ORDER BY
                amendmentfile.effectivedte DESC
            LIMIT
                1
        ), clientofferfile.storenme
    ) AS establishment,
    COALESCE(
        (
            SELECT
                amendparticularsfile.valueto
            FROM
                amendmentfile
                LEFT JOIN amendparticularsfile ON amendmentfile.amendmentcde = amendparticularsfile.amendmentcde
            WHERE
                clientofferfile.termsofleasecde = amendmentfile.termsofleasecde
                AND amendmentfile.posted = 1
                AND amendparticularsfile.amendfield = 'Corporate Name'
                AND amendmentfile.effectivedte < t_m_s_service_invoices.sdt1
            ORDER BY
                amendmentfile.effectivedte DESC
            LIMIT
                1
        ), clientofferfile.clientnme
    ) AS bussiness_style,
    t_m_s_service_invoices.sdt1 AS period_from,
    t_m_s_service_invoices.sdt2 AS period_to,
    t_m_s_service_invoices.sdt2 AS statement_date,
    t_m_s_service_invoices.pdt AS payment_due_date,
    (
        SELECT
            tenantaccountfile.tenantaccttin
        FROM
            tenantaccountfile
        WHERE
            tenantaccountfile.clientacctcde = clientofferfile.clientacctcde
        LIMIT
            1
    ) AS tin,
    t_m_s_service_invoices.lc AS loc_code,
    t_m_s_service_invoices.tarea AS total_area,
    t_m_s_service_invoices.beg AS beginning_balance,
    t_m_s_service_invoices.payamt AS payment,
    t_m_s_service_invoices.chargeamt AS charges_for_month,
    t_m_s_service_invoices.beg + t_m_s_service_invoices.chargeamt + t_m_s_service_invoices.payamt AS amount_due,
    0 AS vatable_sales,
    0 AS vat_exempt_sales,
    0 AS vat_zero_rated_sales,
    t_m_s_service_invoices.vat AS vat_amount,
    (t_m_s_service_invoices.chargeamt-t_m_s_service_invoices.vat) AS subtotal,
    CONCAT(
        "migrated",
        MD5(t_m_s_service_invoices.id + 132088 + 17422)
    ) AS `hash`,
    t_m_s_service_invoices.pby AS prepared_by,
    t_m_s_service_invoices.cby AS checked_by,
    t_m_s_service_invoices.aby AS noted_by,
    CASE
        WHEN (
            t_m_s_service_invoices.posted = 1
            OR t_m_s_service_invoices.prnt = 1
        )
        AND t_m_s_service_invoices.stat = 1 THEN 1
        ELSE 0
    END AS `status`,
    t_m_s_service_invoices.pdt AS posted_at,
    1 AS updated_by,
    1 AS created_by,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    single_soa1 AS t_m_s_service_invoices
    LEFT JOIN clientofferfile ON t_m_s_service_invoices.tc = clientofferfile.termsofleasecde
    AND NOT t_m_s_service_invoices.tc = "";

SELECT
    t_m_s_service_invoice_charges.id + 614916 + 49544 + 316687 + 16289,
    t_m_s_service_invoice_charges.sn AS t_m_s_service_invoice_id,
    "TODO" AS ewt_code,
    t_m_s_service_invoice_charges.tc AS award_notice_no,
    t_m_s_service_invoice_charges.sdt2 AS posting_date,
    0 AS charge_id,
    "TODO" AS charge_type,
    "TODO" AS charge_code,
    t_m_s_service_invoice_charges.cd AS charge_description,
    t_m_s_service_invoice_charges.total AS amount,
    "TODO" AS non_vatable,
    0 AS is_dmcm,
    t_m_s_service_invoice_charges.sn AS `hash`,
    0 AS priority_order,
    1 AS updated_by,
    1 AS created_by,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    single_soa2 AS t_m_s_service_invoice_charges
WHERE
    NOT t_m_s_service_invoice_charges.sn IN ("", 0)
    AND t_m_s_service_invoice_charges.sn IS NOT NULL
    AND NOT t_m_s_service_invoice_charges.total = 0
UNION
ALL
SELECT
    t_m_s_service_invoice_charges.id + 614916 + 49544 + 316687 + 16289 + 343742,
    t_m_s_service_invoice_charges.sn AS t_m_s_service_invoice_id,
    "TODO" AS ewt_code,
    t_m_s_service_invoice_charges.tc AS award_notice_no,
    t_m_s_service_invoice_charges.sdt2 AS posting_date,
    0 AS charge_id,
    "TODO" AS charge_type,
    "TODO" AS charge_code,
    t_m_s_service_invoice_charges.chargenme AS charge_description,
    t_m_s_service_invoice_charges.amt AS amount,
    "TODO" AS non_vatable,
    1 AS is_dmcm,
    t_m_s_service_invoice_charges.sn AS `hash`,
    0 AS priority_order,
    1 AS updated_by,
    1 AS created_by,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    cm AS t_m_s_service_invoice_charges
WHERE
    NOT t_m_s_service_invoice_charges.sn IN ("", 0)
    AND t_m_s_service_invoice_charges.sn IS NOT NULL
    AND NOT t_m_s_service_invoice_charges.amt = 0
UNION
ALL
SELECT
    t_m_s_service_invoice_charges.id + 614916 + 49544 + 316687 + 16289 + 343742 + 24447,
    t_m_s_service_invoice_charges.sn AS t_m_s_service_invoice_id,
    "TODO" AS ewt_code,
    t_m_s_service_invoice_charges.tc AS award_notice_no,
    t_m_s_service_invoice_charges.sdt2 AS posting_date,
    0 AS charge_id,
    "TODO" AS charge_type,
    "TODO" AS charge_code,
    t_m_s_service_invoice_charges.chargenme AS charge_description,
    t_m_s_service_invoice_charges.amt AS amount,
    "TODO" AS non_vatable,
    1 AS is_dmcm,
    t_m_s_service_invoice_charges.sn AS `hash`,
    0 AS priority_order,
    1 AS updated_by,
    1 AS created_by,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    dm AS t_m_s_service_invoice_charges
WHERE
    NOT t_m_s_service_invoice_charges.sn IN ("", 0)
    AND t_m_s_service_invoice_charges.sn IS NOT NULL
    AND NOT t_m_s_service_invoice_charges.amt = 0;

SELECT
    t_m_s_debit_credit_memos.id + 49544 + 316687 AS id,
    CONCAT(
        t_m_s_debit_credit_memos.bsn,
        "-DMCM-",
        t_m_s_debit_credit_memos.id + 49544 + 316687
    ) AS dmcm_id,
    CASE
        WHEN t_m_s_debit_credit_memos.snt = 2 THEN "regular"
        WHEN t_m_s_debit_credit_memos.snt = 1 THEN "initial"
    END AS `type`,
    "credit" AS `class`,
    t_m_s_debit_credit_memos.tc AS anno,
    0 AS t_m_s_contract_id,
    0 AS charge_id,
    "an" AS `document`,
    "TODO" AS charge_type,
    t_m_s_debit_credit_memos.amt AS amount,
    t_m_s_debit_credit_memos.chargenme AS remarks,
    1 AS created_by,
    t_m_s_debit_credit_memos.posted AS posted_status,
    10 AS `status`,
    1 AS posted_by,
    t_m_s_debit_credit_memos.date AS posted_at,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    cm AS t_m_s_debit_credit_memos
WHERE
    NOT t_m_s_debit_credit_memos.sn IN ("", 0)
    AND t_m_s_debit_credit_memos.sn IS NOT NULL
UNION
ALL
SELECT
    t_m_s_debit_credit_memos.id + 49544 + 316687 + 24447 AS id,
    CONCAT(
        t_m_s_debit_credit_memos.bsn,
        "-DMCM-",
        t_m_s_debit_credit_memos.id + 49544 + 316687 + 24447
    ) AS dmcm_id,
    CASE
        WHEN t_m_s_debit_credit_memos.snt = 2 THEN "regular"
        WHEN t_m_s_debit_credit_memos.snt = 1 THEN "initial"
    END AS `type`,
    "debit" AS `class`,
    t_m_s_debit_credit_memos.tc AS anno,
    0 AS t_m_s_contract_id,
    0 AS charge_id,
    "an" AS `document`,
    "TODO" AS charge_type,
    t_m_s_debit_credit_memos.amt AS amount,
    t_m_s_debit_credit_memos.chargenme AS remarks,
    1 AS created_by,
    t_m_s_debit_credit_memos.posted AS posted_status,
    10 AS `status`,
    1 AS posted_by,
    t_m_s_debit_credit_memos.date AS posted_at,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    dm AS t_m_s_debit_credit_memos
WHERE
    NOT t_m_s_debit_credit_memos.sn IN ("", 0)
    AND t_m_s_debit_credit_memos.sn IS NOT NULL;

SELECT
    id,
    sn
FROM
    single_soa1 AS sn_map_mlb
WHERE
    sn IS NOT NULL
    AND NOT sn = ""
    AND NOT sn = 0;

SELECT
    t_m_s_service_invoice_payments.id + 30132 + 48863 AS id,
    0 AS t_m_s_service_invoice_id,
    t_m_s_service_invoice_payments.tc AS award_notice_no,
    t_m_s_service_invoice_payments.odt AS posting_date,
    t_m_s_service_invoice_payments.orn AS reference_no,
    "PAYMENT" AS payment_description,
    -(
        COALESCE(
            t_m_s_service_invoice_payments.amt,
            0
        ) + COALESCE(
            t_m_s_service_invoice_payments.ewt,
            0
        )
    ) AS amount,
    t_m_s_service_invoice_payments.sn AS `hash`,
    1 AS updated_by,
    1 AS created_by,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    or1 AS t_m_s_service_invoice_payments
WHERE
    t_m_s_service_invoice_payments.del_dt IS NULL
    AND t_m_s_service_invoice_payments.cancel_dt IS NULL
    AND t_m_s_service_invoice_payments.stat = 1
UNION
ALL
SELECT
    t_m_s_service_invoice_payments.id + 30132 + 48863 + 16035 AS id,
    0 AS t_m_s_service_invoice_id,
    t_m_s_service_invoice_payments.tc AS award_notice_no,
    t_m_s_service_invoice_payments.ewtdt AS posting_date,
    CONCAT("EWT",t_m_s_service_invoice_payments.id + 30132 + 48863 + 16035) AS reference_no,
    CONCAT("EWT (", t_m_s_service_invoice_payments.ewtdt1, "-", t_m_s_service_invoice_payments.ewtdt2, ")") AS payment_description,
    -COALESCE(
        t_m_s_service_invoice_payments.amt,
        0
    )AS amount,
    (SELECT 
            sn
        FROM
            single_soa1
        WHERE
            (single_soa1.posted = 1
                OR single_soa1.prnt = 1)
                AND single_soa1.tc = t_m_s_service_invoice_payments.tc
                AND single_soa1.sdt1 = t_m_s_service_invoice_payments.sdt1
                AND single_soa1.sdt2 = t_m_s_service_invoice_payments.sdt2
        LIMIT 1) AS hash,
    1 AS updated_by,
    1 AS created_by,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    single_ewt AS t_m_s_service_invoice_payments
WHERE
    t_m_s_service_invoice_payments.deldate IS NULL
    AND t_m_s_service_invoice_payments.candate IS NULL
    AND t_m_s_service_invoice_payments.stat = 1;




SELECT 
    t_m_s_treasuries.id + 30132 + 48863 AS id,
    CONCAT('TRSY-CR0-', t_m_s_treasuries.id) AS payment_id,
    0 AS service_invoice_id,
    t_m_s_treasuries.odt AS transaction_date,
    t_m_s_treasuries.odt AS receipt_date,
    '' AS payment_type,
    t_m_s_treasuries.orn AS reference_no,
    t_m_s_treasuries.chk AS check_no,
    '' AS check_date,
    '' AS check_bank,
    t_m_s_treasuries.fop AS mode_of_payment,
    0 AS bank_account_no,
    COALESCE(t_m_s_treasuries.amt, 0) AS amount_paid,
    CASE
        WHEN t_m_s_treasuries.for_or = 1 THEN 'AR'
        ELSE 'CR'
    END AS receipt_type,
    0 AS pdc,
    COALESCE(t_m_s_treasuries.ewt, 0) AS ewt,
    '' AS total,
    t_m_s_treasuries.edt AS ar_cr_date,
    t_m_s_treasuries.tn AS payor,
    '' AS tin_no,
    '' AS location,
    t_m_s_treasuries.printby AS checked_by,
    t_m_s_treasuries.noted AS noted_by,
    (SELECT 
            sn
        FROM
            or2
        WHERE
            or2.orn = t_m_s_treasuries.orn
        LIMIT 1) AS remarks,
    30 AS `status`,
    10 AS print_status,
    1 AS mall_id,
    '' AS canceled_by,
    '' AS cancel_reason,
    '' AS canceled_at,
    1 AS created_by,
    1 AS updated_by,
    '1990-01-01 12:00:00' AS created_at,
    '1990-01-01 12:00:00' AS updated_at
FROM
    or1 AS t_m_s_treasuries
WHERE
    t_m_s_treasuries.posted = 1
        AND t_m_s_treasuries.del_dt IS NULL
        AND t_m_s_treasuries.cancel_dt IS NULL 
UNION ALL SELECT 
    t_m_s_treasuries.id + 30132 + 48863 + 16035 AS id,
    CONCAT('TRSY-CR0-', t_m_s_treasuries.id) AS payment_id,
    (SELECT 
            id
        FROM
            single_soa1
        WHERE
            t_m_s_treasuries.tc = single_soa1.tc
                AND (single_soa1.posted = 1
                OR single_soa1.prnt = 1)
        LIMIT 1) AS service_invoice_id,
    t_m_s_treasuries.sdt2 AS transaction_date,
    t_m_s_treasuries.sdt2 AS receipt_date,
    '' AS payment_type,
    CONCAT('EWT', t_m_s_treasuries.id + 30132) AS reference_no,
    '' AS check_no,
    '' AS check_date,
    '' AS check_bank,
    1 AS mode_of_payment,
    0 AS bank_account_no,
    0 AS amount_paid,
    'CR' AS receipt_type,
    0 AS pdc,
    COALESCE(t_m_s_treasuries.amt, 0) AS ewt,
    '' AS total,
    t_m_s_treasuries.sdt2 AS ar_cr_date,
    (SELECT 
            clientnme
        FROM
            clientofferfile
        WHERE
            clientofferfile.termsofleasecde = t_m_s_treasuries.tc
        LIMIT 1) AS payor,
    (SELECT 
            tin
        FROM
            clientofferfile
        WHERE
            clientofferfile.termsofleasecde = t_m_s_treasuries.tc
        LIMIT 1) AS tin,
    (SELECT 
            loccde
        FROM
            clientofferfile
        WHERE
            clientofferfile.termsofleasecde = t_m_s_treasuries.tc
        LIMIT 1) AS location,
    t_m_s_treasuries.user AS checked_by,
    'ANGIE DELOS SANTOS' AS noted_by,
    'MIGRATED EWT' AS remarks,
    30 AS `status`,
    10 AS print_status,
    1 AS mall_id,
    '' AS canceled_by,
    '' AS cancel_reason,
    '' AS canceled_at,
    1 AS created_by,
    1 AS updated_by,
    '1990-01-01 12:00:00' AS created_at,
    '1990-01-01 12:00:00' AS updated_at
FROM
    single_ewt AS t_m_s_treasuries
WHERE
    t_m_s_treasuries.stat = 1
        AND t_m_s_treasuries.deldate IS NULL
        AND t_m_s_treasuries.candate IS NULL;

