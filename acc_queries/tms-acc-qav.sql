SELECT
    t_m_s_service_invoices.id,
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
    "" AS vatable_sales,
    "" AS vat_exempt_sales,
    "" AS vat_zero_rated_sales,
    "" AS vat_amount,
    "" AS subtotal,
    CONCAT("migrated", MD5(t_m_s_service_invoices.id)) AS `hash`,
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
    t_m_s_service_invoice_charges.id,
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
UNION
ALL
SELECT
    t_m_s_service_invoice_charges.id + 614916,
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
UNION
ALL
SELECT
    t_m_s_service_invoice_charges.id + 614916 + 49544,
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
    AND t_m_s_service_invoice_charges.sn IS NOT NULL;

SELECT
    t_m_s_debit_credit_memos.id AS id,
    CONCAT(
        t_m_s_debit_credit_memos.bsn,
        "-DMCM-",
        t_m_s_debit_credit_memos.id
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
    t_m_s_debit_credit_memos.id + 49544 AS id,
    CONCAT(
        t_m_s_debit_credit_memos.bsn,
        "-DMCM-",
        t_m_s_debit_credit_memos.id + 49544
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
    single_soa1 AS sn_map_qav
WHERE
    sn IS NOT NULL
    AND NOT sn = ""
    AND NOT sn = 0;

SELECT
    t_m_s_service_invoice_payments.id AS id,
    0 AS t_m_s_service_invoice_id,
    t_m_s_service_invoice_payments.tc AS award_notice_no,
    t_m_s_service_invoice_payments.edt AS posting_date,
    t_m_s_service_invoice_payments.orn AS reference_no,
    "PAYMENT" AS payment_description,
    COALESCE(
        t_m_s_service_invoice_payments.camt,
        t_m_s_service_invoice_payments.amt,
        0
    ) AS amount,
    t_m_s_service_invoice_payments.sn AS `hash`,
    1 AS updated_by,
    1 AS created_by,
    '1990-01-01 12:00:00' AS updated_at,
    '1990-01-01 12:00:00' AS created_at
FROM
    or2 AS t_m_s_service_invoice_payments
WHERE
    t_m_s_service_invoice_payments.deldate IS NULL
    AND t_m_s_service_invoice_payments.candate IS NULL
    AND t_m_s_service_invoice_payments.edt IS NOT NULL;