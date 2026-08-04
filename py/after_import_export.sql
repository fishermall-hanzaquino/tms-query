SET SQL_SAFE_UPDATES = 0;
UPDATE t_m_s_service_invoices AS si
LEFT JOIN e_b_m_events_moa_information AS moa ON moa.id = si.tin
LEFT JOIN e_b_m_locations AS loc ON loc.id = moa.location
SET 
	si.award_notice_no = moa.moa_id, 
	si.loc_code = COALESCE(loc.location_code, ""),
	si.`status` = CASE
		WHEN moa.moa_id IS NULL THEN 0
		ELSE 1
	END
WHERE si.customer_type = 'event' 
AND si.tin IS NOT NULL;
SET SQL_SAFE_UPDATES = 1;
 
DROP INDEX idx_service_main ON t_m_s_service_invoices;
DROP INDEX idx_invoice_charges_main ON t_m_s_service_invoice_charges;
DROP INDEX idx_rental_schemes ON t_m_s_rental_schemes;
DROP INDEX idx_rental_charges ON t_m_s_a_n_rental_charges;

CREATE INDEX idx_service_main 
ON t_m_s_service_invoices (customer_type, status, award_notice_no, payment_due_date, statement_date, created_at);

CREATE INDEX idx_invoice_charges_main
ON t_m_s_service_invoice_charges 
(t_m_s_service_invoice_id, award_notice_no, charge_id, created_at);

CREATE INDEX idx_rental_schemes
ON t_m_s_rental_schemes 
(id);

CREATE INDEX idx_rental_charges
ON t_m_s_a_n_rental_charges 
(id);

SHOW INDEX FROM t_m_s_service_invoices;
SHOW INDEX FROM t_m_s_service_invoice_charges;
SHOW INDEX FROM t_m_s_rental_schemes;
SHOW INDEX FROM t_m_s_a_n_rental_charges;