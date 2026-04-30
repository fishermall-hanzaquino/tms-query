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

