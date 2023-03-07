DROP TABLE IF EXISTS payment;
CALL fill_table_with_data(
	id_tab_name=>'payment_customer',
	tab_target=>'payment',
	column_types=>array['date(''2010-01-01'', current_date)', 'int'],
	column_names=>array['payment_date', 'amount']);

DROP TABLE IF EXISTS customer_data;
CALL fill_table_with_data(
	id_tab_name=>'customer_chain',
	tab_target=>'customer_data',
	column_types=>array['text(10)', 'int'],
	column_names=>array['name', 'phone']);

DROP TABLE IF EXISTS customer;
CALL fill_table_from_catalog(
	tab_name=>'customer_data',
	tab_target=>'customer',
	catalog_name=>'states',
	column_names=>array['state_abbreviation', 'state_name']);

