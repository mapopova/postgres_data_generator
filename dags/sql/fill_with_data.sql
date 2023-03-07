DROP TABLE IF EXISTS sample;
CALL fill_table_with_data(
	id_tab_name=>'client_account',
	tab_target=>'sample', column_types=>array['text', 'int', 'text(10)']);
