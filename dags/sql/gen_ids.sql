DROP TABLE IF EXISTS order_id;
CALL generate_table_id_random(
	tab_name=>'order_id', col_name=>'order_id',
	min_num=>10000, max_num=>50000, amount=>10000);
	
DROP TABLE IF EXISTS customer_id;
CALL generate_table_id_inc(
	tab_name=>'customer_id', col_name=>'customer_id',
	min_num=>'1', max_num=>2000);
	
DROP TABLE IF EXISTS payment_id;
CALL generate_table_uuid(
	tab_name=>'payment_id', col_name=>'check_number',
	amount=>1800);
	
	
