DROP TABLE IF EXISTS order_customer;
CALL generate_one_to_zero_or_many_pairs(
	tab_one=>'customer_id', col_one=>'customer_id', 
	tab_two=>'order_id', col_two=>'order_id',
	tab_target=>'order_customer', amount=>10000); 
	
DROP TABLE IF EXISTS payment_customer;
CALL generate_one_to_one_pairs(
	tab_one=>'customer_id', col_one=>'customer_id', 
	tab_two=>'payment_id', col_two=>'check_number',
	tab_target=>'payment_customer', amount=>1800);
	
DROP TABLE IF EXISTS customer_chain;
CALL generate_chains(
	tab_name=>'customer_id', col_name=>'customer_id',
	tab_target=>'customer_chain', amount=>600);
