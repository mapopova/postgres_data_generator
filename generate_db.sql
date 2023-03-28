DROP TABLE IF EXISTS account_gid;
CALL generate_table_id_inc(
	tab_name=>'account_gid', col_name=>'account_gid', 
	min_num=>1, max_num=>2000);


DROP TABLE IF EXISTS account_summary_gid;
CALL generate_table_id_inc(
	tab_name=>'account_summary_gid', col_name=>'account_summary_gid', 
	min_num=>2001, max_num=>2200);

	
DROP TABLE IF EXISTS client_gid;
CALL generate_table_id_random(
	tab_name=>'client_gid', col_name=>'client_gid', 
	min_num=>10000, max_num=>20000, amount=>10000);
	
	
DROP TABLE IF EXISTS contract_gid;
CALL generate_table_id_random(
	tab_name=>'contract_gid', col_name=>'contract_gid', 
	min_num=>50000, max_num=>60000, amount=>5000);

--SELECT count(DISTINCT(account_gid)) FROM account_gid;
	

DROP TABLE IF EXISTS dm_account_ids;
CALL generate_one_to_one_or_many_pairs(
	tab_one=>'account_summary_gid', col_one=>'account_summary_gid', 
	tab_two=>'account_gid', col_two=>'account_gid',
	tab_target=>'dm_account_ids', amount=>1000);


--SELECT * FROM dm_account_ids;

DROP TABLE IF EXISTS dm_account_done;
CREATE TABLE dm_account_done AS
(SELECT account_gid, account_summary_gid
FROM account_gid
LEFT JOIN dm_account_ids USING (account_gid)

UNION ALL

SELECT account_summary_gid, NULL
FROM account_summary_gid);

--SELECT * FROM dm_account_done;

DROP TABLE dm_account_ids;
ALTER TABLE dm_account_done RENAME TO dm_account_ids;


--SELECT * from dm_account_ids;

CREATE TABLE account_gid_all AS 
(SELECT account_gid
FROM account_gid
UNION ALL
SELECT account_summary_gid 
FROM account_summary_gid);

DROP TABLE account_summary_gid;
DROP TABLE account_gid;
ALTER TABLE account_gid_all RENAME TO account_gid;



DROP TABLE IF EXISTS dp_account_properties_1;
CALL fill_table_from_catalog(
	tab_name=>'account_gid',
	tab_target=>'dp_account_properties_1',
	catalog_name=>'dp_account_status_type',
	column_names=>array['account_status_id']);

DROP TABLE IF EXISTS dp_account_properties;
CALL fill_table_with_data(
	id_tab_name=>'dp_account_properties_1',
	tab_target=>'dp_account_properties',
	column_types=>array['const_string(''A'')', 'const_number(1304)'],
	column_names=>array['tech$row_status', 'property_type_id'],
	delete_source=>true);

--SELECT account_summary_gid, count(DISTINCT(account_gid)) FROM dm_account_ids 
--GROUP BY account_summary_gid 

select count(*) --p_account_properties.account_gid--account_id
  from dp_account_properties
  join dp_account_status_type on dp_account_status_type.account_status_id = dp_account_properties.account_status_id --value_reference
                                                             and dp_account_status_type.ACCOUNT_STATUS_CODE = 'К ОТКРЫТИЮ'
 where dp_account_properties.property_type_id = 1304
   and dp_account_properties.tech$row_status = 'A'
   
--  SELECT * FROM dp_account_status_type dast 
   
SELECT count(*) FROM dp_account_status_type dast 

