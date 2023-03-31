----------------------------------------------------------------------
-- айдишники

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


DROP TABLE IF EXISTS fin_instrument;
CALL generate_table_id_random(
	tab_name=>'fin_instrument', col_name=>'fin_instrument_gid', 
	min_num=>1, max_num=>3000, amount=>3000);

----------------------------------------------------------------------
-- сводные счета

DROP TABLE IF EXISTS dm_account_ids;
CALL generate_one_to_one_or_many_pairs(
	tab_one=>'account_summary_gid', col_one=>'account_summary_gid', 
	tab_two=>'account_gid', col_two=>'account_gid',
	tab_target=>'dm_account_ids', amount=>1000);

DROP TABLE IF EXISTS dm_account_done;
CREATE TABLE dm_account_done AS
(SELECT account_gid, account_summary_gid
FROM account_gid
LEFT JOIN dm_account_ids USING (account_gid)

UNION ALL

SELECT account_summary_gid, NULL
FROM account_summary_gid);

DROP TABLE dm_account_ids;
ALTER TABLE dm_account_done RENAME TO dm_account_ids;

CREATE TABLE account_gid_all AS 
(SELECT account_gid
FROM account_gid
UNION ALL
SELECT account_summary_gid 
FROM account_summary_gid);

DROP TABLE account_summary_gid;
DROP TABLE account_gid;
ALTER TABLE account_gid_all RENAME TO account_gid;

----------------------------------------------------------------------
-- dp_account_properties

DROP TABLE IF EXISTS dp_account_properties_1;
CALL fill_table_from_dict(
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

ALTER TABLE dp_account_properties RENAME COLUMN account_gid TO account_id;
ALTER TABLE dp_account_properties RENAME COLUMN account_status_id TO value_reference;

--SELECT account_summary_gid, count(DISTINCT(account_gid)) FROM dm_account_ids 
--GROUP BY account_summary_gid 

--select count(*) --p_account_properties.account_gid--account_id
--  from dp_account_properties
--  join dp_account_status_type on dp_account_status_type.account_status_id = dp_account_properties.account_status_id --value_reference
--                                                             and dp_account_status_type.ACCOUNT_STATUS_CODE = 'К ОТКРЫТИЮ'
-- where dp_account_properties.property_type_id = 1304
--   and dp_account_properties.tech$row_status = 'A';
--   
----  SELECT * FROM dp_account_status_type dast 
--   
--SELECT count(*) FROM dp_account_status_type dast 

--SELECT count(*) FROM dm_account_ids dai 

----------------------------------------------------------------------
-- dm_account

DROP TABLE IF EXISTS dm_account;
CALL fill_table_with_data(
	id_tab_name=>'dm_account_ids',
	tab_target=>'dm_account',
	column_types=>array['int(1,1000)',
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')',
						'text(4)',
						'int(1,4000)',
						'const_number(0)',
						'text(25)',
						'const_string(''A'')', 
						'int(1,200)'],
	column_names=>array['branch_dept_gid',
						'valid_begin_date',
						'valid_end_date',
						'tech$source',
						'beneficiary_client_gid',
						'is_technical_flg',
						'account_number',
						'tech$row_status', 
						'tech$upd_audit_id'],
	delete_source=>true);

----------------------------------------------------------------------
-- contract

DROP TABLE IF EXISTS contract_1;
CALL fill_table_from_dict(
	tab_name=>'contract_gid',
	tab_target=>'contract_1',
	catalog_name=>'dict_contract_type_cd',
	column_names=>array['crt_contract_type_cd']);

DROP TABLE IF EXISTS contract;
CALL fill_table_with_data(
	id_tab_name=>'contract_1',
	tab_target=>'contract',
	column_types=>array['date(''2399-12-31'',''2399-12-31'')',
						'const_string(''A'')', 
						'date(''2022-12-31'',''2400-12-31'')'],
	column_names=>array['crt$end_date',
						'crt$row_status',
						'crt_actual_end_date'],
	delete_source=>true);

ALTER TABLE contract RENAME COLUMN contract_gid TO crt_gid;

----------------------------------------------------------------------
-- dm_fin_instr_account

DROP TABLE IF EXISTS dm_fin_instr_account_ids;
CALL generate_many_to_many_pairs(
	tab_one=>'account_gid', col_one=>'account_gid', 
	tab_two=>'fin_instrument', col_two=>'fin_instrument_gid',
	tab_target=>'dm_fin_instr_account_ids', amount=>1000);

DROP TABLE IF EXISTS  dm_fin_instr_account;
CALL fill_table_with_data(
	id_tab_name=>'dm_fin_instr_account_ids',
	tab_target=>'dm_fin_instr_account',
	column_types=>array['const_string(''A'')', 
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')'],
	column_names=>array['tech$row_status',
						'valid_begin_date',
						'valid_end_date'],
	delete_source=>true);

----------------------------------------------------------------------
-- dm_contract_account

DROP TABLE IF EXISTS dm_contract_account_ids;
CALL generate_many_to_many_pairs(
	tab_one=>'account_gid', col_one=>'account_gid', 
	tab_two=>'contract_gid', col_two=>'contract_gid',
	tab_target=>'dm_contract_account_ids', amount=>2000);

DROP TABLE IF EXISTS dm_contract_account;
CALL fill_table_with_data(
	id_tab_name=>'dm_contract_account_ids',
	tab_target=>'dm_contract_account',
	column_types=>array['const_string(''A'')', 
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')'],
	column_names=>array['tech$row_status',
						'valid_begin_date',
						'valid_end_date'],
	delete_source=>true);

----------------------------------------------------------------------
-- dm_contract_quality_category

DROP TABLE IF EXISTS dm_contract_quality_category_1;
CALL fill_table_from_dict(
	tab_name=>'contract_gid',
	tab_target=>'dm_contract_quality_category_1',
	catalog_name=>'dm_quality_category',
	column_names=>array['quality_category_type_id', 'quality_category_id']);

DROP TABLE IF EXISTS dm_contract_quality_category;
CALL fill_table_with_data(
	id_tab_name=>'dm_contract_quality_category_1',
	tab_target=>'dm_contract_quality_category',
	column_types=>array['const_string(''A'')', 
						'NULL',
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')',
						'real(0,100)'],
	column_names=>array['tech$row_status',
						'tech$source_pk',
						'valid_begin_date',
						'valid_end_date',
						'reserve_prc'],
	delete_source=>true);

----------------------------------------------------------------------
-- dm_client_quality_category

DROP TABLE IF EXISTS dm_client_quality_category_1;
CALL fill_table_from_dict(
	tab_name=>'client_gid',
	tab_target=>'dm_client_quality_category_1',
	catalog_name=>'dm_quality_category',
	column_names=>array['quality_category_type_id', 'quality_category_id']);

DROP TABLE IF EXISTS dm_client_quality_category;
CALL fill_table_with_data(
	id_tab_name=>'dm_client_quality_category_1',
	tab_target=>'dm_client_quality_category',
	column_types=>array['const_string(''A'')', 
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')',
						'real(0,100)'],
	column_names=>array['tech$row_status',
						'valid_begin_date',
						'valid_end_date',
						'reserve_prc'],
	delete_source=>true);

----------------------------------------------------------------------
-- dm_quality_category_map

DROP TABLE IF EXISTS quality_category_id;
CREATE TABLE quality_category_id AS 
SELECT quality_category_id FROM dm_quality_category;

DROP TABLE IF EXISTS dm_quality_category_map_1;
CALL fill_table_from_dict(
	tab_name=>'quality_category_id',
	tab_target=>'dm_quality_category_map_1',
	catalog_name=>'quality_category_cd',
	column_names=>array['quality_category_gid']);

DROP TABLE quality_category_id;

DROP TABLE IF EXISTS dm_quality_category_map;
CALL fill_table_with_data(
	id_tab_name=>'dm_quality_category_map_1',
	tab_target=>'dm_quality_category_map',
	column_types=>array['const_string(''A'')'],
	column_names=>array['tech$row_status'],
	delete_source=>true);

----------------------------------------------------------------------
-- dp_contract

DROP TABLE IF EXISTS dp_contract_ids;
CALL generate_one_to_zero_or_many_pairs(
	tab_one=>'client_gid', col_one=>'client_gid', 
	tab_two=>'contract_gid', col_two=>'contract_gid',
	tab_target=>'dp_contract_ids', amount=>2000);

DROP TABLE IF EXISTS dp_contract_1;
CALL fill_table_from_dict(
	tab_name=>'dp_contract_ids',
	tab_target=>'dp_contract_1',
	catalog_name=>'dp_contract_type',
	column_names=>array['contract_type_id']);

DROP TABLE IF EXISTS dp_contract;
CALL fill_table_with_data(
	id_tab_name=>'dp_contract_1',
	tab_target=>'dp_contract',
	column_types=>array['const_string(''A'')', 
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')'],
	column_names=>array['tech$row_status',
						'valid_begin_date',
						'valid_end_date'],
	delete_source=>true);

ALTER TABLE dp_contract RENAME COLUMN client_gid TO client_id;
ALTER TABLE dp_contract RENAME COLUMN contract_gid TO contract_id;

----------------------------------------------------------------------
-- reserve_item

DROP TABLE IF EXISTS reserve_item_ids;
CALL generate_many_to_many_pairs(
	tab_one=>'account_gid', col_one=>'account_gid', 
	tab_two=>'contract_gid', col_two=>'contract_gid',
	tab_target=>'reserve_item_ids', amount=>1500);

DROP TABLE IF EXISTS reserve_item_1;
CALL fill_table_from_dict(
	tab_name=>'reserve_item_ids',
	tab_target=>'reserve_item_1',
	catalog_name=>'quality_category_cd',
	column_names=>array['quality_category_gid']);

ALTER TABLE reserve_item_1 RENAME COLUMN quality_category_gid TO quality_category_cd;

DROP TABLE IF EXISTS reserve_item;
CALL fill_table_with_data(
	id_tab_name=>'reserve_item_1',
	tab_target=>'reserve_item',
	column_types=>array['const_string(''A'')', 
						'date(''2399-12-31'',''2399-12-31'')',
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')',
						'real(0,100)',
						'real(0,100)'],
	column_names=>array['tech$row_status',
						'tech$end_date',
						'valid_begin_date',
						'valid_end_date',
						'reserve_prc',
						'reserve_prc_fact'],
	delete_source=>true);

----------------------------------------------------------------------
-- dm_account_link

DROP TABLE IF EXISTS account_gid_2;
CREATE TABLE account_gid_2 AS 
SELECT account_gid AS from_account_gid FROM account_gid;

DROP TABLE IF EXISTS dm_account_link_ids;
CALL generate_many_to_many_pairs(
	tab_one=>'account_gid_2', col_one=>'from_account_gid', 
	tab_two=>'account_gid', col_two=>'account_gid',
	tab_target=>'dm_account_link_ids', amount=>1000);

ALTER TABLE dm_account_link_ids RENAME COLUMN account_gid TO to_account_gid;
DROP TABLE account_gid_2;

DROP TABLE IF EXISTS dm_account_link_1;
CALL fill_table_from_dict(
	tab_name=>'dm_account_link_ids',
	tab_target=>'dm_account_link_1',
	catalog_name=>'dm_account_link_type',
	column_names=>array['account_link_type_id']);

DROP TABLE IF EXISTS dm_account_link;
CALL fill_table_with_data(
	id_tab_name=>'dm_account_link_1',
	tab_target=>'dm_account_link',
	column_types=>array['int(1,200)',
						'const_string(''A'')', 
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')',
						'text(4)'],
	column_names=>array['tech$upd_audit_id',
						'tech$row_status',
						'valid_begin_date',
						'valid_end_date',
						'tech$source'],
	delete_source=>true);

----------------------------------------------------------------------
-- dm_contract_link

DROP TABLE IF EXISTS contract_gid_2;
CREATE TABLE contract_gid_2 AS 
SELECT contract_gid AS from_contract_gid FROM contract_gid;

DROP TABLE IF EXISTS dm_contract_link_ids;
CALL generate_many_to_many_pairs(
	tab_one=>'contract_gid_2', col_one=>'from_contract_gid', 
	tab_two=>'contract_gid', col_two=>'contract_gid',
	tab_target=>'dm_contract_link_ids', amount=>1000);

ALTER TABLE dm_contract_link_ids RENAME COLUMN contract_gid TO to_contract_gid;
DROP TABLE contract_gid_2;

DROP TABLE IF EXISTS dm_contract_link_1;
CALL fill_table_from_dict(
	tab_name=>'dm_contract_link_ids',
	tab_target=>'dm_contract_link_1',
	catalog_name=>'dm_contract_link_type',
	column_names=>array['contract_link_type_cd']);

DROP TABLE IF EXISTS dm_contract_link;
CALL fill_table_with_data(
	id_tab_name=>'dm_contract_link_1',
	tab_target=>'dm_contract_link',
	column_types=>array['const_string(''A'')',
						'const_number(2)',
						'date(''2020-01-01'',''2021-01-01'')',
						'date(''2021-01-01'',''2022-01-01'')'],
	column_names=>array['tech$row_status',
						'algorithm_id'
						'valid_begin_date',
						'valid_end_date'],
	delete_source=>true);


