DROP TABLE IF EXISTS account_gid;
CALL generate_table_id_inc(
	tab_name=>'account_gid', col_name=>'account_gid', 
	min_num=>1, max_num=>100);
	
DROP TABLE IF EXISTS account_summary_gid;
CALL generate_table_id_inc(
	tab_name=>'account_summary_gid', col_name=>'account_summary_gid', 
	min_num=>101, max_num=>130);
	
DROP TABLE IF EXISTS dm_account_ids;
CALL generate_one_to_one_or_many_pairs(
	tab_one=>'account_summary_gid', col_one=>'account_summary_gid', 
	tab_two=>'account_gid', col_two=>'account_gid',
	tab_target=>'dm_account_ids', amount=>40);
	
DROP TABLE IF EXISTS dm_account_done;
CREATE TABLE dm_account_done AS
(SELECT account_gid, account_summary_gid
FROM account_gid
LEFT JOIN dm_account_ids USING (account_gid)

UNION ALL

SELECT account_summary_gid, NULL
FROM account_summary_gid);

DROP TABLE IF EXISTS dm_account_ids_1;
ALTER TABLE dm_account_done RENAME TO dm_account_ids_1;
-----------------------

DROP TABLE IF EXISTS account_gid;
CREATE TABLE account_gid AS
SELECT account_gid FROM dm_account_ids_1 WHERE account_summary_gid IS NULL; 

DROP TABLE IF EXISTS account_summary_gid;
CALL generate_table_id_inc(
	tab_name=>'account_summary_gid', col_name=>'account_summary_gid', 
	min_num=>131, max_num=>150);
	
DROP TABLE IF EXISTS dm_account_ids;
CALL generate_one_to_one_or_many_pairs(
	tab_one=>'account_summary_gid', col_one=>'account_summary_gid', 
	tab_two=>'account_gid', col_two=>'account_gid',
	tab_target=>'dm_account_ids', amount=>30);

-----

--DROP TABLE IF EXISTS account_summary_gid_lvl2;
--CALL generate_table_id_inc(
--	tab_name=>'account_summary_gid_lvl2', col_name=>'account_summary_gid_lvl2', 
--	min_num=>26, max_num=>27);
--
--DROP TABLE IF EXISTS dm_account_ids_lvl2;
--CALL generate_one_to_one_or_many_pairs(
--	tab_one=>'account_summary_gid_lvl2', col_one=>'account_summary_gid_lvl2', 
--	tab_two=>'account_summary_gid', col_two=>'account_summary_gid',
--	tab_target=>'dm_account_ids_lvl2', amount=>3);

--DROP TABLE IF EXISTS dm_account_done;
--CREATE TABLE dm_account_done AS
--(SELECT l1.account_gid, COALESCE(l1.account_summary_gid,l2.account_summary_gid_lvl2)
--FROM dm_account_ids l1
--LEFT JOIN dm_account_ids_lvl2 l2 ON l1.account_gid = l2.account_summary_gid
--
--UNION ALL
--
--SELECT account_summary_gid_lvl2, NULL
--FROM account_summary_gid_lvl2);

DROP TABLE IF EXISTS dm_account_done;
CREATE TABLE dm_account_done AS
(SELECT l1.account_gid, COALESCE(l1.account_summary_gid, l2.account_summary_gid) AS account_summary_gid
FROM dm_account_ids_1 l1
LEFT JOIN dm_account_ids l2 USING (account_gid)

UNION ALL

SELECT account_summary_gid, NULL
FROM account_summary_gid);

--DROP TABLE dm_account_ids;
--ALTER TABLE dm_account_done RENAME TO dm_account_ids;

DROP TABLE IF EXISTS dm_account_ids_1;
ALTER TABLE dm_account_done RENAME TO dm_account_ids_1;


--SELECT * FROM dm_account_ids dai 

-----------------------------------------------------------

DROP TABLE IF EXISTS account_gid;
CREATE TABLE account_gid AS
SELECT account_gid FROM dm_account_ids_1 WHERE account_summary_gid IS NULL; 

DROP TABLE IF EXISTS account_summary_gid;
CALL generate_table_id_inc(
	tab_name=>'account_summary_gid', col_name=>'account_summary_gid', 
	min_num=>151, max_num=>160);
	
DROP TABLE IF EXISTS dm_account_ids;
CALL generate_one_to_one_or_many_pairs(
	tab_one=>'account_summary_gid', col_one=>'account_summary_gid', 
	tab_two=>'account_gid', col_two=>'account_gid',
	tab_target=>'dm_account_ids', amount=>20);

DROP TABLE IF EXISTS dm_account_done;
CREATE TABLE dm_account_done AS
(SELECT l1.account_gid, COALESCE(l1.account_summary_gid, l2.account_summary_gid) AS account_summary_gid
FROM dm_account_ids_1 l1
LEFT JOIN dm_account_ids l2 USING (account_gid)

UNION ALL

SELECT account_summary_gid, NULL
FROM account_summary_gid);

-- у последнего
DROP TABLE dm_account_ids;
ALTER TABLE dm_account_done RENAME TO dm_account_ids;

------------------------------

SELECT * FROM dm_account_ids dai 

------------------------------

WITH RECURSIVE r AS (
   SELECT account_gid, account_summary_gid, 1 AS level
   FROM dm_account_ids 
   WHERE account_gid = 156

   UNION ALL

   SELECT d.account_gid, d.account_summary_gid, r.level + 1 AS level
   FROM dm_account_ids d
      JOIN r
          ON d.account_summary_gid = r.account_gid
)

SELECT * FROM r;
