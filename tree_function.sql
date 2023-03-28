CREATE OR REPLACE PROCEDURE generate_account_trees(h integer DEFAULT 2, n integer DEFAULT 100)
LANGUAGE 'plpgsql' AS 
$$
DECLARE step integer; i integer; n1 integer; amount integer;
BEGIN
	
	step = n/h;
	n1 = n/2;

	IF h > 2 
		THEN amount = (step*1.5)::int;
		ELSE amount = step;
	END IF;

	DROP TABLE IF EXISTS account_gid;
	CALL generate_table_id_inc(
		tab_name=>'account_gid', col_name=>'account_gid', 
		min_num=>1, max_num=>n1);
		
	DROP TABLE IF EXISTS account_summary_gid;
	CALL generate_table_id_inc(
		tab_name=>'account_summary_gid', col_name=>'account_summary_gid', 
		min_num=>n1+1, max_num=>n1+step);
		
	DROP TABLE IF EXISTS dm_account_ids;
	CALL generate_one_to_one_or_many_pairs(
--	CALL generate_one_to_many_mandatory_pairs(
		tab_one=>'account_summary_gid', col_one=>'account_summary_gid', 
		tab_two=>'account_gid', col_two=>'account_gid',
		tab_target=>'dm_account_ids', amount=>amount);--(step*1.5)::int); --- + 10
		
	DROP TABLE IF EXISTS dm_account_done;
	CREATE TABLE dm_account_done AS
	(SELECT account_gid, account_summary_gid
	FROM account_gid
	LEFT JOIN dm_account_ids USING (account_gid)
	
	UNION ALL
	
	SELECT account_summary_gid, NULL
	FROM account_summary_gid);
	
-------------------------------------------------------------
	FOR i IN 1..h-2 LOOP
		
		n1 = n1 + step + 1;
		step = step/2;
	
		IF h > 2 
			THEN amount = (step*1.5)::int;
			ELSE amount = step;
		END IF; 
	
		RAISE NOTICE 'n1: %', n1;
		RAISE NOTICE 'step: %', step;
		
		DROP TABLE IF EXISTS dm_account_ids_1;
		ALTER TABLE dm_account_done RENAME TO dm_account_ids_1;
	
		DROP TABLE IF EXISTS account_gid;
		CREATE TABLE account_gid AS
		SELECT account_gid FROM dm_account_ids_1 WHERE account_summary_gid IS NULL; 
		
		DROP TABLE IF EXISTS account_summary_gid;
		CALL generate_table_id_inc(
			tab_name=>'account_summary_gid', col_name=>'account_summary_gid', 
			min_num=>n1, max_num=>n1+step);
			
		DROP TABLE IF EXISTS dm_account_ids;
		CALL generate_one_to_one_or_many_pairs(
--		CALL generate_one_to_many_mandatory_pairs(
			tab_one=>'account_summary_gid', col_one=>'account_summary_gid', 
			tab_two=>'account_gid', col_two=>'account_gid',
			tab_target=>'dm_account_ids', amount=>amount);--(step*1.5)::int); --- + 10
		
		DROP TABLE IF EXISTS dm_account_done;
		CREATE TABLE dm_account_done AS
		(SELECT l1.account_gid, COALESCE(l1.account_summary_gid, l2.account_summary_gid) AS account_summary_gid
		FROM dm_account_ids_1 l1
		LEFT JOIN dm_account_ids l2 USING (account_gid)
		
		UNION ALL
		
		SELECT account_summary_gid, NULL
		FROM account_summary_gid);
		
	END LOOP;
	
	DROP TABLE dm_account_ids;
	ALTER TABLE dm_account_done RENAME TO dm_account_ids;

END
$$;

CALL generate_account_trees(3,100);


SELECT count(DISTINCT account_summary_gid) FROM dm_account_ids dai 

SELECT count(*) FROM dm_account_ids

WITH RECURSIVE r AS (
   SELECT account_gid, account_summary_gid, 1 AS level
   FROM dm_account_ids 
--   WHERE account_gid = 156

   UNION ALL

   SELECT d.account_gid, d.account_summary_gid, r.level + 1 AS level
   FROM dm_account_ids d
      JOIN r
          ON d.account_summary_gid = r.account_gid
)

SELECT DISTINCT level FROM r;
--SELECT count(*) FROM account_gid--account_summary_gid

SELECT * FROM dm_account_ids dai ORDER BY account_gid--summary_gid 


