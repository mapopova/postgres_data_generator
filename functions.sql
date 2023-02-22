CREATE OR REPLACE PROCEDURE generate_table_id_inc(
	tab_name text, col_name text, min_num bigint, max_num bigint)
LANGUAGE 'plpgsql' AS 
$$
BEGIN 
	EXECUTE 
	'CREATE TABLE ' || quote_ident(tab_name) || ' AS 
		SELECT generate_series(' || min_num || ' ,' || max_num || ' ,1)
			    AS ' || quote_ident(col_name);
END
$$;



-- включительно
CREATE OR REPLACE FUNCTION random_between(low integer, high integer) 
   RETURNS integer 
LANGUAGE 'plpgsql' AS
$$
BEGIN
   RETURN floor(random()* (high-low + 1) + low);
END;
$$;



--CREATE OR REPLACE PROCEDURE generate_table_random(
--	tab_name text, col_name text, 
--	min_num integer, max_num integer, amount integer)
--LANGUAGE 'plpgsql' AS 
--$$
--BEGIN 
--	EXECUTE format('
--	CREATE TABLE %I AS 
--		SELECT random_between(%L,%L) 
--		FROM generate_series(1,%L) AS %I
--	', tab_name, min_num, max_num, amount, col_name);
--END
--$$;



CREATE OR REPLACE PROCEDURE generate_table_uuid(
	tab_name text, col_name text, amount integer)
LANGUAGE 'plpgsql' AS 
$$
BEGIN 
	EXECUTE format('
	CREATE TABLE %I AS 
		SELECT gen_random_uuid() 
		FROM generate_series(1,%L) AS %I
	', tab_name, amount, col_name);
END
$$;



CREATE OR REPLACE PROCEDURE generate_table_id_random(
	tab_name text, col_name text, 
	min_num bigint, max_num bigint, amount integer)
LANGUAGE 'plpgsql' AS 
$$
BEGIN 
	IF ((max_num - min_num + 1) < amount)
		THEN RAISE EXCEPTION 
		'Can not generate % unique numbers between % and %',
		amount, min_num, max_num;
	END IF;
	EXECUTE
	'CREATE TABLE ' || quote_ident(tab_name) || ' AS 
		SELECT ' || quote_ident(col_name) || ' 
		FROM generate_series(' || min_num || ' ,' || max_num || ' ,1)
				AS ' || quote_ident(col_name) || ' 
		ORDER BY random()
		LIMIT ' || amount;
--	EXECUTE format('
--	CREATE TABLE %I AS
--		SELECT %I
--		FROM generate_series(%L,%L,1) AS %I
--		ORDER BY random()
--		LIMIT %L
--	', tab_name, col_name, min_num, max_num,
--	   col_name, amount);
END
$$;



CREATE OR REPLACE PROCEDURE generate_one_to_one_pairs(
	tab_one text, col_one text, tab_two text, col_two text,
	tab_target text, amount integer)
LANGUAGE 'plpgsql' AS 
$$
DECLARE tab_1_size integer; tab_2_size integer;
BEGIN 
	EXECUTE 'SELECT count(*) FROM ' || quote_ident(tab_one)
		INTO tab_1_size;
	EXECUTE 'SELECT count(*) FROM ' || quote_ident(tab_two) 
		INTO tab_2_size;
	-- check that amount <= count(min(tab1,tab2)) 
	IF amount > LEAST(tab_1_size, tab_2_size)
		THEN RAISE EXCEPTION 
		'Can not generate % unique one-to-one pairs', amount;
	END IF;
	EXECUTE format('
	CREATE TABLE %I AS 
		SELECT id_1, id_2
		FROM (
	   		SELECT %I AS id_1, row_number() OVER (ORDER BY random()) AS rn
	   		FROM %I
	   	) x
		JOIN (
			SELECT %I AS id_2, row_number() OVER (ORDER BY random()) AS rn
			FROM %I
		) y USING (rn)
		LIMIT %L
	', tab_target,
--	   col_one, col_two,
	   col_one, tab_one,
	   col_two, tab_two,
	   amount);
END
$$;


-- 1 : 0..N
CREATE OR REPLACE PROCEDURE generate_one_to_many_pairs(
	tab_one text, col_one text, tab_two text, col_two text,
	tab_target text, amount integer)
LANGUAGE 'plpgsql' AS 
$$
DECLARE tab_1_size integer; tab_2_size integer;
BEGIN
	EXECUTE format('SELECT count(*) FROM %I', tab_one) 
		INTO tab_1_size;
	EXECUTE format('SELECT count(*) FROM %I', tab_two) 
		INTO tab_2_size;
	-- check that amount <= count(tab2) 
	IF amount > tab_2_size
		THEN RAISE EXCEPTION 
		'Can not generate % unique one-to-many pairs', amount;
	END IF;
	EXECUTE format('
	CREATE TABLE %I AS
		SELECT id_1, id_2
		FROM (
			SELECT %I AS id_1, row_number() OVER (ORDER BY random()) AS rn
			FROM %I
		) x
		JOIN (
			SELECT %I AS id_2, random_between(1,%L) rn
			FROM %I ORDER BY random()
		) y USING (rn)
		LIMIT %L
	', tab_target,
--	   col_one, col_two,
	   col_one, tab_one,
	   col_two, tab_1_size, tab_two,
	   amount);
END
$$;


-- 1 : 1..N
-- only for dim B > dim A (?)
CREATE OR REPLACE PROCEDURE generate_one_to_many_pairs_v2(
	tab_one text, col_one text, tab_two text, col_two text,
	tab_target text, amount integer)
LANGUAGE 'plpgsql' AS 
$$
DECLARE tab_1_size integer; tab_2_size integer;
BEGIN
	EXECUTE 'SELECT count(*) FROM ' || quote_ident(tab_one) 
		INTO tab_1_size;
	EXECUTE 'SELECT count(*) FROM ' || quote_ident(tab_two) 
		INTO tab_2_size;
	-- to do check amount = ?
	-- to do check dim B > dim A
	EXECUTE 
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 

		WITH tab1 AS
		(SELECT id_1, id_2
		FROM (
	   		SELECT ' || quote_ident(col_one) || ' AS id_1, 
				row_number() OVER (ORDER BY random()) AS rn
	   		FROM ' || quote_ident(tab_one) || '
	   	) AS x
		JOIN (
			SELECT ' || quote_ident(col_two) || ' AS id_2, 
				row_number() OVER (ORDER BY random()) AS rn
			FROM ' || quote_ident(tab_two) || '
		) AS y USING (rn)),

		tab2 AS 
		(SELECT id_1, id_2
		FROM (
			SELECT ' || quote_ident(col_one) || ' AS id_1,
 				row_number() OVER (ORDER BY random()) AS rn
			FROM ' || quote_ident(tab_one) || '
		) x
		JOIN (
			SELECT ' || quote_ident(col_two) || ' AS id_2, 
				random_between(1,' || quote_nullable(tab_1_size) ||') rn
			FROM ' || quote_ident(tab_two) || ' 
			ORDER BY random()
		) y USING (rn))

		(SELECT id_1, id_2
		FROM tab1)

		UNION 

		(SELECT id_1, id_2
		FROM tab2
		WHERE id_2 NOT IN (SELECT id_2 FROM tab1))

		LIMIT ' || quote_nullable(amount);
END
$$;



CREATE OR REPLACE PROCEDURE generate_many_to_many_pairs(
	tab_one text, col_one text, tab_two text, col_two text,
	tab_target text, amount integer)
LANGUAGE 'plpgsql' AS 
$$
DECLARE tab_1_size integer; tab_2_size integer;
BEGIN
	EXECUTE format('SELECT count(*) FROM %I', tab_one) 
		INTO tab_1_size;
	EXECUTE format('SELECT count(*) FROM %I', tab_two) 
		INTO tab_2_size;
	-- check that amount <= count(tab2)*count(tab1) 
	IF amount > tab_2_size * tab_1_size
		THEN RAISE EXCEPTION 
		'Can not generate % unique many-to-many pairs', amount;
	END IF;
	EXECUTE 
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 
		SELECT id_1, id_2
			FROM (
				SELECT ' || quote_ident(col_one) || ' AS id_1,
				random_between(1,' || quote_nullable(tab_2_size) ||') AS rn
				FROM ' || quote_ident(tab_one) || ' 
				ORDER BY random()
			) x
			JOIN (
				SELECT ' || quote_ident(col_two) || ' AS id_2,
				random_between(1,' || quote_nullable(tab_1_size) || ') AS rn
				FROM ' || quote_ident(tab_two) || '  
				ORDER BY random()
			) y USING (rn)
			LIMIT ' || quote_nullable(amount);
--	CREATE TABLE quote_ident(tab_target) AS
--		SELECT id_1, id_2
--		FROM (
--			SELECT quote_ident(col_one) AS id_1, random_between(1, tab_2_size) AS rn
--			FROM quote_ident(tab_one)
--			ORDER BY random()
--		) x
--		JOIN (
--			SELECT quote_ident(col_two) AS id_2, random_between(1, tab_1_size) AS rn
--			FROM quote_ident(tab_two) 
--			ORDER BY random()
--		) y USING (rn)
--		LIMIT amount;
END
$$;



