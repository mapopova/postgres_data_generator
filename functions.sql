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
   RETURN floor(random() * (high-low + 1) + low);
END;
$$;



CREATE OR REPLACE FUNCTION random_double_between(low integer DEFAULT 0, high integer DEFAULT 2147483647) 
   RETURNS double precision
LANGUAGE 'plpgsql' AS
$$
BEGIN
   RETURN random() * (high - low) + low;
END;
$$;


CREATE OR REPLACE PROCEDURE generate_table_uuid(
	tab_name text, col_name text, amount integer)
LANGUAGE 'plpgsql' AS 
$$
BEGIN 
	EXECUTE format('
	CREATE TABLE %I AS 
		SELECT gen_random_uuid() AS %I
		FROM generate_series(1,%L)
	', tab_name, col_name, amount);
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
	EXECUTE
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 
		SELECT ' || quote_ident(col_one) || ', ' || quote_ident(col_two) || ' 
		FROM (
			SELECT ' || quote_ident(col_one) || ', row_number() OVER (ORDER BY random()) AS rn
	   		FROM ' || quote_ident(tab_one) || '
	   	) AS x
		JOIN (
			SELECT ' || quote_ident(col_two) || ', row_number() OVER (ORDER BY random()) AS rn
			FROM ' || quote_ident(tab_two) || '
		) AS y USING (rn)
		LIMIT ' || quote_nullable(amount);
END
$$;


-- 1 : 0..N
CREATE OR REPLACE PROCEDURE generate_one_to_zero_or_many_pairs(
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
	EXECUTE
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 
		SELECT ' || quote_ident(col_one) || ', ' || quote_ident(col_two) || ' 
		FROM (
			SELECT ' || quote_ident(col_one) || ', row_number() OVER (ORDER BY random()) AS rn
			FROM ' || quote_ident(tab_one) || '
		) x
		JOIN (
			SELECT ' || quote_ident(col_two) || ',
			random_between(1,' || quote_nullable(tab_1_size) ||') AS rn
			FROM ' || quote_ident(tab_two) || ' ORDER BY random()
		) y USING (rn)
		LIMIT ' || quote_nullable(amount);
END
$$;


-- 1 : 1..N
-- only for dim B > dim A (?)
CREATE OR REPLACE PROCEDURE generate_one_to_one_or_many_pairs(
	tab_one text, col_one text, tab_two text, col_two text,
	tab_target text, amount integer)
LANGUAGE 'plpgsql' AS 
$$
DECLARE tab_1_size integer; tab_2_size integer; limit_n integer;
BEGIN
	EXECUTE 'SELECT count(*) FROM ' || quote_ident(tab_one) 
		INTO tab_1_size;
	EXECUTE 'SELECT count(*) FROM ' || quote_ident(tab_two) 
		INTO tab_2_size;
	limit_n = amount - tab_1_size;
	IF tab_2_size < tab_1_size OR amount < tab_1_size OR amount > tab_2_size
		THEN RAISE EXCEPTION 
		'Can not generate % unique one to (one or many) pairs', amount;
	END IF;
	EXECUTE 
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 

		WITH tab1 AS
		(SELECT ' || quote_ident(col_one) || ', ' || quote_ident(col_two) || ' 
		FROM (
	   		SELECT ' || quote_ident(col_one) || ', 
				row_number() OVER (ORDER BY random()) AS rn
	   		FROM ' || quote_ident(tab_one) || '
	   	) AS x
		JOIN (
			SELECT ' || quote_ident(col_two) || ', 
				row_number() OVER (ORDER BY random()) AS rn
			FROM ' || quote_ident(tab_two) || '
		) AS y USING (rn)),

		tab2 AS 
		(SELECT ' || quote_ident(col_one) || ', ' || quote_ident(col_two) || '
		FROM (
			SELECT ' || quote_ident(col_one) || ',
 				row_number() OVER (ORDER BY random()) AS rn
			FROM ' || quote_ident(tab_one) || '
		) x
		JOIN (
			SELECT ' || quote_ident(col_two) || ', 
				random_between(1,' || quote_nullable(tab_1_size) ||') rn
			FROM ' || quote_ident(tab_two) || ' 
			ORDER BY random()
		) y USING (rn))

		(SELECT ' || quote_ident(col_one) || ', ' || quote_ident(col_two) || '
		FROM tab1)

		UNION 

		(SELECT ' || quote_ident(col_one) || ', ' || quote_ident(col_two) || '
		FROM tab2
		WHERE ' || quote_ident(col_two) || ' NOT IN (SELECT ' || quote_ident(col_two) || ' FROM tab1)
		LIMIT ' || quote_nullable(limit_n) || ')';
END
$$;



CREATE OR REPLACE PROCEDURE generate_many_to_many_pairs(
	tab_one text, col_one text, tab_two text, col_two text,
	tab_target text, random_1 integer, random_2 integer, amount integer)
LANGUAGE 'plpgsql' AS 
$$
DECLARE --random_1 integer; random_2 integer;
		tab_1_size integer; tab_2_size integer;
BEGIN
--	random_1 = 1000;
--	random_2 = 1000;
	EXECUTE format('SELECT count(*) FROM %I', tab_one) 
		INTO tab_1_size;
	EXECUTE format('SELECT count(*) FROM %I', tab_two) 
		INTO tab_2_size;
--	 check that amount <= count(tab2)*count(tab1) 
	IF amount > tab_2_size * tab_1_size
		THEN RAISE EXCEPTION 
		'Can not generate % unique many-to-many pairs', amount;
	END IF;
	EXECUTE 
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 
		SELECT ' || quote_ident(col_one) || ', ' || quote_ident(col_two) || ' 
			FROM (
				SELECT ' || quote_ident(col_one) || ',
				random_between(1,' || quote_nullable(random_1) ||') AS rn
				FROM ' || quote_ident(tab_one) || ' 
				ORDER BY random()
			) x
			JOIN (
				SELECT ' || quote_ident(col_two) || ',
				random_between(1,' || quote_nullable(random_2) || ') AS rn
				FROM ' || quote_ident(tab_two) || '  
				ORDER BY random()
			) y USING (rn)
			LIMIT ' || quote_nullable(amount);
END
$$;



CREATE OR REPLACE PROCEDURE generate_chains(
	tab_name text, col_name text,
	tab_target text, amount integer)
-- не всегда получается нужное кол-во цепочек из-за рандома
-- подумать, как это исправить и надо ли
-- мб как-то сделать длину цепочек неравномерной..?
LANGUAGE 'plpgsql' AS 
$$
DECLARE tab_size integer;
BEGIN 
	EXECUTE 'SELECT count(*) FROM ' || quote_ident(tab_name)
		INTO tab_size; 
	IF amount > tab_size / 2
	-- мб ужесточить условие
		THEN RAISE EXCEPTION 
		'Can not generate % chains', amount;
	END IF;
	EXECUTE 
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 
		SELECT id_1, id_2
		FROM (
			SELECT ' || quote_ident(col_name) || ' AS id_1,
				lead(' || quote_ident(col_name) || ') 
				OVER (PARTITION BY random_between(1,' 
								|| quote_nullable(amount) ||')
					ORDER BY random()) AS id_2
			FROM ' || quote_ident(tab_name) || ' 
			ORDER BY random() 
			) AS p
		WHERE id_2 IS NOT NULL';
END
$$;



CREATE OR REPLACE FUNCTION random_string_md5(str_length int) RETURNS TEXT
LANGUAGE SQL AS 
$$ 
  SELECT
    substring(
      (SELECT string_agg(md5(random()::TEXT), '')
       FROM generate_series(1, CEIL($1 / 32.)::integer)), 1, $1);
$$;



CREATE OR REPLACE FUNCTION random_string_2(low int DEFAULT 1, high int DEFAULT 100) RETURNS TEXT
LANGUAGE SQL AS 
-- понять, почему иногда возвращает длину меньше заданной 
-- сделать вариант с заглавными буквами, с русским языком?
$$	
    SELECT string_agg(substring('0123456789abcdefghijklmnopqrstuvwxyz', round(random() * 36)::integer, 1), '')
    FROM generate_series(1, random_between(low, high));
$$;



CREATE OR REPLACE FUNCTION random_string(str_length int) RETURNS TEXT
LANGUAGE SQL AS 
-- понять, почему иногда возвращает длину меньше заданной 
-- сделать вариант с заглавными буквами, с русским языком?
$$	
    SELECT string_agg(substring('0123456789abcdefghijklmnopqrstuvwxyz', round(random() * 36)::integer, 1), '')
    FROM generate_series(1, str_length);
$$;



CREATE OR REPLACE FUNCTION random_date(low date DEFAULT '4714-11-24 BC', high date DEFAULT '5874897-12-31') 
   RETURNS date
LANGUAGE 'plpgsql' AS
$$
BEGIN
	RETURN low::date + random_between(0,(high::date - low::date));
END;
$$;



CREATE OR REPLACE PROCEDURE fill_table_with_data(
	id_tab_name text, tab_target text, column_types text[], column_names text[] DEFAULT array[]::text[])
LANGUAGE 'plpgsql' AS 
$$
DECLARE query TEXT = ''; i int = 1; elem TEXT; col_name TEXT; colnames_length int;
BEGIN
	colnames_length = COALESCE(array_length(column_names, 1),0);
	FOREACH elem IN ARRAY column_types
	LOOP
		IF i <= colnames_length 
			THEN col_name = quote_ident(column_names[i]); --RAISE NOTICE 'col_name: %', col_name;
			ELSE col_name = 'col_' || i;
		END IF;
		CASE 
			WHEN elem = 'int' THEN query = query || ', random_between(0,2147483646) AS ' || col_name;
			WHEN elem LIKE 'int(%)' THEN
				elem = replace(elem,'int','random_between');
				query = query || ', ' || elem || ' AS ' || col_name;
			WHEN elem = 'text' THEN --query = query || ', md5(random()::text) AS ' || col_name;
				query = query || ', random_string_2() AS ' || col_name;
			WHEN elem LIKE 'text(%,%)' THEN
				elem = replace(elem,'text','random_string_2');
				query = query || ', ' || elem || ' AS ' || col_name;
			WHEN elem LIKE 'text(%)' THEN 
				elem = replace(elem,'text','random_string'); -- lol
				query = query || ', ' || elem || ' AS ' || col_name;
			WHEN elem = 'date' THEN query = query || ', random_date() AS ' || col_name;
			WHEN elem LIKE 'date(%)' THEN
				elem = regexp_replace(elem,'date','random_date');
				query = query || ', ' || elem || ' AS ' || col_name;
			WHEN elem = 'double' THEN query = query || ', random_double_between() AS ' || col_name;
		END CASE;
	i = i + 1;
	END LOOP;
	RAISE NOTICE 'query: %', query;
	EXECUTE
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 
		SELECT *' ||
			query  || 
		' FROM ' || quote_ident(id_tab_name);
END
$$;



CREATE OR REPLACE PROCEDURE fill_table_from_catalog(
	tab_name text, tab_target text, catalog_name text, column_names text[])
LANGUAGE 'plpgsql' AS 
$$
-- to do мб убрать удаление столбца в конце))
DECLARE catalog_size int; query text = ''; column_name text;
BEGIN 
	EXECUTE format('SELECT count(*) FROM %I', catalog_name) 
		INTO catalog_size;
	FOREACH column_name IN ARRAY column_names
	LOOP 
		query = query || ', ' || quote_ident(column_name);
	END LOOP;
	RAISE NOTICE 'query: %', query;
	EXECUTE 
	'CREATE TABLE ' || quote_ident(tab_target) || ' AS 
		SELECT *
			FROM (
				SELECT *,
				random_between(1,' || quote_nullable(catalog_size) ||') AS rn
				FROM ' || quote_ident(tab_name) || ' 
				ORDER BY random()
			) x
			JOIN (
				SELECT row_number() OVER (ORDER BY random()) AS rn' 
				|| query  || '
				FROM ' || quote_ident(catalog_name) || ' 
			) y USING (rn)
		ORDER BY random()';
	EXECUTE 
	'ALTER TABLE ' || quote_ident(tab_target) || ' DROP COLUMN rn';
END
$$;

