CREATE OR REPLACE FUNCTION random_int(low integer DEFAULT 0, high integer DEFAULT 2147483646) 
   RETURNS integer 
LANGUAGE 'plpgsql' AS
$$
BEGIN
   RETURN floor(random() * (high-low + 1) + low);
END;
$$;



CREATE OR REPLACE FUNCTION random_bigint(low bigint DEFAULT 0, high bigint DEFAULT 9223372036854775806) 
   RETURNS bigint
LANGUAGE 'plpgsql' AS
$$
BEGIN
   RETURN floor(random() * (high-low + 1) + low);
END;
$$;



CREATE OR REPLACE FUNCTION random_double(low integer DEFAULT 0, high integer DEFAULT 2147483647) 
   RETURNS double precision
LANGUAGE 'plpgsql' AS
$$
BEGIN
   RETURN random() * (high - low) + low;
END;
$$;



CREATE OR REPLACE FUNCTION random_real(low integer DEFAULT 0, high integer DEFAULT 2147483647) 
   RETURNS real
LANGUAGE 'plpgsql' AS
$$
BEGIN
   RETURN random() * (high - low) + low;
END;
$$;



CREATE OR REPLACE FUNCTION random_text(str_length int) RETURNS TEXT
LANGUAGE SQL AS 
$$ 
  SELECT
    substring(
      (SELECT string_agg(md5(random()::TEXT), '')
       FROM generate_series(1, CEIL($1 / 32.)::integer)), 1, $1);
$$;



CREATE OR REPLACE FUNCTION random_text(low int, high int) RETURNS TEXT
LANGUAGE SQL AS 
-- понять, почему иногда возвращает длину меньше заданной 
-- сделать вариант с заглавными буквами, с русским языком?
$$	
    SELECT string_agg(substring('0123456789abcdefghijklmnopqrstuvwxyz', round(random() * 36)::integer, 1), '')
    FROM generate_series(1, random_between(low, high));
$$;



CREATE OR REPLACE FUNCTION random_text() RETURNS TEXT -- типа по умолчанию, вставить в существующую не получилось тк конфликт
LANGUAGE SQL AS 
$$	
    SELECT string_agg(substring('0123456789abcdefghijklmnopqrstuvwxyz', round(random() * 36)::integer, 1), '')
    FROM generate_series(1, random_between(1, 100));
$$;



CREATE OR REPLACE FUNCTION random_date(low date DEFAULT '4714-11-24 BC', high date DEFAULT '5874897-12-31') 
   RETURNS date
LANGUAGE 'plpgsql' AS
$$
BEGIN
	RETURN low::date + random_between(0,(high::date - low::date));
END;
$$;



CREATE OR REPLACE FUNCTION random_timestamp(low timestamp DEFAULT '01-01-01 00:00:00', --'4714-11-24 BC 00:00:00'
											high timestamp DEFAULT '294276-12-31 00:00:00') --'294276-12-31 00:00:00'
   RETURNS timestamp
LANGUAGE 'plpgsql' AS
$$
BEGIN
--	RETURN low::timestamp + random_between(0,(high::timestamp - low::timestamp));
	RETURN low::timestamp + random() * (high::timestamp - low::timestamp);
END;
$$;



CREATE OR REPLACE FUNCTION const_string(str text) 
   RETURNS text
LANGUAGE 'plpgsql' AS
$$
BEGIN
   RETURN str;
END;
$$;



CREATE OR REPLACE FUNCTION const_number(num anyelement) 
   RETURNS anyelement
LANGUAGE 'plpgsql' AS
$$
BEGIN
   RETURN num;
END;
$$;