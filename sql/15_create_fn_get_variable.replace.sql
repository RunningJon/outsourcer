CREATE OR REPLACE FUNCTION os.fn_get_variable(p_name text)
  RETURNS text AS
$$
	SET OPTIMIZER=OFF;
        SELECT value FROM os.variables WHERE name = $1;
$$
LANGUAGE sql;
