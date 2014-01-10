DELETE FROM os.variables 
WHERE name = 'oFetchSize';

INSERT INTO os.variables VALUES ('oFetchSize', '2000', false);

CREATE OR REPLACE FUNCTION os.fn_create_ext_table(p_table text, p_columns text[], p_connection_id integer, p_sql_text text)
  RETURNS void AS
$BODY$
DECLARE
        v_function_name text := 'os.fn_create_ext_table';
        v_location int;
        
        v_sql text;
        v_table text;
        v_java text;
        v_count int;
BEGIN
        v_location := 1000;
        PERFORM NULL
        FROM os.ext_connection
        WHERE id = p_connection_id;

        GET DIAGNOSTICS v_count = ROW_COUNT;
        
        v_location := 1500;
        IF v_count = 0 THEN
                RAISE EXCEPTION 'ConnectionID: "%" is not valid', p_connection_id;
        END IF;

        v_location := 2000;
        v_table := LOWER(p_table);
        v_sql := 'DROP EXTERNAL TABLE IF EXISTS ' || v_table;
        RAISE INFO '%', v_sql;
        EXECUTE v_sql;

        v_location := 3000;
        FOR i IN array_lower(p_columns, 1)..array_upper(p_columns,1) LOOP
                IF i = 1 THEN
                        v_sql := 'CREATE EXTERNAL WEB TABLE ' || v_table || E' \n' ||
                                '(' || p_columns[i];
                ELSE
                        v_sql := v_sql || E', \n' || p_columns[i];
                END IF;

        END LOOP;

        v_location := 3500;
        v_sql := v_sql || E')\n' || 'EXECUTE E''java -classpath ';

        v_location := 4000;
        SELECT os.jar || ':' || g.jar || ':' || CASE WHEN ms.jar IS NOT NULL THEN ms.jar || ':'  ELSE '' END || COALESCE(o.jar, '') ||
                ' -Xms' || s.xms || ' -Xmx' || x.xmx || ' -Djava.security.egd=file:///dev/urandom ExternalData ' || current_database() || ' ' || p.port || ' ' || e.id || 
                ' "' || p_sql_text || '"'
        INTO v_java
        FROM    (SELECT value AS jar FROM os.variables WHERE name = 'gpdbJar') AS g,
                (SELECT value AS jar FROM os.variables WHERE name = 'osJar') AS os,
                (SELECT value AS jar FROM os.variables WHERE name = 'msJar') AS ms,
                (SELECT value AS jar FROM os.variables WHERE name = 'oJar') AS o,
                (SELECT value AS xms FROM os.variables WHERE name = 'Xms') AS s,
                (SELECT value AS xmx FROM os.variables WHERE name = 'Xmx') AS x,
                (SELECT setting AS port FROM pg_settings WHERE name = 'port') AS p,
                os.ext_connection e
        WHERE e.id = p_connection_id;

        v_location := 5000;
        v_sql := v_sql || v_java || E''' ON MASTER FORMAT ''TEXT'' (delimiter ''|'' null ''null'')';
        RAISE INFO '%', v_sql;
        EXECUTE v_sql;
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql VOLATILE;

