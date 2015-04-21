CREATE OR REPLACE FUNCTION os.fn_customstop(p_os_server text, p_os_port integer, p_id integer)
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_customstop';
        v_location int;
        v_external_schema text := 'ext';
        v_sql text;
        v_customstop_port text;
        v_gpfdist_port int;
        v_count int := 0;
        
BEGIN
        v_location := 1000;
        SELECT gpfdist_port
        INTO v_gpfdist_port
        FROM os.custom_sql
        WHERE id = p_id;

        --make sure this port isn't used by another custom table
        v_location := 1250;
        SELECT COUNT(*)
        INTO v_count
        FROM os.custom_sql
        WHERE gpfdist_port = v_gpfdist_port
        AND id <> p_id;
        
        IF v_count = 0 THEN

                v_location := 1500;
                v_customstop_port := 'customstop_' || v_gpfdist_port;
                
                --The goal is to stop the gpfdist process
                v_location := 2000;
                v_sql := 'CREATE EXTERNAL TABLE "' || v_external_schema || '"."'  || v_customstop_port || '"' || chr(10) || '(foo text) ' || chr(10) ||
                        'LOCATION (''gpfdist://'|| p_os_server || ':' || p_os_port || '/config.properties+' || v_gpfdist_port  || '#transform=customstop'')' || chr(10) ||
                        E'FORMAT ''TEXT'' (delimiter ''|'' null ''null'' escape E''\\\\'')';
                BEGIN
                        RAISE INFO '%', v_sql;
                        EXECUTE v_sql;
                EXCEPTION 
                        WHEN duplicate_table THEN
                                --Ignore when the table already exists.  This helps to prevent catalog bloat.
                                NULL;
                END;

                v_location := 3000;
                v_sql := 'SELECT foo FROM "' || v_external_schema || '"."'  || v_customstop_port || '"';
                RAISE INFO '%', v_sql;
                EXECUTE v_sql;

        END IF;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
