CREATE OR REPLACE FUNCTION os.fn_jobstop(p_os_server text, p_os_port int, p_job_port int)
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_jobstop';
        v_location int;
        v_external_schema text := 'ext';
        v_sql text;
        v_jobstop_port text;
        
BEGIN
        v_location := 1000;
        v_jobstop_port := 'jobstop_' || p_job_port;

        --The goal is to stop the gpfdist process
        v_location := 2000;
        v_sql := 'CREATE EXTERNAL TABLE "' || v_external_schema || '"."'  || v_jobstop_port || '"' || chr(10) || '(foo text) ' || chr(10) ||
                'LOCATION (''gpfdist://'|| p_os_server || ':' || p_os_port || '/config.properties+' || p_job_port  || '#transform=jobstop'')' || chr(10) ||
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
        v_sql := 'SELECT foo FROM "' || v_external_schema || '"."'  || v_jobstop_port || '"';
        RAISE INFO '%', v_sql;
        EXECUTE v_sql;


EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%,%:%:%)', v_function_name, v_location, sqlstate, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
