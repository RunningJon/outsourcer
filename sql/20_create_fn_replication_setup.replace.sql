CREATE OR REPLACE FUNCTION os.fn_replication_setup(p_target_schema text, p_target_table text, p_ext_schema text, p_stage_table text, p_arch_table text, p_column_name text)
  RETURNS void AS
$$
DECLARE
	/* This function is created but NOT used with HAWQ.  DELETE is not supported by HAWQ */
        v_function text := 'os.fn_replication_setup';
        v_location int;
        v_sql text;
        v_debug BOOLEAN := true;

BEGIN
	SET OPTIMIZER=OFF;
        v_location := 1000;
        v_sql := 'DROP TABLE IF EXISTS "' || p_ext_schema || '"."' || p_stage_table || '"';

        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;

        EXECUTE v_sql;

        v_location := 1500;
        v_sql := 'CREATE TABLE "' || p_ext_schema || '"."' || p_stage_table || '" ' ||
                 '("' || LOWER(p_column_name) || '" bigint not null, ' ||
                 'change_type char(1) NOT NULL, ' ||
                 'LIKE "' || p_target_schema || '"."' || p_target_table || '") ' ||
                 'WITH (APPENDONLY=true) ' ||
                 'DISTRIBUTED BY ("' || LOWER(p_column_name) || '")';
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        
        EXECUTE v_sql;

        v_location := 2000;
        v_sql := 'DROP TABLE IF EXISTS "' || p_ext_schema || '"."' || p_arch_table || '"';

        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;

        EXECUTE v_sql;

        v_location := 2500;
        v_sql := 'CREATE TABLE "' || p_ext_schema || '"."' || p_arch_table || '" ' ||
                 '("' || LOWER(p_column_name) || '" bigint not null, ' ||
                 'change_type char(1) NOT NULL, ' ||
                 'LIKE "' || p_target_schema || '"."' || p_target_table || '") ' ||
                 'WITH (APPENDONLY=true) ' ||
                 'DISTRIBUTED BY ("' || LOWER(p_column_name) || '")';
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        
        EXECUTE v_sql;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql VOLATILE;
