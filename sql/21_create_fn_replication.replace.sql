CREATE OR REPLACE FUNCTION os.fn_replication(p_target_schema text, p_target_table text, p_stage_schema text, p_stage_table text, p_append_column_name text)
  RETURNS void AS
$$
DECLARE
	/* This function is created but NOT used with HAWQ.  DELETE is not supported by HAWQ */
        v_location INT;
        v_procedure VARCHAR := 'os.fn_replication';

        v_debug BOOLEAN := true;
        v_rec RECORD;
        v_sql TEXT;
        v_columns TEXT;        
        v_pk_columns TEXT;
        v_pk_delete text;
        i INT := 0;
        v_arch_table TEXT;
        v_stage_table TEXT;
        
BEGIN
        v_location := 1000;
        --take the columns from the primary key and create the join statement for the DELETE below

        FOR v_rec IN (  SELECT a.attname::text AS column_name
                        FROM    (  
                                SELECT generate_series(1, array_upper(sub.attrnums,1)) as x, sub.attrnums, sub.localoid
                                FROM    (
                                        SELECT g.attrnums, g.localoid 
                                        FROM gp_distribution_policy g
                                        JOIN pg_class c on g.localoid = c.oid
                                        JOIN pg_namespace n on c.relnamespace = n.oid
                                        WHERE n.nspname = p_target_schema
                                        AND c.relname = p_target_table
                                        ) AS sub 
                                ) AS sub2
                        JOIN pg_attribute a ON sub2.localoid = a.attrelid
                        AND attrnums[X] = a.attnum) LOOP

                i := i + 1;
                
                IF i = 1 THEN
                        v_pk_delete := 'x.' || v_rec.column_name || ' = y.' || v_rec.column_name;
                        v_pk_columns := v_rec.column_name;
                ELSE
                        v_pk_delete := v_pk_delete || ' AND x.' || v_rec.column_name || ' = y.' || v_rec.column_name;
                        v_pk_columns := v_pk_columns || ', ' || v_rec.column_name;
                END IF;

        END LOOP;
        
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_pk_delete;
        END IF;
        
        --create the temp table that is distributed the same as the target table
        v_location := 2000;
        v_stage_table := p_stage_table || '_stage';
        v_sql := 'CREATE TEMPORARY TABLE temp_' || p_stage_schema || '_' || p_stage_table || ' ON COMMIT DROP AS ' ||
                         'SELECT * FROM "' || p_stage_schema || '"."' || p_stage_table || '" ' ||
                         'DISTRIBUTED BY (' || v_pk_columns || ')';

        v_location := 2200;
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;
        
        --delete the Update and Delete records in the Target Table
        v_location := 3500;
        v_sql := 'DELETE FROM ' || p_target_schema || '.' || p_target_table || ' x USING temp_' || p_stage_schema || '_' || p_stage_table || ' y ' || 
                         'WHERE ' || v_pk_delete || ' AND change_type IN (''U'', ''D'')';

        v_location := 3600;
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;

        --get the columns from the target table
        v_location := 3800;
        i := 0;
        FOR v_rec in SELECT column_name --, CASE WHEN data_type = 'character' THEN 'text' ELSE data_type END as data_type
                     FROM information_schema.columns 
                     WHERE table_schema = p_target_schema
                     AND table_name = p_target_table
                     ORDER BY ordinal_position LOOP
                
                i := i + 1;
                IF i = 1 THEN
                    v_columns := v_rec.column_name;
                ELSE
                    v_columns := v_columns || ', ' || v_rec.column_name;
                END IF;
        
        END LOOP;
        
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_columns;
        END IF;
        --insert the Update and Insert records
        v_location := 4000;
        v_sql := 'INSERT INTO "' || p_target_schema || '"."' || p_target_table || '" ( ' || v_columns || ') ' ||
                         'SELECT ' || v_columns || ' FROM (' ||
                         'SELECT rank() over (partition by ' || v_pk_columns || ' order by "' || LOWER(p_append_column_name) || '" desc) as rank, ' || v_columns || ', change_type ' ||
                         'FROM temp_' || p_stage_schema || '_' || p_stage_table || ' ) AS SUB1 ' ||
                         'WHERE SUB1.rank = 1 AND change_type IN (''U'', ''I'')';

        v_location := 4100;
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;

        v_location := 5000;
        v_arch_table := p_target_schema || '_' || p_target_table || '_arch';
        
        v_sql := 'TRUNCATE TABLE "' || p_stage_schema || '"."' || v_arch_table || '"';
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;

        v_location := 6000;
        v_sql := 'INSERT INTO "' || p_stage_schema || '"."' || v_arch_table || '" ' ||
                'SELECT * FROM "' || 'temp_' || p_stage_schema || '_' || p_stage_table || '"';

        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;

EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION'(%:%:%)', v_procedure, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
