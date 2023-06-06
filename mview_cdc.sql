CREATE OR REPLACE PACKAGE HR.mview_cdc
IS
  PROCEDURE create_mview_log(
    p_table_name_in     VARCHAR2,
    p_exclusion_flag_in BOOLEAN DEFAULT FALSE); 
END;
/

CREATE OR REPLACE PACKAGE BODY HR.mview_cdc IS
       PROCEDURE create_mview_log (
        p_table_name_in      VARCHAR2,
        p_exclusion_flag_in  BOOLEAN DEFAULT false
    ) IS

        PROCEDURE creation_logic (
            p_table_name  VARCHAR2,
            p_iot_type    VARCHAR2
        ) IS
            l_options           VARCHAR2(4000 CHAR);
            l_mview_log_exists  NUMBER;
            l_pk_available      NUMBER;
            l_collist           VARCHAR2(4000 CHAR);
            l_mview_log_name    VARCHAR2(4000 CHAR);
        BEGIN
            SELECT
                COUNT(1)
            INTO l_mview_log_exists
            FROM
                user_mview_logs
            WHERE
                master = p_table_name;

            IF l_mview_log_exists = 0 THEN
                SELECT
                    COUNT(1)
                INTO l_pk_available
                FROM
                    user_constraints
                WHERE
                        table_name = p_table_name
                    AND constraint_type = 'P';

                l_options := 'SEQUENCE';
                IF p_iot_type IS NULL THEN
                    l_options := l_options
                                 || ','
                                 || 'ROWID';
                END IF;

                IF l_pk_available = 1 THEN
                    l_options := l_options
                                 || ','
                                 || 'PRIMARY KEY';
                ELSE
                    SELECT
                        LISTAGG(column_name, ',') WITHIN GROUP(
                            ORDER BY
                                column_id
                        ) AS collist
                    INTO l_collist
                    FROM
                        user_tab_columns
                    WHERE
                        table_name = p_table_name;

                    l_options := l_options
                                 || '('
                                 || l_collist
                                 || ')';
                END IF;

                EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW LOG ON '
                                  || p_table_name
                                  || ' WITH '
                                  || l_options
                                  || ' INCLUDING NEW VALUES';

            SELECT
                log_table
            INTO l_mview_log_name
            FROM
                user_mview_logs
            WHERE
                master = p_table_name;

          --DBMS_OUTPUT.PUT_LINE( 'CREATE MATERIALIZED VIEW LOG ON '||I.TABLE_NAME||' WITH '||L_OPTIONS||' INCLUDING NEW VALUES');
                EXECUTE IMMEDIATE 'ALTER TABLE '
                                  || l_mview_log_name
                                  || ' ADD CHANGE_TIME$$ TIMESTAMP DEFAULT SYSTIMESTAMP';
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line('ERROR IN CREATING MVLOG ON '
                                     || p_table_name
                                     || ': ERROR: '
                                     || sqlerrm);
        END;

    BEGIN
        IF p_table_name_in = 'ALL' THEN
            FOR i IN (
                SELECT
                    *
                FROM
                    user_tables
                WHERE
                    table_name NOT LIKE 'MLOG$%'
                    AND table_name NOT LIKE 'RUPD$%'
            ) LOOP
                creation_logic(i.table_name, i.iot_type);
            END LOOP;

        ELSIF p_exclusion_flag_in = false THEN
            FOR i IN (
                SELECT
                    *
                FROM
                    user_tables
                WHERE
                    table_name NOT LIKE 'MLOG$%'
                    AND table_name NOT LIKE 'RUPD$%'
                    AND table_name IN (
                        SELECT DISTINCT
                            TRIM(regexp_substr(p_table_name_in, '[^,]+', 1,
                                               level)) value
                        FROM
                            dual
                        CONNECT BY
                            regexp_substr(p_table_name_in,
                                          '[^,]+',
                                          1,
                                          level) IS NOT NULL
                    )
            ) LOOP
                creation_logic(i.table_name, i.iot_type);
            END LOOP;
        ELSIF p_exclusion_flag_in = true THEN
            FOR i IN (
                SELECT
                    *
                FROM
                    user_tables
                WHERE
                    table_name NOT LIKE 'MLOG$%'
                    AND table_name NOT LIKE 'RUPD$%'
                    AND table_name NOT IN (
                        SELECT DISTINCT
                            TRIM(regexp_substr(p_table_name_in, '[^,]+',
                                               1,
                                               level)) value
                        FROM
                            dual
                        CONNECT BY
                            regexp_substr(p_table_name_in,
                                          '[^,]+',
                                          1,
                                          level) IS NOT NULL
                    )
            ) LOOP
                creation_logic(i.table_name, i.iot_type);
            END LOOP;
        END IF;
    END;
END;
/