--原生oracle加
CREATE OR REPLACE FUNCTION random_string(
    p_length IN NUMBER
) RETURN VARCHAR2
IS
    v_chars VARCHAR2(62) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    v_result VARCHAR2(4000) := '';
BEGIN
    FOR i IN 1..p_length LOOP
        v_result := v_result || SUBSTR(v_chars, FLOOR(DBMS_RANDOM.VALUE(1, LENGTH(v_chars) + 1)), 1);
    END LOOP;
    RETURN v_result;
END;
/

--本地
CREATE TABLE t_random (
    col1 VARCHAR2(100),
    col2 VARCHAR2(100),
    col3 VARCHAR2(100),
    col4 VARCHAR2(100),
    col5 VARCHAR2(100),
    col6 VARCHAR2(100),
    col7 VARCHAR2(100),
    col8 VARCHAR2(100),
    col9 VARCHAR2(100),
    col10 VARCHAR2(100),
    col11 VARCHAR2(100),
    col12 VARCHAR2(100),
    col13 VARCHAR2(100),
    col14 VARCHAR2(100),
    col15 VARCHAR2(100),
    col16 VARCHAR2(100),
    col17 VARCHAR2(100),
    col18 VARCHAR2(100),
    col19 VARCHAR2(100),
    col20 VARCHAR2(100)
);

--插远端
CREATE OR REPLACE PROCEDURE insert_t_random_remote(p_rows in number)
IS
BEGIN
    FOR i IN 1..p_rows LOOP
        INSERT INTO t_random@dblinkudf
        VALUES (
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10)
        );
    END LOOP;
END;
/

--插本地
CREATE OR REPLACE PROCEDURE insert_t_random_base(p_rows in number)
IS
BEGIN
    FOR i IN 1..p_rows LOOP
        INSERT INTO t_random
        VALUES (
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10),
            random_string@dblinkudf(10)
        );
    END LOOP;
END;
/