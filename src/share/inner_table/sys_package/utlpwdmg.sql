#function_name:complexity_check, verify_function, string_distance
#author: mingdou.tmd

CREATE OR REPLACE FUNCTION complexity_check(password varchar2,
                                            chars    integer := NULL,
                                            letter   integer := NULL,
                                            upper_c  integer := NULL,
                                            lower_c  integer := NULL,
                                            digit    integer := NULL,
                                            special  integer := NULL) RETURN boolean
IS
    digit_array varchar2(10) := '0123456789';
    alpha_array varchar2(26) := 'abcdefghijklmnopqrstuvwxyz';
    letter_count integer := 0;
    upper_count integer := 0;
    lower_count integer := 0;
    digit_count integer := 0;
    special_count integer := 0;
    flag boolean := FALSE;
    len INTEGER := NVL(length(password), 0);
    i integer;
    ch CHAR(1);
BEGIN

    IF len > 256 THEN
        raise_application_error(-20020, 'Password length more than 256');
    END IF;

    FOR i IN 1..len LOOP
        ch := substr(password, i, 1);
        IF ch = '"' THEN
            flag := TRUE;
        ELSIF instr(digit_array, ch) > 0 THEN
            digit_count := digit_count + 1;
        ELSIF instr(alpha_array, LOWER(ch)) > 0 THEN
            letter_count := letter_count + 1;
            IF ch = LOWER(ch) THEN
                lower_count := lower_count + 1;
            ELSE
                upper_count := upper_count + 1;
            END IF;
        ELSE
            special_count := special_count + 1;
        END IF;
    END LOOP;

    IF flag = 1 THEN
        raise_application_error(-20012, 'password must NOT contain a ' ||
                                        'double-quote character, which is ' ||
                                        'reserved as a password delimiter');
    END IF;
    IF chars IS NOT NULL AND len < chars THEN
        raise_application_error(-20001, 'Password length less than ' ||
                                chars);
    END IF;
    IF letter IS NOT NULL AND letter_count < letter THEN
        raise_application_error(-20022, 'Password must contain at least ' ||
                                        letter || ' letter(s)');
    END IF;
    IF upper_c IS NOT NULL AND upper_count < upper_c THEN
        raise_application_error(-20023, 'Password must contain at least ' ||
                                        upper_c || ' uppercase character(s)');
    END IF;
    IF lower_c IS NOT NULL AND lower_count < lower_c THEN
        raise_application_error(-20024, 'Password must contain at least ' ||
                                        lower_c || ' lowercase character(s)');
    END IF;
    IF digit IS NOT NULL AND digit_count < digit THEN
        raise_application_error(-20025, 'Password must contain at least ' ||
                                        digit || ' digit(s)');
    END IF;
    IF special IS NOT NULL AND special_count < special THEN
        raise_application_error(-20026, 'Password must contain at least ' ||
                                        special || ' special character(s)');
    END IF;

    RETURN TRUE;
END;
/

CREATE OR REPLACE FUNCTION string_distance(s varchar2, t varchar2) RETURN integer
IS
    TYPE arr IS TABLE OF number INDEX BY BINARY_INTEGER;
    TYPE matrix IS TABLE OF arr INDEX BY BINARY_INTEGER; 
    d matrix;
    s_len INTEGER := NVL(LENGTH(s), 0);
    t_len INTEGER := NVL(LENGTH(t), 0);
BEGIN
    FOR i IN 0..s_len LOOP
        d(i)(0) := i;
    END LOOP;
    FOR j IN 0..t_len LOOP
        d(0)(j) := j;
    END LOOP;
    FOR i IN 1..s_len LOOP
        FOR j IN 1..t_len LOOP
            IF substr(s, i, 1) = substr(t, j, 1) THEN
                d(i)(j) := d(i - 1)(j - 1);
            ELSE
                d(i)(j) := least(d(i - 1)(j), d(i)(j - 1), d(i - 1)(j - 1)) + 1;
            END IF;
        END LOOP;
    END LOOP;
    RETURN d(s_len)(t_len);
END;
/

CREATE OR REPLACE FUNCTION verify_function(username varchar2,
                                           password varchar2,
                                           old_password varchar2) RETURN boolean
IS
    differ          integer;
    pw_lower        varchar2(256);
    db_name         varchar2(40);
    i               integer;
    simple_password varchar2(10);
    reverse_user    varchar2(32);
BEGIN
    IF NOT complexity_check(password, 8, 1, 1) THEN
        RETURN FALSE;
    END IF;

    pw_lower := LOWER(password);
    IF instr(pw_lower, LOWER(username)) > 0 THEN
        raise_application_error(-20002, 'Password contains the username');
    END IF;

    reverse_user := '';
    FOR i in REVERSE 1 .. length(username) LOOP
        reverse_user := reverse_user || substr(username, i, 1);
    END LOOP;
    IF instr(pw_lower, LOWER(reverse_user)) > 0 THEN
        raise_application_error(-20003, 'Password contains the username reversed');
    END IF;

    db_name := 'oceanbase';
    IF instr(pw_lower, LOWER(db_name)) > 0 THEN
        raise_application_error(-20004, 'Password contains the server name');
    END IF;

    IF instr(pw_lower, 'oracle') > 0 THEN
        raise_application_error(-20006, 'Password too simple');
    END IF;

    IF pw_lower IN ('welcome1', 'database1', 'account1',
                    'user1234', 'password1', 'oracle123',
                    'computer1', 'abcdefg1', 'change_on_install') THEN
        raise_application_error(-20006, 'Password too simple');
    END IF;

    IF old_password IS NOT NULL THEN
        differ := string_distance(old_password, password);
        IF differ < 3 THEN
            raise_application_error(-20010, 'Password should differ from the old password by at least 3 characters');
        END IF;
    END IF;

    RETURN TRUE;
END;
/
