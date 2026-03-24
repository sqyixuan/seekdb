#package_name:dbms_random_body
#author:jingxing.lj

CREATE OR REPLACE PACKAGE BODY dbms_random AS
  mem       num_arr := num_arr();
  cnt       INTEGER := 55;
  need_init BOOLEAN := TRUE;
  norm_saved NUMBER := 0;

  FUNCTION TRUNC_RAND_VAL(RAND_VAL NUMBER) RETURN NUMBER AS
  BEGIN
    IF (rand_val >= 1.0) THEN
      RETURN RAND_VAL - 1.0;
    END IF;
    RETURN RAND_VAL;
  END TRUNC_RAND_VAL;

  PROCEDURE seed(val IN VARCHAR2) IS
    junk_buf  VARCHAR2(2000);
    piece     VARCHAR2(20);
    rand_val  NUMBER;
    tmp       NUMBER;

  BEGIN
    need_init     := FALSE;
    cnt           := 0;
    mem.delete;
    mem.extend(55);
    junk_buf          := val;
    FOR i IN 1 .. 55 LOOP
      piece       := SUBSTR(junk_buf, 1, 19);
      rand_val    := 0;

      FOR j IN 1 .. 19 LOOP
        rand_val := 100 * rand_val + NVL(ASCII(SUBSTR(piece, j, 1)), 0.0);
      END LOOP;

      rand_val     := rand_val * 0.00000000000000000000000000000000000001 + i * 0.01020304050607080910111213141516171819;
      mem(i)       := rand_val - TRUNC(rand_val);
      junk_buf     := SUBSTR(junk_buf, 20);
    END LOOP;

    rand_val       := mem(55);
    FOR j IN 0 .. 10 LOOP
      FOR i IN 1 .. 55 LOOP
        rand_val  := rand_val * 1000000000000000000000000;
        tmp       := TRUNC(rand_val);
        rand_val  := mem(i) + (rand_val - tmp) + (tmp * 0.00000000000000000000000000000000000001);
        rand_val  := TRUNC_RAND_VAL(rand_val);
        mem(i)    := rand_val;
      END LOOP;
    END LOOP;

  END seed;

  FUNCTION value RETURN NUMBER IS
  rand_val NUMBER;
  BEGIN
    cnt                := cnt + 1;
    IF cnt >= 56 THEN
      IF (need_init) THEN
        seed(TO_CHAR(SYSDATE, 'MM-DD-YYYY HH24:MI:SS') || USER || USERENV('SESSIONID'));
      ELSE
        FOR i IN 1 .. 31 LOOP
          rand_val := mem(i + 24) + mem(i);
          mem(i) := TRUNC_RAND_VAL(rand_val);
        END LOOP;

        FOR i IN 32 .. 55 LOOP 
          rand_val := mem(i - 31) + mem(i);
          mem(i) := TRUNC_RAND_VAL(rand_val);
        END LOOP;
      END IF; 
      cnt := 1;
    END IF;
    RETURN mem(cnt);
  END value;

  FUNCTION value(low IN NUMBER, high IN NUMBER) RETURN NUMBER IS
  BEGIN
   RETURN low + (value() * (high - low));
  END value;

  FUNCTION string (opt IN CHAR, len IN NUMBER)
    RETURN VARCHAR2 IS
    upper_alphabet VARCHAR(128) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    lower_alphabat VARCHAR(128) := 'abcdefghijklmnopqrstuvwxyz';
    number_map     VARCHAR(128) := '0123456789';
    printable_c_map VARCHAR(128) := ' !"$%&''()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~' ;
    opt_low   CHAR (1) := lower(opt);
    map_len       NUMBER;
    n         INTEGER;
    char_map  VARCHAR2 (128);
    res       VARCHAR2 (4000) := NULL;
  BEGIN
    CASE opt_low
      WHEN 'u' THEN
        char_map := upper_alphabet;
        map_len := 26;
      WHEN 'l' THEN
        char_map := lower_alphabat;
        map_len := 26;
      WHEN 'a' THEN
        char_map := upper_alphabet || lower_alphabat;
        map_len := 52;
      WHEN 'x' THEN
        char_map := number_map || upper_alphabet;
        map_len := 36;
      WHEN 'p' THEN
        char_map := printable_c_map;
        map_len := 95;
      ELSE
        char_map := upper_alphabet;
        map_len := 26;
      END CASE;

    IF len > 0 THEN
      FOR i IN 1 .. least(len, 4000) LOOP
        n := 1 + TRUNC(map_len * value());
        res := res || SUBSTR(char_map, n, 1);
      END LOOP;
    ELSE
      res := NULL;
    END IF;
    RETURN res;
  END string;

  FUNCTION normal RETURN NUMBER IS
  val_1          NUMBER;
  val_2          NUMBER;
  val_r          NUMBER;
  val_f         NUMBER;
  BEGIN
    IF norm_saved != 0 THEN         
      val_1 := norm_saved;
      norm_saved := 0;
    ELSE
      val_r := 2;
      WHILE val_r > 1 OR val_r = 0 LOOP
        val_1 := value() * 2 - 1;
        val_2 := value() * 2 - 1;
        val_r := val_1 * val_1 + val_2 * val_2;
      END LOOP;                            
      val_f      := sqrt((-2 * ln(val_r)) / val_r);
      val_1      := val_1 * val_f;
      norm_saved := val_2 * val_f;
    END IF;
    RETURN val_1;
  END normal;

  PROCEDURE initialize(val IN BINARY_INTEGER) IS
  BEGIN
    seed(TO_CHAR(val));
  END initialize;

  FUNCTION random RETURN BINARY_INTEGER IS
  BEGIN
    RETURN TRUNC(value() * POWER(2,32)) - POWER(2,31);
  END random;

END dbms_random;
//
