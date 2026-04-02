delimiter //;
DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_users where username='TPCH_1G_PART';
  IF (v1 = 0) then
    execute immediate 'CREATE USER TPCH_1G_PART IDENTIFIED BY TPCH_1G_PART';
    execute immediate 'GRANT ALL ON *.* TO TPCH_1G_PART';
  END IF;
END//;
