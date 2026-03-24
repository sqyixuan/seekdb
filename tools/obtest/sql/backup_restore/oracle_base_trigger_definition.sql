
create table employees (
  pk         int primary key,
  emp_id     int,
  emp_name   varchar2(64),
  dep_id     int,
  salary     decimal(10, 2)
);

CREATE OR REPLACE TRIGGER insert_update_trigger1 BEFORE INSERT OR UPDATE ON employees FOR EACH ROW WHEN (NEW.dep_id = 101) DECLARE emp_name varchar2(64); salary number; BEGIN if (INSERTING and :NEW.pk is NULL) then :NEW.pk := 10001; end if; emp_name := :NEW.emp_name; salary := :NEW.salary; if (INSERTING) then DBMS_OUTPUT.PUT_LINE('insert_trigger1 when INSERTING: ' || emp_name || ', salary: ' || salary); end if; if (UPDATING) then DBMS_OUTPUT.PUT_LINE('insert_trigger1 when UPDATING, do nothing'); end if; END;

