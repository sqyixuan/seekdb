
CREATE OR REPLACE TRIGGER update_trigger1 BEFORE UPDATE ON employees FOR EACH ROW WHEN (NEW.dep_id = 101) BEGIN DBMS_OUTPUT.PUT_LINE('update_trigger1: ' || :NEW.emp_name); if (:NEW.salary > 20000) then :NEW.salary := 20000; end if; END;

CREATE OR REPLACE TRIGGER delete_trigger1 BEFORE DELETE ON employees FOR EACH ROW WHEN (OLD.dep_id = 101) BEGIN DBMS_OUTPUT.PUT_LINE('delete_trigger1: ' || :OLD.emp_name); END;

