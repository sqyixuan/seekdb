
set foreign_key_checks=1;

insert into category(c_cat) values ('P-aa');
insert into category(c_cat) values ('P-cc');
insert into category(c_cat) values ('P-ee');
insert into category(c_cat) values ('P-gg');
insert into category(c_cat) values ('NP-BB');
insert into category(c_cat) values ('NP-CC');
insert into category(c_cat) values ('NP-DD');
insert into category(c_cat) values ('NP-EE');
insert into category(c_cat) values ('NP-FF');
insert into category(c_cat) values ('NP-GG');
insert into category(c_cat) values ('NP-AA-BI');
insert into category(c_cat) values ('NP-CC-BI');
insert into category(c_cat) values ('NP-DD-BI');
insert into category(c_cat) values ('NP-EE-BI');
insert into category(c_cat) values ('NP-FF-BI');
insert into category(c_cat) values ('NP-GG-BI');
insert into products(p_id, p_name, p_cat) values (1, 'P+aa', 'P-aa');
insert into products(p_id, p_name, p_cat) values (3, 'P+cc', 'P-cc');
insert into products(p_id, p_name, p_cat) values (5, 'P+ee', 'P-ee');
insert into products(p_id, p_name, p_cat) values (7, 'P+gg', 'P-gg');
insert into newproducts(np_id, np_name, np_cat) values (1, 'NP+AA', 'NP-AA');
insert into newproducts(np_id, np_name, np_cat) values (2, 'NP+BB', 'NP-BB');
insert into newproducts(np_id, np_name, np_cat) values (3, 'NP+CC', 'NP-CC');
insert into newproducts(np_id, np_name, np_cat) values (4, 'NP+DD', 'NP-DD');
insert into newproducts(np_id, np_name, np_cat) values (5, 'NP+EE', 'NP-EE');
insert into newproducts(np_id, np_name, np_cat) values (6, 'NP+FF', 'NP-FF');
insert into newproducts(np_id, np_name, np_cat) values (7, 'NP+GG', 'NP-GG');
commit;
select * from products order by 1;
select * from newproducts order by 1;

delimiter /;
create or replace trigger before_trigger
  before insert or update or delete
  on products
  for each row
begin
  if (INSERTING) then
    :new.p_cat := :new.p_cat || '-BI';
    insert into trg_log values (seq.nextval, :new.p_id, 'before', 'insert', NULL, :new.p_name);
  end if;
  if (UPDATING) then
    :new.p_name := :new.p_name || '+BU';
    insert into trg_log values (seq.nextval, :new.p_id, 'before', 'update', :old.p_name, :new.p_name);
  end if;
  if (DELETING) then
    insert into trg_log values (seq.nextval, :old.p_id, 'before', 'delete', :old.p_name, NULL);
  end if;
end;
/
create or replace trigger after_trigger
  after insert or update or delete
  on products
  for each row
begin
  if (INSERTING) then
    insert into trg_log values (seq.nextval, :new.p_id, 'after', 'insert', NULL, :new.p_name);
  end if;
  if (UPDATING) then
    insert into trg_log values (seq.nextval, :new.p_id, 'after', 'update', :old.p_name, :new.p_name);
  end if;
  if (DELETING) then
    insert into trg_log values (seq.nextval, :old.p_id, 'after', 'delete', :old.p_name, NULL);
  end if;
end;
/
delimiter ;/

--error 2291
MERGE INTO products p
USING newproducts np
  ON (p.p_id = np.np_id)
  WHEN MATCHED THEN
  UPDATE SET p.p_name = np.np_name, p.p_cat = np.np_cat
    WHERE p.p_cat != 'P-cc'
  DELETE
    WHERE p.p_name = 'NP+EE'
  WHEN NOT MATCHED THEN
  INSERT (p_id, p_name, p_cat) VALUES (np.np_id, np.np_name, np.np_cat)
    WHERE np.np_cat != 'P-bb'
;

insert into category(c_cat) values ('NP-AA');
insert into category(c_cat) values ('NP-BB-BI');
insert into orders(o_name) values ('P+cc');
insert into orders(o_name) values ('P+ee');
commit;

--error 2292
MERGE INTO products p
USING newproducts np
  ON (p.p_id = np.np_id)
  WHEN MATCHED THEN
  UPDATE SET p.p_name = np.np_name, p.p_cat = np.np_cat
    WHERE p.p_cat != 'P-cc'
  DELETE
    WHERE p.p_name = 'NP+EE'
  WHEN NOT MATCHED THEN
  INSERT (p_id, p_name, p_cat) VALUES (np.np_id, np.np_name, np.np_cat)
    WHERE np.np_cat != 'P-bb'
;

delete from orders;

--error 2290
MERGE INTO products p
USING newproducts np
  ON (p.p_id = np.np_id)
  WHEN MATCHED THEN
  UPDATE SET p.p_name = np.np_name, p.p_cat = np.np_cat
    WHERE p.p_cat = 'P-cc'
  DELETE
    WHERE p.p_name = 'NP+EE'
  WHEN NOT MATCHED THEN
  INSERT (p_id, p_name, p_cat) VALUES (np.np_id, np.np_name, np.np_cat)
    WHERE np.np_cat != 'P-bb'
;

MERGE INTO products p
USING newproducts np
  ON (p.p_id = np.np_id)
  WHEN MATCHED THEN
  UPDATE SET p.p_name = np.np_name, p.p_cat = np.np_cat
    WHERE p.p_cat != 'P-cc'
  DELETE
    WHERE p.p_name = 'NP+EE'
  WHEN NOT MATCHED THEN
  INSERT (p_id, p_name, p_cat) VALUES (np.np_id, np.np_name, np.np_cat)
    WHERE np.np_cat != 'P-bb'
;

# merge功能的生成列本来就有问题，下条语句的p_cat_len目前也是错误的，需要等修复生成列的问题后再更新。
select p_id, p_name, p_cat, p_cat_len from products order by 1;
select log_id, log_tp, log_ev, log_old, log_new from trg_log order by 1, 3, 2 desc;

