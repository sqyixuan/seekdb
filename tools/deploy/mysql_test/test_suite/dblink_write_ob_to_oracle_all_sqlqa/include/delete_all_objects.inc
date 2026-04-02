###owner: luocheng
###owner group: qa
###description: dblink udf
###tags: dblink write

delimiter /;

CREATE OR REPLACE PROCEDURE delete_all_objects AS
BEGIN
  -- 删除所有表
  FOR tbl IN (SELECT table_name FROM user_tables) LOOP
    EXECUTE IMMEDIATE 'DROP TABLE ' || tbl.table_name || ' CASCADE CONSTRAINTS');
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all table finish');
  
  -- 删除所有存储过程
  FOR proc IN (select distinct name from user_source where type=upper('procedure')) LOOP
  if proc.name != 'DELETE_ALL_OBJECTS' then  --不删除本procedure本身
    EXECUTE IMMEDIATE 'DROP PROCEDURE ' || proc.name;
  end if;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all procedure finish');
  
  -- 删除所有函数
  FOR func IN (select distinct name from user_source where type=upper('function')) LOOP
    if func.name != '"LUO"' then  --创建了"luo" 函数,不带双引号删不掉
    EXECUTE IMMEDIATE 'DROP FUNCTION ' || func.name;
    end if;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all function finish');

  -- 删除所有视图
  FOR v  IN (SELECT view_name FROM user_views) LOOP
    EXECUTE IMMEDIATE 'DROP VIEW ' || v.view_name;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all view finish');

  -- 删除所有同义词
  FOR synonym IN (SELECT synonym_name FROM user_synonyms) LOOP
    EXECUTE IMMEDIATE 'DROP SYNONYM ' || synonym.synonym_name;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all synonym finish');
  
  COMMIT;
END;
/

delimiter ;/