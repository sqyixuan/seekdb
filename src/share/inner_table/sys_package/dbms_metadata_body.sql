#package_name:dbms_metadata_body
#author: guangang.gg
#normalizer: linlin.xll
-- !!从tenant_virtual_object_definition获取object定义时，执行的sql中需要包含六个主键过滤条件
-- 且顺序与主键定义的顺序一致，顺序为object_type, object_name, schema, version, model, transform
CREATE OR REPLACE PACKAGE BODY dbms_metadata AS

  FUNCTION show_create_table_or_view(
    object_type VARCHAR,
    name VARCHAR,
    ob_schema VARCHAR)
  RETURN CLOB
  IS
    ddl_stmt CLOB DEFAULT '';
    obj_sql VARCHAR(32767)
      DEFAULT 'SELECT object_id FROM all_objects WHERE OWNER = ''' || ob_schema
              || ''' and object_name = ''' || name || ''' AND object_type = ''' || object_type || '''';
    show_create_table_sql VARCHAR(32767)
      DEFAULT 'SELECT create_table FROM SYS.TENANT_VIRTUAL_SHOW_CREATE_TABLE WHERE table_id = ?';
    obj_id INTEGER;
  BEGIN
    EXECUTE IMMEDIATE obj_sql INTO obj_id;
    EXECUTE IMMEDIATE show_create_table_sql INTO ddl_stmt USING obj_id;
    RETURN ddl_stmt;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      DBMS_SYS_ERROR.RAISE_SYSTEM_ERROR(OBJECT_NOT_FOUND_NUM,
                                        name, object_type, ob_schema);
  END;

  FUNCTION get_object_def(
    object_type INT,
    name VARCHAR,
    ob_schema VARCHAR,
    version VARCHAR,
    model VARCHAR,
    transform VARCHAR)
  RETURN CLOB
  IS
    ddl_stmt CLOB DEFAULT '';
    get_obj_def_sql VARCHAR(32767);
  BEGIN
    IF ob_schema IS NULL THEN
      get_obj_def_sql := 'SELECT definition FROM SYS.TENANT_VIRTUAL_OBJECT_DEFINITION WHERE object_type = '''
          || object_type || ''' and object_name = ''' || name || ''' and schema is null and version = '''
          || version || ''' and model = ''' || model || ''' and transform = ''' || transform || '''';
    ELSE
      get_obj_def_sql := 'SELECT definition FROM SYS.TENANT_VIRTUAL_OBJECT_DEFINITION WHERE object_type = '''
          || object_type || ''' and object_name = ''' || name || ''' and schema = '''
          || ob_schema || ''' and version = ''' || version || ''' and model = '''
          || model || ''' and transform = ''' || transform || '''';
    END IF;
    EXECUTE IMMEDIATE get_obj_def_sql INTO ddl_stmt;
    RETURN ddl_stmt;
  END;

  FUNCTION get_ddl (
    object_type     VARCHAR,
    name            VARCHAR,
    ob_schema       VARCHAR DEFAULT NULL,
    version         VARCHAR DEFAULT 'COMPATIBLE',
    model           VARCHAR DEFAULT 'ORACLE',
    transform       VARCHAR DEFAULT 'DDL')
  RETURN CLOB
  IS
    ddl_stmt VARCHAR(32767) default '';
    real_schema VARCHAR(32767);
    real_version VARCHAR(32767);
    real_model VARCHAR(32767);
    real_transform VARCHAR(32767);
  BEGIN
    -- case sensitive in object type, name, ob_schema, do not transform it;

    IF ob_schema IS NOT NULL and (object_type = 'TABLESPACE' OR object_type = 'USER') THEN
      DBMS_SYS_ERROR.RAISE_SYSTEM_ERROR(invalid_argval_num,
                                        ob_schema, 'SCHEMA', 'GET_DDL');
    ELSIF ob_schema IS NULL THEN
      real_schema := SYS_CONTEXT('USERENV','CURRENT_USER');
    ELSE 
      real_schema := ob_schema;
    END IF;

    IF name IS NULL THEN
      DBMS_SYS_ERROR.RAISE_SYSTEM_ERROR(invalid_argval_num, 'NULL', 'NAME', 'GET_DDL');
    ELSIF version != 'COMPATIBLE' THEN
      DBMS_SYS_ERROR.RAISE_SYSTEM_ERROR(invalid_argval_num, version, 'VERSION', 'GET_DDL');
    ELSIF model != 'ORACLE' THEN
      DBMS_SYS_ERROR.RAISE_SYSTEM_ERROR(invalid_argval_num, model, 'MODEL', 'GET_DDL');
    ELSIF transform != 'DDL' THEN
      DBMS_SYS_ERROR.RAISE_SYSTEM_ERROR(invalid_argval_num, transform, 'TRANSFORM', 'GET_DDL');
    END IF;

    IF version IS NULL THEN
      real_version := 'COMPATIBLE';
    ELSE
      real_version := version;
    END IF;
    IF model IS NULL THEN
      real_model := 'ORACLE';
    ELSE
      real_model := model;
    END IF;
    IF transform IS NULL THEN
      real_transform := 'DDL';
    ELSE
      real_transform := transform;
    END IF;

    CASE
      WHEN object_type = 'TABLE' OR object_type = 'VIEW' OR object_type = 'INDEX' THEN
        RETURN show_create_table_or_view(object_type, name, real_schema);
      WHEN object_type = 'PROCEDURE' THEN
        RETURN get_object_def(1, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'FUNCTION' THEN
        RETURN get_object_def(16, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'MATERIALIZED_VIEW' THEN
        RETURN show_create_table_or_view('MATERIALIZED VIEW', name, real_schema);
      WHEN object_type = 'PACKAGE' THEN
        RETURN get_object_def(2, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'PACKAGE SPEC' or object_type = 'PACKAGE_SPEC' THEN
        RETURN get_object_def(14, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'PACKAGE BODY' or object_type = 'PACKAGE_BODY' THEN
        RETURN get_object_def(15, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'CONSTRAINT' THEN
        RETURN get_object_def(3, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'REF_CONSTRAINT' THEN
        RETURN get_object_def(4, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'TABLESPACE' THEN
        RETURN get_object_def(5, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'SEQUENCE' THEN
        RETURN get_object_def(6, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'TRIGGER' THEN
        RETURN get_object_def(7, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'USER' THEN
        RETURN get_object_def(8, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'ROLE' THEN
        RETURN get_object_def(13, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'SYNONYM' THEN
        RETURN get_object_def(9, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'TYPE' THEN
        RETURN get_object_def(10, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'TYPE_SPEC' THEN
        RETURN get_object_def(11, name, real_schema, real_version, real_model, real_transform);
      WHEN object_type = 'TYPE_BODY' THEN
        RETURN get_object_def(12, name, real_schema, real_version, real_model, real_transform);
      ELSE
        DBMS_SYS_ERROR.RAISE_SYSTEM_ERROR(invalid_argval_num,
                                        object_type, 'OBJECT_TYPE', 'GET_DDL');
    END CASE;
  END;
END DBMS_METADATA;
//
