
create table t1 (c1 int, c2 int, c3 int);
insert into t1 select level,level,level from dual connect by level <= 10;
create table t2 (d1 int, d2 int, d3 int);
insert into t2 select level,level,level from dual connect by level <= 10;

delimiter //;
BEGIN
  DBMS_RLS.ADD_POLICY(
    object_name     => 't1',
    policy_name     => 'p_simple',
    policy_function => 'f_static'
  );
END;
//

BEGIN
  DBMS_RLS.ADD_POLICY(
    object_name     => 't2',
    policy_name     => 'p_simple',
    policy_function => 'f_static',
    statement_types => 'select,insert,index',
    update_check    => true,
    static_policy   => true
  );
END;
//

BEGIN
  DBMS_RLS.ADD_POLICY(
    object_name     => 't2',
    policy_name     => 'p_default1',
    policy_function => 'f_static',
    update_check    => true,
    static_policy   => true
  );
END;
//

BEGIN
  DBMS_RLS.ADD_POLICY(
    object_name     => 't1',
    policy_name     => 'p_row1',
    policy_function => 'f_static',
    sec_relevant_cols => 'c1,c2',
    sec_relevant_cols_opt => dbms_rls.ALL_ROWS,
    enable          => false
  );
END;
//

BEGIN
  DBMS_RLS.ADD_POLICY(
    object_name     => 't1',
    policy_name     => 'p_default2',
    policy_function => 'f_static',
    sec_relevant_cols => 'c1,c2',
    sec_relevant_cols_opt => dbms_rls.ALL_ROWS,
    enable          => false,
    update_check    => true
  );
END;
//

BEGIN
  DBMS_RLS.ADD_POLICY(
    object_name     => 't2',
    policy_name     => 'p_row2',
    policy_function => 'f_static',
    sec_relevant_cols => 'd3',
    long_predicate  => true
  );
END;
//

BEGIN
  DBMS_RLS.ADD_POLICY_CONTEXT(
    object_name     => 't1',
    namespace       => 'a',
    attribute       => 'b'
  );
END;
//

BEGIN
  DBMS_RLS.ADD_POLICY_CONTEXT(
    object_name     => 't1',
    namespace       => 'a',
    attribute       => 'c'
  );
END;
//

BEGIN
  DBMS_RLS.ADD_POLICY_CONTEXT(
    object_name     => 't2',
    namespace       => 'a',
    attribute       => 'b'
  );
END;
//

BEGIN
  DBMS_RLS.CREATE_POLICY_GROUP(
    object_name     => 't1',
    policy_group    => 'g_simple1'
  );
END;
//

BEGIN
  DBMS_RLS.CREATE_POLICY_GROUP(
    object_name     => 't1',
    policy_group    => 'g_simple2'
  );
END;
//

BEGIN
  DBMS_RLS.ADD_GROUPED_POLICY(
    object_name     => 't1',
    policy_group    => 'g_simple1',
    policy_name     => 'gp_simple',
    policy_function => 'f_static'
  );
END;
//

BEGIN
  DBMS_RLS.ADD_GROUPED_POLICY(
    object_name     => 't1',
    policy_group    => 'g_simple1',
    policy_name     => 'gp_row',
    policy_function => 'f_static',
    sec_relevant_cols => 'c2',
    static_policy   => true,
    policy_type     => DBMS_RLS.SHARED_STATIC
  );
END;
//

BEGIN
  DBMS_RLS.ADD_GROUPED_POLICY(
    object_name     => 't1',
    policy_group    => 'g_simple2',
    policy_name     => 'gp_simple',
    policy_function => 'f_static'
  );
END;
//
delimiter ;//
