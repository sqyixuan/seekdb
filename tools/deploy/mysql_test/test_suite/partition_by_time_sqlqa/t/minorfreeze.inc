connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;
alter system minor freeze tenant = sys;
alter system minor freeze tenant = all_user;
alter system minor freeze tenant = all_meta;
disconnect obsys;
connection default;
