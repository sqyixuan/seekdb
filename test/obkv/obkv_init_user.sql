use test;
create user if not exists 'test' IDENTIFIED BY '';
grant all on *.* to 'test' WITH GRANT OPTION;
