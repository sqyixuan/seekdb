--disable_query_log
--disable_warnings
drop function if exists rand_string;
--enable_warnings
delimiter $$;
create function rand_string(n int) RETURNS varchar(10000)
BEGIN
DECLARE chars_str varchar(100)
DEFAULT 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
DECLARE return_str varchar(10000) default '';
DECLARE i int DEFAULT 0;
 while i<n DO
 set return_str=CONCAT(return_str,SUBSTR(chars_str,FLOOR(1+RAND()*52),1));
 set i=i+1;
 end while;
 return return_str;
END $$
create function rand_length() returns int
BEGIN
declare length int default 0;
set length = FLOOR(1+rand()*10000);
return length;
END $$
delimiter ;$$
--enable_query_log
