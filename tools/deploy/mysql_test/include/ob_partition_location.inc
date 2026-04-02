
eval explain basic insert into $ts values ('2016-1-1 1:1:1.011212', 1);
eval explain basic insert into $ts values ('2016-1-1 1:1:1.111212', 1);
eval explain basic insert into $ts values ('2016-1-1 1:1:1', 1);

eval insert into $ts values ('2016-1-1 1:1:1.011212', 1);
--error 1062
eval insert into $ts values ('2016-1-1 1:1:1.111212', 1);

eval explain basic select * from $ts where c1 = '2016-1-1 1:1:1.011212';
eval explain basic select * from $ts where c1 = '2016-1-1 1:1:1';

eval select * from $ts where c1 = '2016-1-1 1:1:1';
eval select * from $ts where c1 = '2016-1-1 1:1:1.011212';

eval explain basic insert into $tb values('a');
eval explain basic insert into $tb values();
eval explain basic insert into $tb values(unhex(6100000000));

eval insert into $tb values('a');
--error 1062
eval insert into $tb values();
--error 1062
eval insert into $tb values(unhex(6100000000));

eval explain basic select * from $tb where c1 = 'a';
eval explain basic select * from $tb where c1 = unhex(6100000000);
eval select * from $tb where c1 = 'a';
eval select * from $tb where c1 = unhex(6100000000);

eval update $tb set c1 = 'a' where c1 = unhex(6100000000);

eval explain insert into $tc values('a');
eval explain insert into $tc values('a    ');

eval insert into $tc values('a');
--error 1062
eval insert into $tc values('a    ');

eval explain select * from $tc where c1 = 'a';
eval explain select * from $tc where c1 = 'a    ';

eval select * from $tc where c1 = 'a';
eval select * from $tc where c1 = 'a    ';

#type and collation
eval explain select * from $tc where c1 = 'a' collate 'utf8mb4_bin';
eval explain select * from $tc where c1 = 'A' collate 'utf8mb4_bin';
eval select * from $tc where c1 = 'a' collate 'utf8mb4_bin';
eval select * from $tc where c1 = 'A' collate 'utf8mb4_bin';
