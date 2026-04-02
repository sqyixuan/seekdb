create tablegroup TYPE_GROUP;
create table type_t2(int_c int,                 
                    integer_c INTEGER,
                    char_c CHAR(100),
                    varchar_c VARCHAR(100),
                    date_c DATE,
                    time_c TIMESTAMP,
                    raw_c raw(10),
                    timezone_c1 timestamp(6) with time zone,
                    timezone_c2 timestamp(6) with local time zone,
                    interval_year_c interval year(9) to month,
                    interval_day_c interval day(9) to second
)  tablegroup='TYPE_GROUP';

insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values ('', 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (NULL, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, '', 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, NULL, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, '', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, NULL, 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', '', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', NULL, date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', '', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', NULL, timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', '', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', NULL, hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', '', timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', NULL, timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), '', timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12')
;insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), NULL, timestamp '2019-09-16 16:31:02', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', '', '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', NULL, '19-11', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '', '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', NULL, '12 12:12:12');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', '');
insert into type_t2 values (1, 1, 'normal_c', 'normal_vc', date '2019-09-16', timestamp '2019-09-16 16:22:05', hextoraw('0A'), timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai', timestamp '2019-09-16 16:31:02', '19-11', NULL);

select * from type_t2 where int_c = 1;
select * from type_t2 where int_c = 1;
select * from type_t2 where int_c = NULL;
select * from type_t2 where int_c = '';
select * from type_t2 where int_c = '1';
select * from type_t2 where int_c = '1';

select * from type_t2 where integer_c = 1;
select * from type_t2 where integer_c = 1;
select * from type_t2 where integer_c = NULL;
select * from type_t2 where integer_c = '';
select * from type_t2 where integer_c = '1';
select * from type_t2 where integer_c = '1';

select * from type_t2 where char_c = NULL;
select * from type_t2 where char_c = 'normal_c';
select * from type_t2 where char_c = 'normal_c';
select * from type_t2 where char_c = '';

select * from type_t2 where varchar_c = NULL;
select * from type_t2 where varchar_c = 'normal_vc';
select * from type_t2 where varchar_c = 'normal_vc';
select * from type_t2 where varchar_c = '';

select * from type_t2 where date_c = date '2019-09-16';
select * from type_t2 where date_c = date '2019-09-16';
select * from type_t2 where date_c = NULL;
select * from type_t2 where date_c = '';
select * from type_t2 where date_c = '2019-09-16';
select * from type_t2 where date_c = '2019-09-16';

select * from type_t2 where time_c = timestamp '2019-09-16 16:22:05';
select * from type_t2 where time_c = timestamp '2019-09-16 16:22:05';
select * from type_t2 where time_c = NULL;
select * from type_t2 where time_c = '';
select * from type_t2 where time_c = '2019-09-16 16:22:05';
select * from type_t2 where time_c = '2019-09-16 16:22:05';

select * from type_t2 where raw_c = hextoraw('0A');
select * from type_t2 where raw_c = hextoraw('0A');
select * from type_t2 where raw_c = NULL;
select * from type_t2 where raw_c = '';

select * from type_t2 where timezone_c1 = timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai';
select * from type_t2 where timezone_c1 = timestamp '2019-09-16 16:31:02.123456789 Asia/Shanghai';
select * from type_t2 where timezone_c1 = NULL;
select * from type_t2 where timezone_c1 = '';
select * from type_t2 where timezone_c1 = '2019-09-16 16:31:02.123456789 Asia/Shanghai';
select * from type_t2 where timezone_c1 = '2019-09-16 16:31:02.123456789 Asia/Shanghai';

select * from type_t2 where timezone_c2 = timestamp '2019-09-16 16:31:02';
select * from type_t2 where timezone_c2 = timestamp '2019-09-16 16:31:02';
select * from type_t2 where timezone_c2 = NULL;
select * from type_t2 where timezone_c2 = '';
select * from type_t2 where timezone_c2 = '2019-09-16 16:31:02';
select * from type_t2 where timezone_c2 = '2019-09-16 16:31:02';

select * from type_t2 where interval_year_c = NULL;
select * from type_t2 where interval_year_c = '19-11';
select * from type_t2 where interval_year_c = '19-11';
select * from type_t2 where interval_year_c = '';

select * from type_t2 where interval_day_c = NULL;
select * from type_t2 where interval_day_c = '12 12:12:12';
select * from type_t2 where interval_day_c = '12 12:12:12';
select * from type_t2 where interval_day_c = '';


