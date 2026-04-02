--echo ob_nls_initcap_1
#nls_initcap一个参数
select nls_initcap('asd') from dual;
select nls_initcap('Asd') from dual;
select nls_initcap('ASD') from dual;

#nls_initcap 第一个字符不是字母
select nls_initcap(' asd') from dual;
select nls_initcap('#asd') from dual;
select nls_initcap('中asd') from dual;
select nls_initcap('12asd') from dual;
select nls_initcap('a12sd') from dual;
select nls_initcap('a1s2d') from dual;

SELECT nls_initcap(' ASD') FROM DUAL;
SELECT nls_initcap('#ASD') FROM DUAL;
SELECT nls_initcap('中ASD') FROM DUAL;
SELECT nls_initcap('12ASD') FROM DUAL;
SELECT nls_initcap('A12SD') FROM DUAL;
SELECT nls_initcap('A1S2D') FROM DUAL;

#nls_initcap 空格或非字母数字字符分隔

select nls_initcap('#a#b#c#d#e#f#g') from dual;
select nls_initcap(' a b c d e f g') from dual;
select nls_initcap('a,b,c,d,e,f,g') from dual;
select nls_initcap('ab0cd4efg6') from dual;
select nls_initcap('1a2b3c4d5e6f7g') from dual;
select nls_initcap('1A2B3C4D5E6F7G') from dual;

select nls_initcap('a1b2c3d4e5fg6') from dual;
select nls_initcap('a1B2C3D4E5FG6') from dual;

select nls_initcap('a，b，c，d，e，f，g') from dual;
select nls_initcap('ab,bc,cd,df,eg,fh,gj') from dual;
select nls_initcap('Ab,Bc,Cd,Df,Eg,Fh,Gj') from dual;
select nls_initcap(',ab,bc,cd,df,eg,fh,gj') from dual;
select nls_initcap('中a中b中c中d中e中f中g') from dual;
select nls_initcap('a中b中c中d中e中f中g中') from dual;
select nls_initcap(' #a #b #c #d #e #f #g') from dual;
select nls_initcap('中asd') from dual;
--echo #ob只支持nls_sort=binary
#select nls_initcap('ab cde'),nls_initcap('a c h d e','nls_sort= SCHINESE_RADICAL_M') from dual;
#select nls_initcap('ab cde'),nls_initcap('a c h d e','nls_sort= SCHINESE_STROKE_M') from dual;
#select nls_initcap('ab cde'),nls_initcap('a c h d e','nls_sort= SCHINESE_PINYIN_M') from dual;
select nls_initcap('ab cde'),nls_initcap('d a c h d e','nls_sort= binary') from dual;


#nls_initcap 其他字符

select Initcap('áǎàā') from dual;
SELECT INITCAP('ÁǍÀĀ') FROM DUAL;

--echo #49368616- [initcap] 无法判断出拼音字母属于字母/数字
select nls_initcap('āáǎà') from dual;
select nls_initcap('АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ') from dual;
--echo #49368616- [initcap] 无法判断出拼音字母属于字母/数字
select nls_initcap('α β γ δ ε ζ η θ ι κ λ μ ν ξ ο π ρ σ τ υ φ χ ψ ω') from dual;
--echo #49368616- [initcap] 无法判断出拼音字母属于字母/数字
select nls_initcap('а б в г д е ё ж з и й к л м н о п р с т у ф х ц ч ш щ ъ ы ь э ю я') from dual;
--echo #49368616- [initcap] 无法判断出拼音字母属于字母/数字
select nls_initcap('ā á ǎ à、ō ó ǒ ò、ê ē é ě èī í ǐ ì、ū ú ǔ ù、ǖ ǘ ǚ ǜ ü') from dual;


#nls_initcap nls_sort = binary
select nls_initcap('Asd','nls_sort = binary') from dual;
select nls_initcap('asd','nls_sort = binary') from dual;
select nls_initcap(' asd','nls_sort = binary') from dual;
select nls_initcap('中asd','nls_sort = binary') from dual;
select nls_initcap(' asd ','nls_sort = binary') from dual;
select nls_initcap('#asd ','nls_sort = binary') from dual;
select nls_initcap('#a#b#c#d#e#f#g','nls_sort = binary') from dual;
select nls_initcap(' a b c d e f g','nls_sort = binary') from dual;
select nls_initcap('a,b,c,d,e,f,g','nls_sort = binary') from dual;
select nls_initcap('ab0cd4efg6','nls_sort = binary') from dual;
select nls_initcap('1a2b3c4d5e6f7g','nls_sort = binary') from dual;
select nls_initcap('a1b2c3d4e5fg6','nls_sort = binary') from dual;
select nls_initcap('a，b，c，d，e，f，g','nls_sort = binary') from dual;
select nls_initcap('ab,bc,cd,df,eg,fh,gj','nls_sort = binary') from dual;
select nls_initcap('Ab,Bc,Cd,Df,Eg,Fh,Gj','nls_sort = binary') from dual;
select nls_initcap(',ab,bc,cd,df,eg,fh,gj','nls_sort = binary') from dual;
select nls_initcap('中a中b中c中d中e中f中g','nls_sort = binary') from dual;
select nls_initcap('a中b中c中d中e中f中g中','nls_sort = binary') from dual;
select nls_initcap(' #a #b #c #d #e #f #g','nls_sort = binary') from dual;

#--echo #ob只支持nls_sort = binary
#select nls_initcap('ab cde'),nls_initcap('a c h d e','nls_sort= SCHINESE_RADICAL_M') from dual;
#select nls_initcap('ab cde'),nls_initcap('a c h d e','nls_sort= SCHINESE_STROKE_M') from dual;
#select nls_initcap('ab cde'),nls_initcap('a c h d e','nls_sort= SCHINESE_PINYIN_M') from dual;

select nls_initcap('acb-fgb-ppq') from dual;


--echo ob_nls_initcap_2
--echo ob_nls_initcap_2_1
--echo #char 类型 char
--error 0,942
drop table t1;
create table t1(c0 char(50 char),id number);
insert into t1 values(null,1);
insert into t1 values('123',2);
insert into t1 values('null',3);
insert into t1 values('中文',4);
insert into t1 values('A b C D e',5);
insert into t1 values('AC,bA,cc,dR,Fe',6);
insert into t1 values('JEllyFish',7);
insert into t1 values('Ac B1d c7q',8);
insert into t1 values('acb-fgb-ppq',9);
insert into t1 values('aA,Bb,cc,Dd,EE',10);
insert into t1 values('Ab,Bb,cC,dB,eB',11);
insert into t1 values('中文a中国B语言c',12);
insert into t1 values('中文A中国B语言C',13);
insert into t1 values('ChénG zHǔ míNG sHí',14);
insert into t1 values('2018aBc-10fGH-12Wwe',15);
insert into t1 values('BeiJing LonDon New York',16);
insert into t1 values('aPPle baNANA CHerry lEMon',17);
insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;

--echo #char 类型 byte 
--error 0,942
drop table t1;
create table t1(c0 char(100 byte),id number);
desc t1;
select lengthb('中文a中国B语言c') from dual;
insert into t1 values(null,1);
insert into t1 values('123',2);
insert into t1 values('null',3);
insert into t1 values('中文',4);
insert into t1 values('A b C D e',5);
insert into t1 values('AC,bA,cc,dR,Fe',6);
insert into t1 values('JEllyFish',7);
insert into t1 values('Ac B1d c7q',8);
insert into t1 values('acb-fgb-ppq',9);
insert into t1 values('aA,Bb,cc,Dd,EE',10);
insert into t1 values('Ab,Bb,cC,dB,eB',11);
insert into t1 values('中文a中国B语言c',12);
insert into t1 values('中文A中国B语言C',13);
insert into t1 values('ChénG zHǔ míNG sHí',14);
insert into t1 values('2018aBc-10fGH-12Wwe',15);
insert into t1 values('BeiJing LonDon New York',16);
insert into t1 values('aPPle baNANA CHerry lEMon',17);
insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;


--echo ob_nls_initcap_2_2
--echo #varchar2 类型 char

--error 0,942
drop table t1;
create table t1(c0 varchar2(50 char),id number);
insert into t1 values(null,1);
insert into t1 values('123',2);
insert into t1 values('null',3);
insert into t1 values('中文',4);
insert into t1 values('A b C D e',5);
insert into t1 values('AC,bA,cc,dR,Fe',6);
insert into t1 values('JEllyFish',7);
insert into t1 values('Ac B1d c7q',8);
insert into t1 values('acb-fgb-ppq',9);
insert into t1 values('aA,Bb,cc,Dd,EE',10);
insert into t1 values('Ab,Bb,cC,dB,eB',11);
insert into t1 values('中文a中国B语言c',12);
insert into t1 values('中文A中国B语言C',13);
insert into t1 values('ChénG zHǔ míNG sHí',14);
insert into t1 values('2018aBc-10fGH-12Wwe',15);
insert into t1 values('BeiJing LonDon New York',16);
insert into t1 values('aPPle baNANA CHerry lEMon',17);
insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;

--echo #varchar2 类型 byte 
--error 0,942
drop table t1;
create table t1(c0 varchar2(100 byte),id number);
insert into t1 values(null,1);
insert into t1 values('123',2);
insert into t1 values('null',3);
insert into t1 values('中文',4);
insert into t1 values('A b C D e',5);
insert into t1 values('AC,bA,cc,dR,Fe',6);
insert into t1 values('JEllyFish',7);
insert into t1 values('Ac B1d c7q',8);
insert into t1 values('acb-fgb-ppq',9);
insert into t1 values('aA,Bb,cc,Dd,EE',10);
insert into t1 values('Ab,Bb,cC,dB,eB',11);
insert into t1 values('中文a中国B语言c',12);
insert into t1 values('中文A中国B语言C',13);
insert into t1 values('ChénG zHǔ míNG sHí',14);
insert into t1 values('2018aBc-10fGH-12Wwe',15);
insert into t1 values('BeiJing LonDon New York',16);
insert into t1 values('aPPle baNANA CHerry lEMon',17);
insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;



--echo ob_nls_initcap_2_3
--echo #nchar 类型 char

#--error 0,942
#drop table t1;
#create table t1(c0 nchar(50 char),id number);
#insert into t1 values(null,1);
#insert into t1 values('123',2);
#insert into t1 values('null',3);
#insert into t1 values('中文',4);
#insert into t1 values('A b C D e',5);
#insert into t1 values('AC,bA,cc,dR,Fe',6);
#insert into t1 values('JEllyFish',7);
#insert into t1 values('Ac B1d c7q',8);
#insert into t1 values('acb-fgb-ppq',9);
#insert into t1 values('aA,Bb,cc,Dd,EE',10);
#insert into t1 values('Ab,Bb,cC,dB,eB',11);
#insert into t1 values('中文a中国B语言c',12);
#insert into t1 values('中文A中国B语言C',13);
#insert into t1 values('ChénG zHǔ míNG sHí',14);
#insert into t1 values('2018aBc-10fGH-12Wwe',15);
#insert into t1 values('BeiJing LonDon New York',16);
#insert into t1 values('aPPle baNANA CHerry lEMon',17);
#insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

#select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

#--echo #CTAS
#--error 0,942
#drop table t2;
#create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
#desc t2;
#select * from t2 order by 1,2;


#--error 0,942
#drop table t1;
#create table t1(c0 nchar(50 char),id number);
#insert into t1 values(null,1);
#insert into t1 values('123',2);
#insert into t1 values('null',3);
#insert into t1 values('中文',4);
#insert into t1 values('A b C D e',5);
#insert into t1 values('AC,bA,cc,dR,Fe',6);
#insert into t1 values('JEllyFish',7);
#insert into t1 values('Ac B1d c7q',8);
#insert into t1 values('acb-fgb-ppq',9);
#insert into t1 values('aA,Bb,cc,Dd,EE',10);
#insert into t1 values('Ab,Bb,cC,dB,eB',11);
#insert into t1 values('中文a中国B语言c',12);
#insert into t1 values('中文A中国B语言C',13);
#insert into t1 values('ChénG zHǔ míNG sHí',14);
#insert into t1 values('2018aBc-10fGH-12Wwe',15);
#insert into t1 values('BeiJing LonDon New York',16);
#insert into t1 values('aPPle baNANA CHerry lEMon',17);
#insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

#select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

#--echo #CTAS
#--error 0,942
#drop table t2;
#create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
#desc t2;
#select * from t2 order by 1,2;

#--echo #nchar 类型 byte 
#--error 0,942
#drop table t1;
#create table t1(c0 nchar(50 byte),id number);
#insert into t1 values(null,1);
#insert into t1 values('123',2);
#insert into t1 values('null',3);
#insert into t1 values('中文',4);
#insert into t1 values('A b C D e',5);
#insert into t1 values('AC,bA,cc,dR,Fe',6);
#insert into t1 values('JEllyFish',7);
#insert into t1 values('Ac B1d c7q',8);
#insert into t1 values('acb-fgb-ppq',9);
#insert into t1 values('aA,Bb,cc,Dd,EE',10);
#insert into t1 values('Ab,Bb,cC,dB,eB',11);
#insert into t1 values('中文a中国B语言c',12);
#insert into t1 values('中文A中国B语言C',13);
#insert into t1 values('ChénG zHǔ míNG sHí',14);
#insert into t1 values('2018aBc-10fGH-12Wwe',15);
#insert into t1 values('BeiJing LonDon New York',16);
#insert into t1 values('aPPle baNANA CHerry lEMon',17);
#insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

#select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

#--echo #CTAS
#--error 0,942
#drop table t2;
#create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
#desc t2;
#select * from t2 order by 1,2;

--echo #nchar 类型 
--error 0,942
drop table t1;
create table t1(c0 nchar(50),id number);
insert into t1 values(null,1);
insert into t1 values('123',2);
insert into t1 values('null',3);
insert into t1 values('中文',4);
insert into t1 values('A b C D e',5);
insert into t1 values('AC,bA,cc,dR,Fe',6);
insert into t1 values('JEllyFish',7);
insert into t1 values('Ac B1d c7q',8);
insert into t1 values('acb-fgb-ppq',9);
insert into t1 values('aA,Bb,cc,Dd,EE',10);
insert into t1 values('Ab,Bb,cC,dB,eB',11);
insert into t1 values('中文a中国B语言c',12);
insert into t1 values('中文A中国B语言C',13);
insert into t1 values('ChénG zHǔ míNG sHí',14);
insert into t1 values('2018aBc-10fGH-12Wwe',15);
insert into t1 values('BeiJing LonDon New York',16);
insert into t1 values('aPPle baNANA CHerry lEMon',17);
insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;

--echo ob_nls_initcap_2_4
--echo #nvarchar2 类型 char

--error 0,942
drop table t1;
create table t1(c0 nvarchar2(50),id number);
insert into t1 values(null,1);
insert into t1 values('123',2);
insert into t1 values('null',3);
insert into t1 values('中文',4);
insert into t1 values('A b C D e',5);
insert into t1 values('AC,bA,cc,dR,Fe',6);
insert into t1 values('JEllyFish',7);
insert into t1 values('Ac B1d c7q',8);
insert into t1 values('acb-fgb-ppq',9);
insert into t1 values('aA,Bb,cc,Dd,EE',10);
insert into t1 values('Ab,Bb,cC,dB,eB',11);
insert into t1 values('中文a中国B语言c',12);
insert into t1 values('中文A中国B语言C',13);
insert into t1 values('ChénG zHǔ míNG sHí',14);
insert into t1 values('2018aBc-10fGH-12Wwe',15);
insert into t1 values('BeiJing LonDon New York',16);
insert into t1 values('aPPle baNANA CHerry lEMon',17);
insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;


#--error 0,942
#drop table t1;
#create table t1(c0 nvarchar2(50 char),id number);
#insert into t1 values(null,1);
#insert into t1 values('123',2);
#insert into t1 values('null',3);
#insert into t1 values('中文',4);
#insert into t1 values('A b C D e',5);
#insert into t1 values('AC,bA,cc,dR,Fe',6);
#insert into t1 values('JEllyFish',7);
#insert into t1 values('Ac B1d c7q',8);
#insert into t1 values('acb-fgb-ppq',9);
#insert into t1 values('aA,Bb,cc,Dd,EE',10);
#insert into t1 values('Ab,Bb,cC,dB,eB',11);
#insert into t1 values('中文a中国B语言c',12);
#insert into t1 values('中文A中国B语言C',13);
#insert into t1 values('ChénG zHǔ míNG sHí',14);
#insert into t1 values('2018aBc-10fGH-12Wwe',15);
#insert into t1 values('BeiJing LonDon New York',16);
#insert into t1 values('aPPle baNANA CHerry lEMon',17);
#insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

#select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

#--echo #CTAS
#--error 0,942
#drop table t2;
#create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
#desc t2;
#select * from t2 order by 1,2;

#--echo #nvarchar2 类型 byte 
#--error 0,942
#drop table t1;
#create table t1(c0 nvarchar2(50 byte),id number);
#insert into t1 values(null,1);
#insert into t1 values('123',2);
#insert into t1 values('null',3);
#insert into t1 values('中文',4);
#insert into t1 values('A b C D e',5);
#insert into t1 values('AC,bA,cc,dR,Fe',6);
#insert into t1 values('JEllyFish',7);
#insert into t1 values('Ac B1d c7q',8);
#insert into t1 values('acb-fgb-ppq',9);
#insert into t1 values('aA,Bb,cc,Dd,EE',10);
#insert into t1 values('Ab,Bb,cC,dB,eB',11);
#insert into t1 values('中文a中国B语言c',12);
#insert into t1 values('中文A中国B语言C',13);
#insert into t1 values('ChénG zHǔ míNG sHí',14);
#insert into t1 values('2018aBc-10fGH-12Wwe',15);
#insert into t1 values('BeiJing LonDon New York',16);
#insert into t1 values('aPPle baNANA CHerry lEMon',17);
#insert into t1 values('choNGQing QINGdao  guangZHOU naNJing hanGZhou',18);

#select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

#--echo #CTAS
#--error 0,942
#drop table t2;
#create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
#desc t2;
#select * from t2 order by 1,2;


--echo ob_nls_initcap_2_5
--echo #date 类型

drop table t1;
create table t1(c0 date,id number);
insert into t1 values (null, 0);
insert into t1 values (to_date('2022-01-01','yyyy-mm-dd'), 1);
insert into t1 values (to_date('2022-01-02','yyyy-mm-dd'), 2);
insert into t1 values (to_date('2022-01-03','yyyy-mm-dd'), 3);
insert into t1 values (to_date('2022-01-04','yyyy-mm-dd'), 4);
insert into t1 values (to_date('2022-01-05','yyyy-mm-dd'), 5);
insert into t1 values (to_date('2022-01-06','yyyy-mm-dd'), 6);
insert into t1 values (to_date('2022-01-07','yyyy-mm-dd'), 7);
insert into t1 values (to_date('2022-01-08','yyyy-mm-dd'), 8);
insert into t1 values (to_date('2022-01-09','yyyy-mm-dd'), 9);
insert into t1 values (to_date('2022-01-10','yyyy-mm-dd'), 10);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;

--echo ob_nls_initcap_2_6
--echo #timestamp

drop table t1;
create table t1(c0 timestamp,id number);
insert into t1 (c0, id) values (null, 0);
insert into t1 (c0, id) values (to_timestamp('2022-01-01 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 1);
insert into t1 (c0, id) values (to_timestamp('2022-01-02 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 2);
insert into t1 (c0, id) values (to_timestamp('2022-01-03 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 3);
insert into t1 (c0, id) values (to_timestamp('2022-01-04 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 4);
insert into t1 (c0, id) values (to_timestamp('2022-01-05 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 5);
insert into t1 (c0, id) values (to_timestamp('2022-01-06 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 6);
insert into t1 (c0, id) values (to_timestamp('2022-01-07 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 7);
insert into t1 (c0, id) values (to_timestamp('2022-01-08 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 8);
insert into t1 (c0, id) values (to_timestamp('2022-01-09 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 9);
insert into t1 (c0, id) values (to_timestamp('2022-01-10 12:00:00', 'yyyy-mm-dd hh24:mi:ss'), 10);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;


--echo ob_nls_initcap_2_7
--echo #timestamp with timezone

drop table t1;
create table t1(c0 timestamp with time zone,id number);
insert into t1 (c0, id) values (null, 0);
INSERT INTO t1 (c0, id) VALUES ('2022-01-01 00:00:00+00:00', 1);
INSERT INTO t1 (c0, id) VALUES ('2022-01-02 00:00:00+00:00', 2);
INSERT INTO t1 (c0, id) VALUES ('2022-01-03 00:00:00+00:00', 3);
INSERT INTO t1 (c0, id) VALUES ('2022-01-04 00:00:00+00:00', 4);
INSERT INTO t1 (c0, id) VALUES ('2022-01-05 00:00:00+00:00', 5);
INSERT INTO t1 (c0, id) VALUES ('2022-01-06 00:00:00+00:00', 6);
INSERT INTO t1 (c0, id) VALUES ('2022-01-07 00:00:00+00:00', 7);
INSERT INTO t1 (c0, id) VALUES ('2022-01-08 00:00:00+00:00', 8);
INSERT INTO t1 (c0, id) VALUES ('2022-01-09 00:00:00+00:00', 9);
INSERT INTO t1 (c0, id) VALUES ('2022-01-10 00:00:00+00:00', 10);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;


--echo ob_nls_initcap_2_8
--echo #timestamp with local timezone

drop table t1;
create table t1(c0 timestamp with local time zone,id number);
insert into t1 (c0, id) values (null, 0);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-01 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 1);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-02 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 2);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-03 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 3);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-04 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 4);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-05 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 5);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-06 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 6);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-07 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 7);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-08 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 8);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-09 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 9);
insert into t1(c0, id) values (to_timestamp_tz('2022-01-10 00:00:00 +08:00', 'yyyy-mm-dd hh24:mi:ss tzr'), 10);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;


--echo ob_nls_initcap_2_9
--echo #interval year to month

drop table t1;
create table t1(c0 interval year to month,id number);
insert into t1 (c0, id) values (null, 0);
insert into t1(c0, id) values (interval '1' year, 1);
insert into t1(c0, id) values (interval '2' year, 2);
insert into t1(c0, id) values (interval '3' year, 3);
insert into t1(c0, id) values (interval '4' year, 4);
insert into t1(c0, id) values (interval '5' year, 5);
insert into t1(c0, id) values (interval '6' year, 6);
insert into t1(c0, id) values (interval '7' year, 7);
insert into t1(c0, id) values (interval '8' year, 8);
insert into t1(c0, id) values (interval '9' year, 9);
insert into t1(c0, id) values (interval '10' year, 10);


select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;

--echo ob_nls_initcap_2_11

drop table t1;
create table t1(c0 number,id number);
insert into t1 (c0, id) values (null, 0);
insert into t1(c0, id) values (0, 1);
insert into t1(c0, id) values (100, 2);
insert into t1(c0, id) values (78.56, 3);
insert into t1(c0, id) values (-88.56, 4);
insert into t1(c0, id) values (-219.5, 5);
insert into t1(c0, id) values (0.0000000001, 6);
insert into t1(c0, id) values (19.589, 7);
insert into t1(c0, id) values (-783.25, 8);
insert into t1(c0, id) values (16.956, 9);
insert into t1(c0, id) values (31.269, 10);


select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;



--echo ob_nls_initcap_2_13
--echo #binary_float

drop table t1;
create table t1(c0 binary_float,id number);
insert into t1 values(0,1);
insert into t1 values(12.5f,2);
insert into t1 values(binary_float_nan,3);
insert into t1 values(-9.09f,4);
insert into t1 values(21.311111119f,5);
insert into t1 values(null,6);
insert into t1 values(binary_float_infinity,7);
insert into t1 values(31.255552f,8);
insert into t1 values(21.56555555,9);
insert into t1 values(31.2561,10);
insert into t1 values(15.625,11);
insert into t1 values(-15.654,12);
insert into t1 values(31.25,13);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;



--echo ob_nls_initcap_2_13
--echo #binary_double

drop table t1;
create table t1(c0 binary_double,id number);
insert into t1 values(0,1);
insert into t1 values(12.5d,2);
insert into t1 values(12.59d,3);
insert into t1 values(binary_double_nan,4);
insert into t1 values(-9.09d,5);
insert into t1 values(21.311111119f,-10.58);
insert into t1 values(null,-10.59);
insert into t1 values(null,null);
insert into t1 values(binary_double_infinity,6);
insert into t1 values(31.255552d,7);
insert into t1 values(21.596d,8);
insert into t1 values(31.256d,9);
insert into t1 values(-15.654,10);
insert into t1 values(31.15,11);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;



--echo ob_nls_initcap_2_14
--echo #float

drop table t1;
create table t1(c0 float,id number);
insert into t1 values(0,1);
insert into t1 values(12.5f,2);
insert into t1 values(-9.09f,3);
insert into t1 values(21.311111119f,4);
insert into t1 values(null,5);
insert into t1 values(31.255552f,6);
insert into t1 values(21.56555555,7);
insert into t1 values(31.2561,8);
insert into t1 values(15.625,9);
insert into t1 values(-15.654,10);
insert into t1 values(31.25,11);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;


--echo ob_nls_initcap_2_15
--echo #raw

drop table t1;
create table t1(c0 raw(50),id number);
insert into t1 values(utl_raw.cast_to_raw('fdsfd'),1);
insert into t1 values(utl_raw.cast_to_raw('中文'),2);
insert into t1 values(utl_raw.cast_to_raw(' 乐 福 '),3);
insert into t1 values(utl_raw.cast_to_raw('0352656'),4);
insert into t1 values(utl_raw.cast_to_raw('0 3 5 2 6 5 6 '),5);
insert into t1 values(utl_raw.cast_to_raw('乐乐乐'),6);
insert into t1 values(null,7);
insert into t1 values(utl_raw.cast_to_raw('98912中文'),8);
insert into t1 values(utl_raw.cast_to_raw('123中文'),9);
insert into t1 values(utl_raw.cast_to_raw('英文123'),10);
insert into t1 values(utl_raw.cast_to_raw('额与他人'),11);
insert into t1 values(utl_raw.cast_to_raw('话语'),12);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;



--echo ob_nls_initcap_2_16
--echo #clob

drop table t1;
create table t1(c0 clob,id number);
insert into t1 values('20E4B88E20E69687',1);
insert into t1 values('20E4B88E20E69687',2);
insert into t1 values('20E4B88E20E69687',3);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;


--echo ob_nls_initcap_2_17
--echo #blob

drop table t1;
create table t1(c0 blob,id number);
insert into t1 values('20E4B88E20E69687',1);
insert into t1 values('20E4B88E20E69687',2);
insert into t1 values('20E4B88E20E69687',3);

--error 600
select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

#--echo #CTAS
#--error 0,942
#drop table t2;
#--error 600
#create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
#desc t2;
#select * from t2 order by 1,2;


--echo ob_nls_initcap_2_18
--echo #rowid

drop table t1;
create table t1(c0 rowid,id number);
insert into t1 values('*AAIKAQAAAAAAAAA=',1);
insert into t1 values('*AAIKAQBAAAAAAAA=',2);
insert into t1 values('*AAIKAQCAAAAAAAA=',3);
insert into t1 values('*AAIKAQDAAAAAAAA=',4);
insert into t1 values('*AAIKAQRAAAAAAAA=',5);
insert into t1 values('*AAIKAQFAAAAAAAA=',6);
insert into t1 values('*AAIKAQGAAAAAAAA=',7);
insert into t1 values('*AAIKAQHAAAAAAAA=',8);
insert into t1 values('*AAIKAQQAAAAAAAA=',9);
insert into t1 values('*AAIKAQSAAAAAAAA=',10);
insert into t1 values('*AAIKAQMAAAAAAAA=',11);
insert into t1 values('*AAIKAQNAAAAAAAA=',12);
insert into t1 values(null,13);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;
#select nls_initcap(rowid),nls_initcap(rowid,'nls_sort=binary') from t1;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;

--echo ob_nls_initcap_2_19
--echo #urowid

drop table t1;
create table t1(c0 urowid,id number);
insert into t1 values('*AAIKAQAAAAAAAAA=',1);
insert into t1 values('*AAIKAQBAAAAAAAA=',2);
insert into t1 values('*AAIKAQCAAAAAAAA=',3);
insert into t1 values('*AAIKAQDAAAAAAAA=',4);
insert into t1 values('*AAIKAQRAAAAAAAA=',5);
insert into t1 values('*AAIKAQFAAAAAAAA=',6);
insert into t1 values('*AAIKAQGAAAAAAAA=',7);
insert into t1 values('*AAIKAQHAAAAAAAA=',8);
insert into t1 values('*AAIKAQQAAAAAAAA=',9);
insert into t1 values('*AAIKAQSAAAAAAAA=',10);
insert into t1 values('*AAIKAQMAAAAAAAA=',11);
insert into t1 values('*AAIKAQNAAAAAAAA=',12);
insert into t1 values(null,13);

select nls_initcap(c0),nls_initcap(c0,'nls_sort=binary') from t1 order by id;

--echo #CTAS
--error 0,942
drop table t2;
create table t2 as select nls_initcap(c0) c0,nls_initcap(c0,'nls_sort=binary')c1 from t1 order by id;
desc t2;
select * from t2 order by 1,2;

drop table t1;
drop table t2;
