connect (conn_obcdc_msyql_lob,$ip,root@cdc_mysql,,oceanbase,$port);

create database geo_basic_db1;
use geo_basic_db1;

# 启动liboblog
ob1.oblog.set_extra_argument -m;
ob1.oblog.gen_conf tb_white_list=sys.oblog*.*|cdc_mysql.*.*|oracle.*.* log_level=DEBUG;
ob1.oblog.restart;

create table t1(id int primary key, g1 geometry);
ob1.oblog.next;

# point
insert into t1 values(1, ST_GeomFromText('POINT(-1234 5678)'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 1;
ob1.oblog.next;
# LineString
insert into t1 values(2, ST_GeomFromText('LINESTRING(0 0,-4 8,2000 2000)'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 2;
ob1.oblog.next;
# Polygon
insert into t1 values(3, ST_GeomFromText('Polygon((0 0,0 8,8 8,8 10,-10 10,-10 0,0 0))'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 3;
ob1.oblog.next;
# MultiPoint
insert into t1 values(4, ST_GeomFromText('MULTIPOINT(1 1,11 11,11 21,21 21)'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 4;
ob1.oblog.next;
# MultiLineString
insert into t1 values(5, ST_GeomFromText('MULTILINESTRING((10 48,10 21,10 0),(16 0,16 23,16 48))'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 5;
ob1.oblog.next;
# MultiPolyGon
insert into t1 values(6, ST_GeomFromText('MULTIPOLYGON(((28 26,28 0,84 0,84 42,28 26),(52 18,66 23,73 9,48 6,52 18)),((59 18,67 18,67 13,59 13,59 18)))'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 6;
ob1.oblog.next;
# Collection
insert into t1 values(7, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 7;
ob1.oblog.next;
insert into t1 values(8, ST_GeomFromText('GEOMETRYCOLLECTION ()'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 8;
ob1.oblog.next;
insert into t1 values(9, ST_GeomFromText('GEOMETRYCOLLECTION(Point(9 9),LINESTRING(1 5,0 0),POLYGON((2 2,2 8,8 8,8 2,2 2)))'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 9;
ob1.oblog.next;
insert into t1 values(10, ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POLYGON((0 0,50 45,40 50,0 0)),LINESTRING(-10 -10,0 0),LINESTRING(46.666666666666664 46.666666666666664,200 200,199 201,45.333333333333336 47.333333333333336),LINESTRING(8 10,-11 -9))'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 10;
ob1.oblog.next;
insert into t1 values(11, ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 1)),GEOMETRYCOLLECTION(LINESTRING(1 1,2 2)))'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 11;
ob1.oblog.next;
insert into t1 values(12, ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(),GEOMETRYCOLLECTION EMPTY, GEOMETRYCOLLECTION(POINT(1 1)), MULTILINESTRING((10 48,10 21,10 0),(16 0,16 23,16 48)), MULTIPOLYGON(((28 26,28 0,84 0,84 42,28 26),(52 18,66 23,73 9,48 6,52 18)),((59 18,67 18,67 13,59 13,59 18))), MULTIPOINT((0 0),(10 10),(10 20),(20 20)), MULTIPOINT(1 1, 11 11, 11 21, 21 21), GEOMETRYCOLLECTION(LINESTRING(1 1,2 2)), GEOMETRYCOLLECTION(POLYGON((0 0,50 45,40 50,0 0)),LINESTRING(-10 -10,0 0),LINESTRING(46.666666666666664 46.666666666666664,200 200,199 201,45.333333333333336 47.333333333333336),LINESTRING(8 10,-11 -9)))'));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 12;
ob1.oblog.next;

# update inrow -> inrow
update t1 set g1=ST_GEOMFROMTEXT('point(7000e-7 1e-10)') WHERE id=1;
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 1;
ob1.oblog.next;

# update inrow -> outrow
update t1 set g1=ST_GEOMFROMTEXT(concat('GEOMETRYCOLLECTION(', 'POINT(3 1)', repeat(',POINT(2 2)', 5000), ')')) WHERE id=2;
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 2;
ob1.oblog.next;

# delete outrow
delete from t1 where id=2;
ob1.oblog.next;

# insert outrow
insert into t1 values(19, ST_GEOMFROMTEXT(concat('GEOMETRYCOLLECTION(', 'POINT(1 1)', repeat(',POINT(1 1)', 5000), ')')));
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 19;
ob1.oblog.next;

# update outrow -> outrow
update t1 set g1=ST_GEOMFROMTEXT(concat('GEOMETRYCOLLECTION(', 'POINT(4 1)', repeat(',POINT(5 2)', 5000), ')')) WHERE id=19;
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 19;
ob1.oblog.next;

# update rowkey with outrow lob
update t1 set id=20 where id=19;
ob1.oblog.next;

# update outrow  -> inrow
update t1 set g1=ST_GeomFromText('MULTIPOLYGON(((28 26,28 3,84 3,84 42,28 26),(52 18,66 23,73 9,48 6,52 18)),((59 18,67 18,67 13,59 13,59 18)))') WHERE id=20;
select md5(concat('SRID=0;', st_astext(g1))) from t1 where id = 20;
ob1.oblog.next;

# delete inrow
delete from t1 where id=20;
ob1.oblog.next;

# delete
delete from t1;
ob1.oblog.next;

ob1.oblog.stop;