#owner: linlin.xll
#owner group: sql1
#description: xian bank user define types
#tags: pl

--result_format 4

--disable_warnings

delimiter /;

#### clean
DROP TYPE IDX_LOC_TYPE FORCE;
/
DROP TYPE LIMIT_DATA_OBJECT FORCE;
/
DROP TYPE LIMIT_DATA_OBJECT_NEW FORCE;
/
DROP TYPE LIMIT_LIST_TYPE FORCE;
/
DROP TYPE LIMIT_LIST_TYPE_NEW FORCE;
/
DROP TYPE OBJECTS_LIST FORCE;
/
DROP TYPE REPORT_TEMPLATE FORCE;
/