--error 0,942
drop table t0;
create table t0(c0 int,c1 int)partition by hash(c0) partitions 4;

insert into t0 values(0,0);
insert into t0 values(1,1);
insert into t0 values(2,2);
insert into t0 values(3,3);

create or replace package pkg1 WRAPPED
176
c6kB3BDC2JNJnFrSgFUiBYhcaaU+P/anxxDmFpSnXsZb6zgGdq9mMU4nNTU+lJdy7Uvq/npO1EP7
sPs6llVsibH35xQnExcBRXEFSy5d3+fKjZCUV9KzdONjf5u8AlbSIbS+ZcW3Y9VjbDf3/F624ev2
CH23qTI/GTBRc0rHB4YiL1U=
;
/

create or replace package body pkg1 WRAPPED
280
0HQBe8V0Hj4yjccCRFqAVSgE14a5pXqYS1PuZW2S1Oi6vTxSEOGPDjmZKwwN1SXJEmweFLJDgEXY
i5IXiHfUTN9xlkdeYlmDSuXnfUrkwkT9aqb5Xs6nQAECZMNdqoreCoOIqZY74SZbUuMrhtfxPUKp
wS6UiG7ksDsNXkx0FSbVOwWaLvSbT+WF42vHGwQMI1dt4AIcvNwp9ti2X4CY5JbP4xc413uEp+//
yEGgQpnHhlziOIQEe0D0s84e4/fIO8VKBUw2R/XQIo+oXHK/ww==
;
/

drop table t0;
drop package pkg1;
