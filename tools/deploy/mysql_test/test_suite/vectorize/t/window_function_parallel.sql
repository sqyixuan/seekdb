--explain_protocol 1
alter session force parallel query parallel 1;

--echo only one window function
select max(c2) over () c1 from test_wf;
select min(c2) over () c1 from test_wf;
select sum(c2) over () c1 from test_wf;

--echo multiple window functions
select min(c2) over () c1,  max(c2) over () c2 from test_wf;
select min(c2) over () c1,  max(c2) over () c2, sum(c2) over () c3 from test_wf;


alter session force parallel query parallel 2;
--echo only one window function
select max(c2) over () c1 from test_wf;
select min(c2) over () c1 from test_wf;
select sum(c2) over () c1 from test_wf;

--echo multiple window functions
select min(c2) over () c1,  max(c2) over () c2 from test_wf;
select min(c2) over () c1,  max(c2) over () c2, sum(c2) over () c3 from test_wf;

alter session force parallel query parallel 3;
--echo only one window function
select max(c2) over () c1 from test_wf;
select min(c2) over () c1 from test_wf;
select sum(c2) over () c1 from test_wf;

--echo multiple window functions
select min(c2) over () c1,  max(c2) over () c2 from test_wf;
select min(c2) over () c1,  max(c2) over () c2, sum(c2) over () c3 from test_wf;


alter session force parallel query parallel 8;
--echo only one window function
select max(c2) over () c1 from test_wf;
select min(c2) over () c1 from test_wf;
select sum(c2) over () c1 from test_wf;

--echo multiple window functions
select min(c2) over () c1,  max(c2) over () c2 from test_wf;
select min(c2) over () c1,  max(c2) over () c2, sum(c2) over () c3 from test_wf;

alter session force parallel query parallel 1;

--explain_protocol 0