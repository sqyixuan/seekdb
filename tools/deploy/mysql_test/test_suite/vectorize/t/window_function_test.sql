--explain_protocol 1
############# test no partition by ##############
--echo only one window function
select sum(c1) from (select max(c2) over () c1 from test_wf);
select sum(c1) from (select min(c2) over () c1 from test_wf);
select sum(c1) from (select avg(c2) over () c1 from test_wf);


--echo multiple window functions
select sum(c1), sum(c2) from (select min(c2) over () c1,  max(c2) over () c2 from test_wf);

select sum(c1), sum(c2), sum(c3) from (select min(c2) over () c1,  max(c2) over () c2, avg(c2) over () c3 from test_wf);



#############  one partition by expr, no order by ##############
--echo only one window function
select sum(c1) from (select max(c2) over (partition by c1) c1 from test_wf);
select sum(c1) from (select min(c2) over (partition by c1) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1) c1 from test_wf);


--echo multiple window functions
select sum(c1), sum(c2) from (select min(c2) over (partition by c1) c1,  max(c2) over (partition by c1) c2 from test_wf);

select sum(c1), sum(c2) from (select min(c2) over (partition by c1) c1,  max(c2) over () c2 from test_wf);

select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1) c1,  max(c2) over () c2, avg(c2) over (partition by c1) c3 from test_wf);



#############  multiple partition by exprs, no order by ##############
--echo only one window function
select sum(c1) from (select max(c2) over (partition by c1, c2) c1 from test_wf);
select sum(c1) from (select min(c2) over (partition by c1, c2) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1, c2) c1 from test_wf);


--echo multiple window functions
select sum(c1), sum(c2) from (select min(c2) over (partition by c1, c2) c1,  max(c2) over (partition by c1, c2) c2 from test_wf);

select sum(c1), sum(c2) from (select min(c2) over (partition by c1) c1,  max(c2) over (partition by c1, c2) c2 from test_wf);

select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1) c1,  max(c2) over () c2, avg(c2) over (partition by c1, c2) c3 from test_wf);



#############  one partition by expr, with order by expr ##############
--echo only one window function, default range(RANGE UNBOUNDED PRECEDING AND CURRENT ROW)
select sum(c1) from (select max(c2) over (partition by c1 order by c2) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1 order by c2) c1 from test_wf);

--echo only one window function, range between
select sum(c1) from (select max(c2) over (partition by c1 order by c2 range between unbounded preceding and unbounded following) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1 order by c2 range between unbounded preceding and unbounded following) c1 from test_wf);

--echo only one window function, rows between
select sum(c1) from (select max(c2) over (partition by c1 order by c2 rows between unbounded preceding and current row) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1 order by c2 rows between unbounded preceding and current row) c1 from test_wf);

select sum(c1) from (select max(c2) over (partition by c1 order by c2 rows between unbounded preceding and unbounded following) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1 order by c2 rows between unbounded preceding and unbounded following) c1 from test_wf);


--echo multiple window functions, default range(RANGE UNBOUNDED PRECEDING AND CURRENT ROW)
select sum(c1), sum(c2) from (select min(c2) over (partition by c1 order by c2) c1,  max(c2) over (partition by c1 order by c2) c2 from test_wf);

select sum(c1), sum(c2) from (select min(c2) over (partition by c1 order by c2) c1,  max(c2) over () c2 from test_wf);

select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1 order by c2) c1,  max(c2) over () c2, avg(c2) over (partition by c1) c3 from test_wf);

--echo multiple window functions, range between
select sum(c1), sum(c2) from (select min(c2) over (partition by c1 order by c2 range between unbounded preceding and unbounded following) c1,
                              max(c2) over (partition by c1 order by c2 range between unbounded preceding and unbounded following) c2 from test_wf);
select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1 order by c2 range between unbounded preceding and unbounded following) c1,
                              max(c2) over () c2, avg(c2) over (partition by c1) c3 from test_wf);



--echo multiple window functions, rows between
select sum(c1), sum(c2) from (select min(c2) over (partition by c1 order by c2 rows between unbounded preceding and current row) c1,
                              max(c2) over (partition by c1 order by c2 rows between unbounded preceding and current row) c2 from test_wf);
select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1 order by c2 rows between unbounded preceding and current row) c1,
                              max(c2) over () c2, avg(c2) over (partition by c1) c3 from test_wf);

select sum(c1), sum(c2) from (select min(c2) over (partition by c1 order by c2 rows between unbounded preceding and unbounded following) c1,
                              max(c2) over (partition by c1 order by c2 rows between unbounded preceding and unbounded following) c2 from test_wf);
select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1 order by c2 rows between unbounded preceding and unbounded following) c1,
                              max(c2) over () c2, avg(c2) over (partition by c1) c3 from test_wf);



#############  multiple partition by exprs, with order by expr ###########################
--echo only one window function, default range(RANGE UNBOUNDED PRECEDING AND CURRENT ROW)
select sum(c1) from (select max(c2) over (partition by c1, c2 order by c3) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1, c2 order by c3) c1 from test_wf);

--echo only one window function, range between
select sum(c1) from (select max(c2) over (partition by c1, c2 order by c3 range between unbounded preceding and unbounded following) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1, c2 order by c3 range between unbounded preceding and unbounded following) c1 from test_wf);

--echo only one window function, rows between
select sum(c1) from (select max(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and current row) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and current row) c1 from test_wf);

select sum(c1) from (select max(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and unbounded following) c1 from test_wf);
select sum(c1) from (select avg(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and unbounded following) c1 from test_wf);


--echo multiple window functions, default range(RANGE UNBOUNDED PRECEDING AND CURRENT ROW)
select sum(c1), sum(c2) from (select min(c2) over (partition by c1, c2 order by c3) c1,  max(c2) over (partition by c1, c2 order by c3) c2 from test_wf);

select sum(c1), sum(c2) from (select min(c2) over (partition by c1, c2 order by c3) c1,  max(c2) over () c2 from test_wf);

select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1, c2 order by c3) c1,  max(c2) over () c2, avg(c2) over (partition by c1, c2) c3 from test_wf);

--echo multiple window functions, range between
select sum(c1), sum(c2) from (select min(c2) over (partition by c1, c2 order by c3 range between unbounded preceding and unbounded following) c1,
                              max(c2) over (partition by c1, c2 order by c3 range between unbounded preceding and unbounded following) c2 from test_wf);
select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1, c2 order by c3 range between unbounded preceding and unbounded following) c1,
                              max(c2) over () c2, avg(c2) over (partition by c1) c3 from test_wf);



--echo multiple window functions, rows between
select sum(c1), sum(c2) from (select min(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and current row) c1,
                              max(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and current row) c2 from test_wf);
select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and current row) c1,
                              max(c2) over () c2, avg(c2) over (partition by c1) c3 from test_wf);

select sum(c1), sum(c2) from (select min(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and unbounded following) c1,
                              max(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and unbounded following) c2 from test_wf);
select sum(c1), sum(c2), sum(c3) from (select min(c2) over (partition by c1, c2 order by c3 rows between unbounded preceding and unbounded following) c1,
                              max(c2) over () c2, avg(c2) over (partition by c1) c3 from test_wf);

--explain_protocol 0