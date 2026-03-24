#owner: ryan.ly
#owner group: sql4
#description: OB优化器暂时没有运行时诊断，我们只看看explain的结果和执行结果。

# include/execute_with_statistics.inc
#
# SUMMARY
#
#    Explain and execute the select statment in $query.
#    Then report 'Last_query_cost' estimate from the query
#    optimizer and total number of 'Handler_read%' when the
#    query was executed.
#    Intended usage is to verify that there are not regressions
#    in either calculated or actuall cost for $query.
#
# USAGE
#
#    let $query= <select statement>;
#    --source include/execute_with_statistics.inc
#
# EXAMPLE
#    t/greedy_optimizer.test
#

eval EXPLAIN $query;
eval $query;
#SHOW STATUS LIKE 'Last_query_cost';

#FLUSH STATUS;
#eval $query;
#SELECT SUM(variable_value) AS Total_handler_reads
#   FROM information_schema.session_status
#   WHERE variable_name LIKE 'Handler_read%';
