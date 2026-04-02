# include/have_hypergraph.inc
#
# Enable use of the hypergraph optimizer in the current connection if
# it is available. Skip the test if mysqld was built without support
# for the hypergraph optimizer.

--disable_query_log
--disable_warnings

SET optimizer_switch = 'hypergraph_optimizer=on';

--enable_warnings
--enable_query_log
