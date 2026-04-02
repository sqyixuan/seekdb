
SELECT is_ipv4(inet_ntoa('1'));

###--disable_warnings ER_WRONG_VALUE_FOR_TYPE

SELECT is_ipv6(inet_ntoa('1'));
SELECT HEX(inet6_aton(inet_ntoa('1')));
SELECT inet6_ntoa(inet_ntoa('1'));

--echo #
--echo # Bug#14040277 UNINITIALIZED VALUE REFERENCED IN STR_TO_IPV6
--echo #
SELECT inet6_aton(soundex('a'));

--echo #
--echo # Bug#19047425 UNINITIALISED VALUE IN STR_TO_IPV6
--echo #

###--enable_warnings ER_WRONG_VALUE_FOR_TYPE

let $charset=`select @@character_set_client`;
--error 0, 1411
eval do is_ipv4_mapped(inet6_aton(convert(_ascii "a:" using $charset)));
