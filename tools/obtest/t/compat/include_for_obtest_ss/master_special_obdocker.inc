#升级后source配置项
obdocker.sh cp ob_special_config/master/sys/config.sql.bak  ob_special_config/master/sys/config.sql;
obdocker.sh cp ob_special_config/master/mysql/config.sql.bak ob_special_config/master/mysql/config.sql;
obdocker.sh cp ob_special_config/master/oracle/config.sql.bak ob_special_config/master/oracle/config.sql;
obdocker.sh sed -i '/ssl_/s/^/#/' ob_special_config/master/sys/config.sql;
obdocker.sh sed -i '/SSL/s/^/#/' ob_special_config/master/sys/config.sql;
connection proxy_conn;
source ob_special_config/master/sys/config.sql;
connection tentsql_conn;
source ob_special_config/master/mysql/config.sql;
connection tentora_conn;
source ob_special_config/master/oracle/config.sql;
