
set ob_query_timeout=10000000000;
select sleep(5);

use oceanbase;
alter system set memory_chunk_cache_size = '0M';

alter system set _pushdown_storage_level=3 tenant=sys;
alter system set _pushdown_storage_level=3 tenant=all_user;
alter system set _pushdown_storage_level=3 tenant=all_meta;
set global ob_query_timeout=10000000000;
