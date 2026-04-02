#
# Utility functions for working with ICU.
#

--disable_query_log
--error 0,1305
DROP FUNCTION icu_major_version;
CREATE FUNCTION icu_major_version() RETURNS INT
  RETURN regexp_replace(icu_version(), '([[:digit:]]+)\..*', '$1');
--enable_query_log
