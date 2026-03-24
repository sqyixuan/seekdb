# ==== Purpose ====
#
# Checks that a given file does not exist. If the file exists, the
# test fails.
#
# ==== Usage ====
#
# --let $file_does_not_exist= /path/to/file
# --source mysql_test/test_suite/mysql8_t_2/include/file_does_not_exist.inc

# Will fail if file exists.
--write_file $file_does_not_exist
tmp
EOF

# Remove file again.
--remove_file $file_does_not_exist
