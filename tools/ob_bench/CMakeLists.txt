add_executable(ob_bench
  ob_bench.cpp
  )

target_include_directories(ob_bench PRIVATE)

target_link_libraries(ob_bench
  PRIVATE
  -Wl,--start-group
  oceanbase_static
  ob_sql_static
  ob_storage_static
  -Wl,--end-group
  -static-libgcc
  -static-libstdc++)
