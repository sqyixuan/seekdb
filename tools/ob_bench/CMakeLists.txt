add_executable(ob_bench
  ob_bench.cpp
  )

target_include_directories(ob_bench PRIVATE)

target_link_libraries(ob_bench
  PRIVATE
  $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--start-group>
  oceanbase_static
  ob_sql_static
  ob_storage_static
  $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--end-group>
  $<$<NOT:$<PLATFORM_ID:Darwin>>:-static-libgcc>
  $<$<NOT:$<PLATFORM_ID:Darwin>>:-static-libstdc++>
)
