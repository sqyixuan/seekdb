function(ob_perftest case)
  if(ARGC EQUAL 1)
    add_executable(${case} ${case}.cpp)
  else()
    add_executable(${ARGV})
  endif()
  target_link_libraries(${case} PRIVATE gtest gmock oceanbase_static -static-libgcc -static-libstdc++)
  target_include_directories(${case}
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/test/benchmark)
endfunction()

add_subdirectory(common)
add_subdirectory(lib)

