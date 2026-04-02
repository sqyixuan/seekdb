oblib_addtest(test_allocators.cpp)
target_link_libraries(test_allocators PRIVATE oblib_base $<$<NOT:$<PLATFORM_ID:Darwin>>:malloc_hook>)
