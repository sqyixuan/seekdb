oblib_addtest(test_allocators.cpp)
target_link_libraries(test_allocators PRIVATE oblib_base $<$<AND:$<NOT:$<PLATFORM_ID:Darwin>>,$<NOT:$<PLATFORM_ID:Windows>>>:malloc_hook>)
