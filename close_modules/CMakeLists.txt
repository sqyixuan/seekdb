# 闭源模块cmake根文件

# ob_close_modules 主要是src的核心代码
# ob_close_deps 主要是deps的依赖代码

if(NOT OB_BUILD_OBSERVER_LITE)
ob_set_subtarget(ob_logservice archiveservice
  observer_lite/logservice/archiveservice/ob_archive_allocator.cpp
  observer_lite/logservice/archiveservice/ob_archive_define.cpp
  observer_lite/logservice/archiveservice/ob_archive_fetcher.cpp
  observer_lite/logservice/archiveservice/ob_archive_file_utils.cpp
  observer_lite/logservice/archiveservice/ob_archive_io.cpp
  observer_lite/logservice/archiveservice/ob_archive_persist_mgr.cpp
  observer_lite/logservice/archiveservice/ob_archive_round_mgr.cpp
  observer_lite/logservice/archiveservice/ob_archive_sender.cpp
  observer_lite/logservice/archiveservice/ob_archive_sequencer.cpp
  observer_lite/logservice/archiveservice/ob_archive_service.cpp
  observer_lite/logservice/archiveservice/ob_archive_task.cpp
  observer_lite/logservice/archiveservice/ob_archive_task_queue.cpp
  observer_lite/logservice/archiveservice/ob_archive_util.cpp
  observer_lite/logservice/archiveservice/ob_ls_mgr.cpp
  observer_lite/logservice/archiveservice/ob_ls_task.cpp
  observer_lite/logservice/archiveservice/ob_start_archive_helper.cpp
  observer_lite/logservice/archiveservice/dynamic_buffer.cpp
  observer_lite/logservice/archiveservice/large_buffer_pool.cpp
  observer_lite/logservice/archiveservice/ob_archive_scheduler.cpp
  observer_lite/logservice/archiveservice/ob_archive_timer.cpp
  observer_lite/logservice/archiveservice/ob_ls_meta_recorder.cpp
  observer_lite/logservice/archiveservice/ob_ls_meta_record_task.cpp
)

ob_set_subtarget(ob_logservice cdcservice
  observer_lite/logservice/cdcservice/ob_cdc_fetcher.cpp
  observer_lite/logservice/cdcservice/ob_cdc_raw_log_req.cpp
  observer_lite/logservice/cdcservice/ob_cdc_req_struct.cpp
  observer_lite/logservice/cdcservice/ob_cdc_req.cpp
  observer_lite/logservice/cdcservice/ob_cdc_rpc_processor.cpp
  observer_lite/logservice/cdcservice/ob_cdc_rpc_proxy.cpp
  observer_lite/logservice/cdcservice/ob_cdc_service.cpp
  observer_lite/logservice/cdcservice/ob_cdc_service_monitor.cpp
  observer_lite/logservice/cdcservice/ob_cdc_start_lsn_locator.cpp
  observer_lite/logservice/cdcservice/ob_cdc_struct.cpp
  observer_lite/logservice/cdcservice/ob_cdc_util.cpp
)


ob_set_subtarget(ob_logservice logrouteservice
  observer_lite/logservice/logrouteservice/ob_all_server_info.cpp
  observer_lite/logservice/logrouteservice/ob_all_zone_info.cpp
  observer_lite/logservice/logrouteservice/ob_all_units_info.cpp
  observer_lite/logservice/logrouteservice/ob_external_server_blacklist.cpp
  observer_lite/logservice/logrouteservice/ob_log_all_svr_cache.cpp
  observer_lite/logservice/logrouteservice/ob_log_route_key.cpp
  observer_lite/logservice/logrouteservice/ob_log_route_service.cpp
  observer_lite/logservice/logrouteservice/ob_log_route_struct.cpp
  observer_lite/logservice/logrouteservice/ob_log_systable_queryer.cpp
  observer_lite/logservice/logrouteservice/ob_ls_log_stat_info.cpp
  observer_lite/logservice/logrouteservice/ob_ls_server_list.cpp
  observer_lite/logservice/logrouteservice/ob_server_blacklist.cpp
  observer_lite/logservice/logrouteservice/ob_server_priority.cpp
)


ob_set_subtarget(ob_logservice restoreservice
  observer_lite/logservice/restoreservice/ob_fetch_log_task.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_define.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_handler.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_rpc.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_rpc_define.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_service.cpp
  observer_lite/logservice/restoreservice/ob_remote_data_generator.cpp
  observer_lite/logservice/restoreservice/ob_remote_fetch_log.cpp
  observer_lite/logservice/restoreservice/ob_remote_fetch_log_worker.cpp
  observer_lite/logservice/restoreservice/ob_remote_location_adaptor.cpp
  observer_lite/logservice/restoreservice/ob_remote_log_source.cpp
  observer_lite/logservice/restoreservice/ob_remote_log_source_allocator.cpp
  observer_lite/logservice/restoreservice/ob_log_archive_piece_mgr.cpp
  observer_lite/logservice/restoreservice/ob_remote_error_reporter.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_allocator.cpp
  observer_lite/logservice/restoreservice/ob_remote_fetch_context.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_scheduler.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_driver_base.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_net_driver.cpp
  observer_lite/logservice/restoreservice/ob_log_restore_archive_driver.cpp
  observer_lite/logservice/restoreservice/ob_restore_log_function.cpp
  observer_lite/logservice/restoreservice/ob_remote_log_writer.cpp
  observer_lite/logservice/restoreservice/ob_remote_log_raw_reader.cpp
)

ob_set_subtarget(ob_logservice data_dictionary
  observer_lite/logservice/data_dictionary/ob_data_dict_utils.cpp
  observer_lite/logservice/data_dictionary/ob_data_dict_struct.cpp
  observer_lite/logservice/data_dictionary/ob_data_dict_service.cpp
  observer_lite/logservice/data_dictionary/ob_data_dict_sql_client.cpp
  observer_lite/logservice/data_dictionary/ob_data_dict_storager.cpp
  observer_lite/logservice/data_dictionary/ob_data_dict_iterator.cpp
  observer_lite/logservice/data_dictionary/ob_data_dict_meta_info.cpp
)

ob_set_subtarget(ob_logservice logfetcher
  observer_lite/logservice/logfetcher/ob_log_fetcher.cpp
  observer_lite/logservice/logfetcher/ob_log_fetching_mode.cpp
  observer_lite/logservice/logfetcher/ob_log_ls_fetch_ctx.cpp
  observer_lite/logservice/logfetcher/ob_log_fetcher_ls_ctx_default_factory.cpp
  observer_lite/logservice/logfetcher/ob_log_ls_fetch_mgr.cpp
  observer_lite/logservice/logfetcher/ob_log_fetcher_idle_pool.cpp
  observer_lite/logservice/logfetcher/ob_log_fetcher_dead_pool.cpp
  observer_lite/logservice/logfetcher/ob_log_start_lsn_locator.cpp
  observer_lite/logservice/logfetcher/ob_log_part_progress_controller.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_stream_container.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_stream_container_mgr.cpp
  observer_lite/logservice/logfetcher/ob_ls_worker.cpp
  observer_lite/logservice/logfetcher/ob_log_rpc.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_log_rpc.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_stream_type.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_stream_pool.cpp
  observer_lite/logservice/logfetcher/ob_log_fetcher_bg_worker.cpp
  observer_lite/logservice/logfetcher/ob_log_fetcher_switch_info.cpp
  observer_lite/logservice/logfetcher/ob_log_ls_fetch_stream.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_stat_info.cpp
  observer_lite/logservice/logfetcher/ob_log_trace_id.cpp
  observer_lite/logservice/logfetcher/ob_log_utils.cpp
  observer_lite/logservice/logfetcher/ob_log_progress_info.cpp
  observer_lite/logservice/logfetcher/ob_log_timer.cpp
  observer_lite/logservice/logfetcher/ob_log_config.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_log_rpc_req.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_log_rpc_result.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_log_rpc_splitter.cpp
  observer_lite/logservice/logfetcher/ob_log_fetch_log_rpc_stop_reason.cpp
  observer_lite/logservice/logfetcher/ob_log_file_buffer_pool.cpp
)

if (BUILD_CDC_ONLY)
  add_subdirectory(observer_lite/logservice/logminer EXCLUDE_FROM_ALL)
else()
  add_subdirectory(observer_lite/logservice/logminer)
endif()

if (CMAKE_BUILD_RPM AND NOT BUILD_CDC_ONLY)
  add_subdirectory(observer_lite/logservice/libobcdc EXCLUDE_FROM_ALL)
else()
  add_subdirectory(observer_lite/logservice/libobcdc)
endif()
endif()

# 检查是否需要编译
check_need_build_unity_target(ob_close_modules need_build_close_modules)
check_need_build_unity_target(ob_close_deps need_build_close_deps)

message(STATUS "check need build ob_close_modules: ${need_build_close_modules}")
message(STATUS "check need build ob_close_deps: ${need_build_close_deps}")

if (need_build_close_modules)
  set(ob_close_modules_static_name "ob_close_modules_static" PARENT_SCOPE)
 
  ob_add_new_object_target(ob_close_modules ob_close_modules)
  target_link_libraries(ob_close_modules PUBLIC ob_base)
 
  add_library(ob_close_modules_static
    STATIC
    EXCLUDE_FROM_ALL)
 
  target_link_libraries(ob_close_modules_static
    PUBLIC ob_close_modules)
  
endif()
 
if (need_build_close_deps)
  set(ob_close_deps_static_name "ob_close_deps_static" PARENT_SCOPE)
 
  ob_add_new_object_target(ob_close_deps ob_close_deps)
 
  target_link_libraries(ob_close_deps PUBLIC ob_base)
 
  add_library(ob_close_deps_static
    STATIC
    EXCLUDE_FROM_ALL)
 
  target_link_libraries(ob_close_deps_static
    PUBLIC ob_close_deps)
endif()
