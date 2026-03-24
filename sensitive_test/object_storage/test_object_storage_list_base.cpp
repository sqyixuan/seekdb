/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#define private public
#include "test_object_storage.h"
namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;

char TestObjectStorage::dir_name_[OB_MAX_URI_LENGTH] = { 0 };
char TestObjectStorage::uri[OB_MAX_URI_LENGTH] = { 0 };

TEST_P(TestObjectStorage, test_util_list_files_with_consecutive_slash)
{
  int ret = OB_SUCCESS;
  if (enable_test_ && GetParam().extension_ == nullptr) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));

    // uri has two '//;
    TestObjectStorageListOp op;
    if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), "%s%s//", OB_FILE_PREFIX ,get_current_dir_name()));
      ASSERT_EQ(OB_SUCCESS, util.list_files(uri, true/*is_adaptive*/, op));
    } else {
      const Config &config = GetParam();
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s//test",
                                            config.bucket_, dir_name_));
      ASSERT_EQ(OB_INVALID_ARGUMENT, util.list_files(uri, true/*is_adaptive*/, op));
    }
  }
}

TEST_P(TestObjectStorage, test_util_list_files)
{
  int ret = OB_SUCCESS;
  if (enable_test_ 
      && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE
      && GetParam().extension_ == nullptr) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_list_files";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));

    {
      // wrong uri
      uri[0] = '\0';
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.list_files(uri, op));
    }
    
    int64_t file_num = 11;
    const char *write_content = "0123456789";

    // list objects
    {
      const char *format = "%s/%ld_%ld";
      std::set<std::string> files;
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri_, file_idx, file_idx));
        files.emplace(std::string(uri + strlen(dir_uri_) + 1));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));
      }

      // list and check and clean
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri_, false, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      ASSERT_EQ(files, op.object_names_);
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri_, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }

    // list subfolders' objects
    {
      const char *format = "%s/%ld/%ld";
      std::set<std::string> files;
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri_, file_idx, file_idx));
        files.emplace(std::string(uri + strlen(dir_uri_) + 1));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));
      }

      // list and check and clean
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri_, false, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      ASSERT_EQ(files, op.object_names_);
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri_, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }

    {
      // list empty dir, now dir_uri_ should be empty after delete
      TestObjectStorageListOp list_empty_op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri_, false, list_empty_op));
      ASSERT_EQ(0, list_empty_op.object_names_.size());
    }
    
    util.close();
  }
}

TEST_P(TestObjectStorage, test_util_list_directories)
{
  int ret = OB_SUCCESS;
  if (enable_test_ 
      && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE
      && GetParam().extension_ == nullptr) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_list_directories";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));

    {
      // wrong uri
      uri[0] = '\0';
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.list_directories(uri, false/*is_adaptive*/, op));
    }
    
    int64_t file_num = 11;
    const char *write_content = "0123456789";

    // list objects
    {
      const char *format = "%s/%ld/%ld";
      std::set<std::string> files;
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri_, file_idx, file_idx));
        files.emplace(std::string(std::to_string(file_idx)));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));
      }

      // list and check and clean
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri_, false, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      ASSERT_EQ(files, op.object_names_);
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri_, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }

    // no sub dirs
    {
      const char *format = "%s/%ld_%ld";
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri_, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));
      }

      // list and check and clean
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri_, false, op));
      ASSERT_EQ(0, op.object_names_.size());
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri_, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }
    
    util.close();
  }
}

TEST_P(TestObjectStorage, test_util_list_adaptive_files)
{
  int ret = OB_SUCCESS;
  if (enable_test_ 
      && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE
      && GetParam().extension_ == nullptr) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_append_dir = "test_util_list_files";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_append_dir, ts));
    
    {
      std::set<std::string> files;
      ObStorageAppender appender;
      const char *write_content = "012345678";
      auto write_group_files = [&](const int64_t file_num, const char *prefix) {
        for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
          std::string file_name;
          // normal file
          ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s%ld-normal",
                                                dir_uri_, prefix, file_idx));
          files.emplace(std::string(uri + strlen(dir_uri_) + 1));
          ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

          // appendable_file
          ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s%ld-appendable",
                                                dir_uri_, prefix, file_idx));
          files.emplace(std::string(uri + strlen(dir_uri_) + 1));
          ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
          ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, strlen(write_content), 0));
          ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
          ASSERT_EQ(OB_SUCCESS, appender.close());

          // appendable_file with suffix
          ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s%ld-appendable.back",
                                                dir_uri_, prefix, file_idx));
          files.emplace(std::string(uri + strlen(dir_uri_) + 1));
          ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
          ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, strlen(write_content), 0));
          ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
          ASSERT_EQ(OB_SUCCESS, appender.close());
        }
      };
      // file hierarchy
      // First level: 0-normal, 0-appendable, 0-appendable.back .... 9-normal, 9-appendable, 9-appendable.back
      // Second level: 0/0-normal, 0/0-appendable, 0/0-appendable.back, ... 9/9-normal, 9/9-appendable, 9/9-appendable.back
      // Third level: 0/0/0-normal, 0/0/0-appendable, 0/0/0-appendable.back, ... 9/4/4-normal, 9/4/4-appendable, 9/4/4-appendable.back,
      write_group_files(10, "");
      std::string prefix;
      // for (int64_t i = 0; i < 10; i++) {
      //   prefix = std::to_string(i) + "/";
      //   write_group_files(10, prefix.c_str());
      // }
      // for (int64_t i = 0; i < 10; i++) {
      //   for (int64_t j = 0; j < 5; j++) {
      //     prefix = std::to_string(i) + "/" + std::to_string(j) + "/";
      //     write_group_files(5, prefix.c_str());
      //   }
      // }

      // list and check and clean
      TestObjectStorageListOp op(strlen(write_content));
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri_, true/*is_adaptive*/, op));
      ASSERT_EQ(files, op.object_names_);

      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/", dir_uri_));
      op.object_names_.clear();
      ASSERT_EQ(OB_SUCCESS, util.list_files(uri, true/*is_adaptive*/, op));
      ASSERT_EQ(files, op.object_names_);

      for (auto &file : files) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s", dir_uri_, file.c_str()));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
      }
    }
  }
}
// Write a simulated append write file, containing one format file and 500 data files, totaling 501 sub-files
int write_appendable_object(const char *obj_name, ObObjectStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  if (OB_ISNULL(obj_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "obj_name is name", K(ret), K(obj_name), K(storage_info));
  } else if (OB_FAIL(util.open(&storage_info))) {
    OB_LOG(WARN, "fail to open util", K(ret), K(obj_name), K(storage_info));
  } else if (OB_FAIL(util.mkdir(obj_name))) {
    OB_LOG(WARN, "fail to mkdir", K(ret), K(obj_name), K(storage_info));
  } else if (OB_FAIL(construct_fragment_full_name(obj_name, OB_ADAPTIVELY_APPENDABLE_FORMAT_META,
                                                  uri_buf, sizeof(uri_buf)))) {
    OB_LOG(WARN, "fail to construct format meta name", K(ret), K(obj_name), K(storage_info));
  } else if (OB_FAIL(util.write_single_file(uri_buf, OB_ADAPTIVELY_APPENDABLE_FORMAT_CONTENT_V1,
                                            strlen(OB_ADAPTIVELY_APPENDABLE_FORMAT_CONTENT_V1)))) {
    OB_LOG(WARN, "fail to write format meta file", K(ret), K(obj_name), K(storage_info));
  } else {
    std::vector<std::future<int>> futures;
    const int64_t total_files = 500;
    const int64_t MAX_THREADS = 20;
    const int64_t per_threads_files = 25;
    for (int i = 0; i < MAX_THREADS; i++) {
      std::promise<int> promise;
      futures.push_back(promise.get_future());

      std::thread([obj_name, &storage_info, &util](
            const int i, const int64_t per_threads_files, std::promise<int> &&promise) mutable {
        int ret = OB_SUCCESS;
        char uri_buf[OB_MAX_URI_LENGTH] = {0};

        for (int j = i * per_threads_files; OB_SUCC(ret) && j < (i + 1) * per_threads_files; j++) {
          if (OB_FAIL(construct_fragment_full_name(obj_name, j, j + 1, uri_buf, sizeof(uri_buf)))) {
            OB_LOG(WARN, "fail to construct fragment name", K(ret),
                K(i), K(j), K(obj_name), K(storage_info));
          } else if (OB_FAIL(util.write_single_file(uri_buf, "a", 1))) {
            OB_LOG(WARN, "fail to write fragment", K(ret), K(obj_name), K(storage_info));
          }
        }  
        promise.set_value(ret);
      }, i, per_threads_files, std::move(promise)).detach();
    }

    int tmp_ret = OB_SUCCESS;
    for (auto &future : futures) {
      tmp_ret = future.get();
      if (OB_TMP_FAIL(tmp_ret)) {
        OB_LOG(WARN, "failed in writing fragment", K(tmp_ret));
      }
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

TEST_P(TestObjectStorage, test_util_list_with_marker)
{
  int ret = OB_SUCCESS;
  if (enable_test_ && GetParam().extension_ == nullptr) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_util_list_with_marker";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.mkdir(dir_uri_));
    // Write data, file format is:
    // base_dir/
    // ├── a
    // ├── b
    // ├── c_appendable.bak
    // ├── d_appendable
    // ├── e
    // │    ├── e_appendable
    // │    └── f
    // └── f
    // where 'appendable' indicates that the file is simulated as an append write file using small files
    const int64_t buf_size = 500;
    char buf[buf_size + 1];
    memset(buf, 'a', buf_size);
    buf[buf_size] = '\0';
    std::vector<std::string> object_list =
        {"a", "b", "c_appendable.bak", "d_appendable", "e/e_appendable", "e/f", "f"};
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/a", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, buf, buf_size));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/b", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, buf, buf_size));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/c_appendable.bak", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, write_appendable_object(uri, info_base_));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/d_appendable", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, write_appendable_object(uri, info_base_));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/e", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.mkdir(uri));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/e/e_appendable", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, write_appendable_object(uri, info_base_));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/e/f", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, buf, buf_size));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/f", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, buf, buf_size));

    {
      // Only list ordinary files
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("a", 1));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(1, op.object_list_.size());
      ASSERT_EQ(object_list[1], op.object_list_[0]);
    }

    OB_LOG(INFO, "=============================================");
    {
      // List ordinary files + simulate append write file, and the last file is a simulate append write file
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("a", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(3, op.object_list_.size());
      for (int64_t i = 0; i < 3; i++) {
        ASSERT_EQ(object_list[i + 1], op.object_list_[i]);
      }
    }
    // nfs will not list files in subdirectories
    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      // List ordinary files + simulate append write file, and the last file is a simulated append write file in the subdirectory
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("a", 4));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(4, op.object_list_.size());
      for (int64_t i = 0; i < 4; i++) {
        ASSERT_EQ(object_list[i + 1], op.object_list_[i]);
      }
    }

    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      // Expect the listed count to exceed the total number of files
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("a", 10));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(6, op.object_list_.size());
      for (int64_t i = 0; i < 6; i++) {
        ASSERT_EQ(object_list[i + 1], op.object_list_[i]);
      }
    } else {
      // nfs will not list files in subdirectories
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("a", 10));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(4, op.object_list_.size());
      ASSERT_EQ(object_list[1], op.object_list_[0]);
      ASSERT_EQ(object_list[2], op.object_list_[1]);
      ASSERT_EQ(object_list[3], op.object_list_[2]);
      ASSERT_EQ(object_list[6], op.object_list_[3]);
    }

    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      // The first file is a simulation of appending to a file
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("b", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(3, op.object_list_.size());
      for (int64_t i = 0; i < 3; i++) {
        ASSERT_EQ(object_list[i + 2], op.object_list_[i]);
      }
    } else {
      // nfs will not list files in subdirectories
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("b", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(3, op.object_list_.size());
      ASSERT_EQ(object_list[2], op.object_list_[0]);
      ASSERT_EQ(object_list[3], op.object_list_[1]);
      ASSERT_EQ(object_list[6], op.object_list_[2]);
    }

    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      // marker corresponds to simulated append write file
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("c_appendable.bak", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(3, op.object_list_.size());
      for (int64_t i = 0; i < 3; i++) {
        ASSERT_EQ(object_list[i + 3], op.object_list_[i]);
      }
    } else {
      // nfs will not list files in subdirectories
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("c_appendable.bak", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(2, op.object_list_.size());
      ASSERT_EQ(object_list[3], op.object_list_[0]);
      ASSERT_EQ(object_list[6], op.object_list_[1]);
    }

    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      // marker has a '/' suffix,
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("c_appendable.bak/", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(3, op.object_list_.size());
      for (int64_t i = 0; i < 3; i++) {
        ASSERT_EQ(object_list[i + 3], op.object_list_[i]);
      }
    } else {
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("c_appendable.bak", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(2, op.object_list_.size());
      ASSERT_EQ(object_list[3], op.object_list_[0]);
      ASSERT_EQ(object_list[6], op.object_list_[1]);
    }

    {
      // marker less than the first file
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("-", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(3, op.object_list_.size());
      for (int64_t i = 0; i < 3; i++) {
        ASSERT_EQ(object_list[i], op.object_list_[i]);
      }
    }

    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      // the file corresponding to marker does not exist
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("d", 10));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(4, op.object_list_.size());
      for (int64_t i = 0; i < 4; i++) {
        ASSERT_EQ(object_list[i + 3], op.object_list_[i]);
      }
    } else {
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("d", 10));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(2, op.object_list_.size());
      ASSERT_EQ(object_list[3], op.object_list_[0]);
      ASSERT_EQ(object_list[6], op.object_list_[1]);
    }
    
    {
      // marker is the last file
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("f", 10));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(0, op.object_list_.size());
    }

    {
      // marker greater than the last file
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("g", 10));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(0, op.object_list_.size());
    }

    // scan out <= 0
    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      // the file corresponding to marker does not exist
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("d", -1));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(4, op.object_list_.size());
      for (int64_t i = 0; i < 4; i++) {
        ASSERT_EQ(object_list[i + 3], op.object_list_[i]);
      }
    } else {
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("d", -1));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(2, op.object_list_.size());
      ASSERT_EQ(object_list[3], op.object_list_[0]);
      ASSERT_EQ(object_list[6], op.object_list_[1]);
    }
    // marker is empty
    {
      TestObjectStorageListOp op(buf_size);
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("", 3));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(dir_uri_, op));
      ASSERT_EQ(3, op.object_list_.size());
      for (int64_t i = 0; i < 3; i++) {
        ASSERT_EQ(object_list[i], op.object_list_[i]);
      }
    }
    // Clear data
    if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {

    } else {
      for (auto &obj : object_list) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s", dir_uri_, obj.c_str()));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true/*is_adaptive*/));
      }
    }
  }
}

int create_directories(const std::string &base_path, ObObjectStorageInfo &storage_info, char ch,
    int64_t length, std::string &current_path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(length));
  } else {
    int64_t chunk_size = 100;                                       // Number of per directory
    int64_t total_chunks = (length + chunk_size - 1) / chunk_size;  // Calculate total chunks
    current_path = base_path;
    ObStorageUtil util;
    util.open(&storage_info);
    for (int64_t i = 0; i < total_chunks; ++i) {
      int64_t start_pos = i * chunk_size;
      int64_t end_pos = std::min(start_pos + chunk_size, length);
      std::string chunk(end_pos - start_pos - 1, ch);
      // Append to the current path
      current_path += "/" + chunk ;
      // Create the directory
      util.mkdir(current_path.c_str());
    }
  }
  return ret;
}

// For the current URI length limit and file name length limit of the underlying storage driver,
// please refer to the document for details: 
TEST_P(TestObjectStorage, test_list_max_length_uri)
{
  if (enable_test_ && !with_checksum_) {
    const int64_t TEST_OBJ_LEN = 50;
    // NFS will create a temporary file before writing to the file, the temporary file
    // suffix is ​​in the form like `.tmp.1724415434483011`. The length of the temporary file is 21.
    const int64_t FILE_TMP_LENGTH = 21;
    // Currently, the internal code in NFS will limit the file length when creating a file. The
    // specific restrictions are: after removing the protocol prefix and adding the temporary file
    // suffix, the overall file length is limited to 2048. For users, the actual overall uri length
    // limit is 2034 (including protocol prefix)
    const int64_t OB_REAL_MAX_URI_LENGTH
        = OB_MAX_URI_LENGTH - FILE_TMP_LENGTH + strlen(OB_FILE_PREFIX);
    const char *tmp_dir = "test_list_max_length_uri";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    char uri[2100] = {0};
    ObStorageUtil util;
    ObStorageReader reader;
    TestObjectStorageListOp op;
    const Config &config = GetParam();

    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld", tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.mkdir(dir_uri_));
    std::vector<int64_t> uri_len_list
        = {OB_REAL_MAX_URI_LENGTH - 1, OB_REAL_MAX_URI_LENGTH, OB_REAL_MAX_URI_LENGTH + 1};
    for (int i = 0; i < uri_len_list.size(); i++) {
      char read_buf[5] = {0};
      int64_t read_size = 0;
      int64_t uri_pos = 0;
      const int64_t cur_uri_len = uri_len_list[i];
      int64_t uri_len = cur_uri_len;
      std::string dir_path;
      std::string invalid_uri(TEST_OBJ_LEN, i + 'a');
      int64_t dir_path_len = uri_len - TEST_OBJ_LEN - strlen(dir_uri_) - 1;

      if (ObStorageType::OB_STORAGE_FILE == info_base_.get_type()) {
        // Directories in NFS are also a type of files. The length limit of a single directory name
        // is the same as the file name, which is 255. Therefore, we need to create multiple levels
        // of nested directories to ensure that it can eventually be written.
        ASSERT_EQ(OB_SUCCESS, create_directories(dir_uri_, info_base_, 'A', 
            dir_path_len, dir_path));
      } else {
        dir_path.assign(dir_uri_, strlen(dir_uri_));
        dir_path.append(1, '/');
        dir_path.append(dir_path_len - 1, 'A');
      }

      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), uri_pos, "%s/%s", 
          dir_path.c_str(), invalid_uri.c_str()));

      ASSERT_EQ(cur_uri_len, strlen(uri));

      if (ObStorageType::OB_STORAGE_FILE == info_base_.get_type()) {
        if (i == 0) {
          // cur_uri_len == 2033
          ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123456", 6));
          ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
          ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 5, 0, read_size));
          ASSERT_EQ(5, read_size);
          ASSERT_EQ(OB_SUCCESS, reader.close());
          ASSERT_EQ(OB_SUCCESS, util.list_files(dir_path.c_str(), false, op));
          ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
        } else {
          // cur_uri_len == 2034, 2035
          ASSERT_EQ(OB_SIZE_OVERFLOW, util.write_single_file(uri, "123456", 6));
          ASSERT_NE(OB_SIZE_OVERFLOW, reader.open(uri, &info_base_));
          ASSERT_EQ(OB_SUCCESS, util.list_files(uri, false, op));
          ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
        }
      }
      // For object storage, lengths above 1024 are illegal.
      else {
        ASSERT_EQ(OB_INVALID_ARGUMENT, util.write_single_file(uri, "123456", 6));
        ASSERT_NE(OB_INVALID_ARGUMENT, reader.open(uri, &info_base_));
        if (0 == STRNCMP(config.storage_type_, "S3", strlen("S3"))
            || 0 == STRNCMP(config.storage_type_, "GCS", strlen("GCS"))
            || 0 == STRNCMP(config.storage_type_, "AZBLOB", strlen("AZBLOB"))) {
          ASSERT_EQ(OB_SUCCESS, util.list_files(uri, false, op));
        } else {
          ASSERT_EQ(OB_INVALID_ARGUMENT, util.list_files(uri, false, op));
        }
        ASSERT_EQ(OB_INVALID_ARGUMENT, util.del_file(uri));
      }
    }
    util.close();
  }
}

// Currently, we has a length limit of 256 for object_name in handle_listed_object because of NFS.
TEST_P(TestObjectStorage, test_list_when_obj_len_exceeds)
{
  if (enable_test_ && !with_checksum_) {
    ObStorageUtil util;
    TestObjectStorageListOp op;
    const int64_t MAX_LENGTH_LIST_OBJ_NAME = 256;
    const int64_t FILE_TMP_LENGTH = 21;
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
        "test_obj_len_exceeds", ts));

    std::vector<int64_t> object_uri_len_list
        = {MAX_LENGTH_LIST_OBJ_NAME - 1, MAX_LENGTH_LIST_OBJ_NAME, MAX_LENGTH_LIST_OBJ_NAME + 1};
    for (int i = 0; i < object_uri_len_list.size(); i++) {
      int64_t cur_uri_len = object_uri_len_list[i];
      if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
        // Since NFS will create a temporary file before writing to the file. Therefore, we need to remove
        // the length of the temporary file suffix.
        cur_uri_len -= FILE_TMP_LENGTH;
      }
      int64_t uri_pos = 0;
      std::string invalid_uri(cur_uri_len, i + 'a');
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), uri_pos, "%s/%s", dir_uri_, invalid_uri.c_str()));

      ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
      ASSERT_EQ(OB_SUCCESS, util.mkdir(dir_uri_));
      if (i == 0) {
        // cur_uri_len == 255
        // Test the limitation in list_files
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123456", 6));
        ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri_, false, op));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

        // Test the limitation in list_directories
        ASSERT_EQ(OB_SUCCESS, util.mkdir(uri));
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), uri_pos, "/test"));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123456", 6));
        ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri_, false, op));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
        util.close();
      } else {
        // curi_uri_len == 256, 257
        // Test the limitation in list_files
        if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
          ASSERT_EQ(OB_INVALID_ARGUMENT, util.write_single_file(uri, "123456", 6));
        } else {
          ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123456", 6));
          ASSERT_EQ(OB_INVALID_ARGUMENT, util.list_files(dir_uri_, false, op));
          ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

          // Test the limitation in list_directories
          ASSERT_EQ(OB_SUCCESS, util.mkdir(uri));
          ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), uri_pos, "/test"));
          ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123456", 6));
          ASSERT_EQ(OB_INVALID_ARGUMENT, util.list_directories(dir_uri_, false, op));
          ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
        }
        util.close();
      }
    }
  }
}

bool contains(const std::vector<std::string>& sourceVec, const std::string& targetStr)
{
  return std::find(sourceVec.begin(), sourceVec.end(), targetStr) != sourceVec.end();
}

TEST_P(TestObjectStorage, test_list_bucket)
{
  // use `is_file_storage_` to skip NFS test
  // use `with_checksum_` to skip test with different checksum type
  if (enable_test_ && (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE && !with_checksum_)) {
    // write file in the bucket
    ObStorageUtil util;
    bool is_exist = true;
    const char *content = "test";
    char tmp_uri[OB_MAX_URI_LENGTH];
    char target_object[OB_MAX_URI_LENGTH];
    char target_dir[OB_MAX_URI_LENGTH];
    int64_t obj_num;
    const int64_t ts = ObTimeUtility::current_time();
    ObStorageAppender appender;

    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    // // e.g. oss://bucket_example/object_storage_unittest_dir_ts/ts_test
    ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_uri, sizeof(tmp_uri), "%s%ld_test", dir_uri_, ts));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(target_object, sizeof(target_object), "%s/%ld_test", dir_name_, ts));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(target_dir, sizeof(target_dir), "%s", dir_name_));

    ASSERT_EQ(OB_SUCCESS, util.is_exist(tmp_uri, is_exist));
    ASSERT_FALSE(is_exist);

    ASSERT_EQ(OB_SUCCESS, util.write_single_file(tmp_uri, content, strlen(content)));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(tmp_uri, is_exist));
    ASSERT_TRUE(is_exist);

    // try to list bucket using list_files while no slash at the end of the bucket
    // bucket: oss://example-bucket
    {
      TestObjectStorageListOp op;
      char tmp_bucket[OB_MAX_URI_LENGTH] = {0};
      ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_bucket, sizeof(tmp_bucket), "%s", bucket_));
      tmp_bucket[strlen(tmp_bucket) - 1] = '\0';
      ASSERT_EQ(OB_SUCCESS, util.list_files(tmp_bucket, op));
      ASSERT_TRUE(contains(op.object_list_, target_object));
    }

    // try to list bucket using list_files
    // bucket: oss://example-bucket/
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(bucket_, op));
      ASSERT_TRUE(contains(op.object_list_, target_object));
    }

    // try to list bucket using list_directories
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(bucket_, op));
      ASSERT_TRUE(contains(op.object_list_, target_dir));
    }
    
    // try to list bucket using list_files_with_marker
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("0", 0));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(bucket_, op));
      ASSERT_TRUE(contains(op.object_list_, target_object));
    }

    // // try to list bucket using list_adaptive_files
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_adaptive_files(bucket_, op));
      ASSERT_TRUE(contains(op.object_list_, target_object));
    }

    // try to list bucket using list_files with is_adaptive parameter
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(bucket_, true/*is_adaptive*/, op));
      ASSERT_TRUE(contains(op.object_list_, target_object));
    }
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(bucket_, false, op));
      ASSERT_TRUE(contains(op.object_list_, target_object));
    }

    // try to list bucket using list_directories with is_adaptive parameter
    // TODO: list_directories with is_adaptive parameter is not supported
    /*
      ASSERT_EQ(OB_SUCCESS, util.list_directories(bucket_, true, op));
      ASSERT_TRUE(op.object_list_.size() > 0);
      obj_num = op.object_list_.size();
      ASSERT_EQ(target_dir, op.object_list_[obj_num-1]);
    */
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(bucket_, false, op));
      ASSERT_TRUE(contains(op.object_list_, target_dir));
    }

    ASSERT_EQ(OB_SUCCESS, util.del_file(tmp_uri));
    const int64_t end_ts = ObTimeUtility::current_time();
    OB_LOG(INFO, "list bucket completed", "cost_time", (end_ts - ts), "type", info_base_.get_type());
  }
}

bool contains_all(const std::vector<std::string>& sourceVec, const std::vector<std::string>& targetVec) {
  std::unordered_set<std::string> lookupSet(sourceVec.begin(), sourceVec.end());
  bool allContained = true;
  for (const auto& str : targetVec) {
    if (lookupSet.find(str) == lookupSet.end()) {
        allContained = false;
        break;
    }
  }
  return allContained;
}

// notice: this test need an empty bucket, or it will list all files in the bucket
// TODO(baonian.wcx): after the list limited objects is supported, we can use this test by the given marker
// to list the files in the bucket in the sepciifed timestamp range
TEST_P(TestObjectStorage, test_list_bucket_with_appendable_files)
{
  if (enable_test_ && (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE && !with_checksum_)) {
    // write file in the bucket
    ObStorageUtil util;
    bool is_exist = true;
    const char *content = "test";
    char tmp_uri[OB_MAX_URI_LENGTH];
    int64_t obj_num;
    const int64_t ts = ObTimeUtility::current_time();
    std::vector<std::string> files;
    std::vector<std::string> adaptive_files;
    std::vector<std::string> dirs;
    std::vector<std::string> adaptive_dirs;
    ObStorageAppender appender;

    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_uri, sizeof(tmp_uri), "%s%ld_", bucket_, ts));

    auto write_group_files = [&](const int64_t file_num, const char *prefix, const char *tmp_uri) {
      char uri[OB_MAX_URI_LENGTH] = {0};
      char format_meta_uri[OB_MAX_URI_LENGTH] = {0};
      char seal_meta_uri[OB_MAX_URI_LENGTH] = {0};
      char fragment_uri[OB_MAX_URI_LENGTH] = {0};

      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        std::string file_name;
        // normal file
        // e.g. s3://example-bucket/ts_1-normal
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s%s%ld-normal",
                                              tmp_uri, prefix, file_idx));
        files.push_back(std::string(uri + strlen(bucket_)));
        adaptive_files.push_back(std::string(uri + strlen(bucket_)));
        OB_LOG(INFO, "write_single_file", K(uri+ strlen(bucket_)), K(bucket_), K(strlen(bucket_)));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, content, strlen(content)));

        // appendable_file
        // e.g. s3://example-bucket/prefix1-appendable
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s%s%ld-appendable",
                                              tmp_uri, prefix, file_idx));
        files.push_back(std::string(uri + strlen(bucket_)));
        ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
        ASSERT_EQ(OB_SUCCESS, appender.pwrite(content, strlen(content), 0));
        ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
        ASSERT_EQ(OB_SUCCESS, appender.close());
        // add format meta file: s3://example-bucket/prefix-1-appendable/@APD_PART@FORMAT_META
        ASSERT_EQ(OB_SUCCESS, construct_fragment_full_name(uri, OB_ADAPTIVELY_APPENDABLE_FORMAT_META,
                                               format_meta_uri, sizeof(format_meta_uri)));
        adaptive_files.push_back(std::string(format_meta_uri + strlen(bucket_)));
        // add fragment file: s3://example-bucket/prefix-1-appendable/@APD_PART@0-strlen(content)
        ASSERT_EQ(OB_SUCCESS, construct_fragment_full_name(uri, 0, strlen(content),
                                               fragment_uri, sizeof(fragment_uri)));
        adaptive_files.push_back(std::string(fragment_uri + strlen(bucket_)));
        // add seal meta file: s3://example-bucket/prefix-1-appendable/@APD_PART@SEAL_META
        ASSERT_EQ(OB_SUCCESS, construct_fragment_full_name(uri, OB_ADAPTIVELY_APPENDABLE_SEAL_META,
                                               seal_meta_uri, sizeof(seal_meta_uri)));
        adaptive_files.push_back(std::string(seal_meta_uri + strlen(bucket_)));

        // appendable_file with suffix
        // e.g. s3://example-bucket/prefix1-appendable.back
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s%s%ld-appendable.back",
                                              tmp_uri, prefix, file_idx));
        files.push_back(std::string(uri + strlen(bucket_)));
        ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
        ASSERT_EQ(OB_SUCCESS, appender.pwrite(content, strlen(content), 0));
        ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
        ASSERT_EQ(OB_SUCCESS, appender.close());
        // add format meta file: s3://example-bucket/prefix-1-appendable/@APD_PART@FORMAT_META
        ASSERT_EQ(OB_SUCCESS, construct_fragment_full_name(uri, OB_ADAPTIVELY_APPENDABLE_FORMAT_META,
                                               format_meta_uri, sizeof(format_meta_uri)));
        adaptive_files.push_back(std::string(format_meta_uri + strlen(bucket_)));
        // add fragment file: s3://example-bucket/prefix-1-appendable/@APD_PART@0-strlen(content)
        ASSERT_EQ(OB_SUCCESS,construct_fragment_full_name(uri, 0, strlen(content),
                                               fragment_uri, sizeof(fragment_uri)));
        adaptive_files.push_back(std::string(fragment_uri + strlen(bucket_)));
        // add seal meta file: s3://example-bucket/prefix-1-appendable/@APD_PART@SEAL_META
        ASSERT_EQ(OB_SUCCESS, construct_fragment_full_name(uri, OB_ADAPTIVELY_APPENDABLE_SEAL_META,
                                               seal_meta_uri, sizeof(seal_meta_uri)));
        adaptive_files.push_back(std::string(seal_meta_uri + strlen(bucket_)));
      }
    };

    //  PRE ts_0-appendable.back/
    //  PRE ts_0-appendable/
    //  PRE ts_0-normal
    //  PRE ts_0/
    //       ├──  PRE 0-appendable.back/
    //       ├──  PRE 0-appendable/
    //       ├──  PRE 0-normal
    //       ├──  PRE 0/
    //               ├──  PRE 0-appendable.back/
    //               ├──  PRE 0-appendable/
    //               ├──  PRE 0-normal
    write_group_files(5, "", tmp_uri);
    std::string prefix;
    for (int64_t i = 0; i < 5; i++) {
      prefix = std::to_string(i) + "/";
      write_group_files(5, prefix.c_str(), tmp_uri);
    }
    for (int64_t i = 0; i < 5; i++) {
      for (int64_t j = 0; j < 5; j++) {
        prefix = std::to_string(i) + "/" + std::to_string(j) + "/";
        write_group_files(5, prefix.c_str(), tmp_uri);
      }
    }

    for (int64_t i = 0; i < 5; i++) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_uri, sizeof(tmp_uri), "%ld_%ld", ts, i));
      dirs.push_back(tmp_uri);
      adaptive_dirs.push_back(tmp_uri);
      ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_uri, sizeof(tmp_uri), "%ld_%ld-appendable", ts, i));
      adaptive_dirs.push_back(tmp_uri);
      ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_uri, sizeof(tmp_uri), "%ld_%ld-appendable.back", ts, i));
      adaptive_dirs.push_back(tmp_uri);
    }

    {
      // try to list bucket using list_files_with_marker
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("0", 0));
      ASSERT_EQ(OB_SUCCESS, util.list_files_with_marker(bucket_, op));
      ASSERT_TRUE(contains_all(op.object_list_, files));
    }

    {
      // try to list bucket using list_adaptive_files
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_adaptive_files(bucket_, op));
      ASSERT_TRUE(contains_all(op.object_list_, files));
    }

    // try to list bucket using list_directories with is_adaptive parameter
    // TODO: list_directories with is_adaptive parameter is not supported
    /*
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(bucket_, true, op));
      ASSERT_EQ(op.object_list_.size(), DIR_NUM);
      ASSERT_EQ(dirs, op.object_names_);
      }
    */
    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(bucket_, false, op));
      if (is_adaptive_append_mode(info_base_)) {
        ASSERT_TRUE(contains_all(op.object_list_, adaptive_dirs));
      }
    }

    // try to list bucket using list_files with is_adaptive parameter
    {
       // try to list bucket using list_files with is_adaptive parameter
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(bucket_, false, op));
      if (is_adaptive_append_mode(info_base_)) {
        ASSERT_TRUE(contains_all(op.object_list_, adaptive_files));
      }
    }

    {
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(bucket_, true, op));
      ASSERT_TRUE(contains_all(op.object_list_, files));
    }
    // clean the current test dir after test
    
    for (auto &obj : files) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_uri, sizeof(tmp_uri), "%s%s", bucket_, obj.c_str()));
      ASSERT_EQ(OB_SUCCESS, util.del_file(tmp_uri, true/*is_adaptive*/));
    }
    const int64_t end_ts = ObTimeUtility::current_time();
    OB_LOG(INFO, "list bucket with adaptive files completed", "cost_time", (end_ts - ts), "type", info_base_.get_type());
  }
}

std::vector<Config> all_configs;


} // end namaspace unittest
} // end namespace oceanbase
