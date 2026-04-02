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
#include "lib/utility/ob_test_util.h"
#define protected public
#define private public
#include "lib/restore/ob_storage.h"
#include "lib/allocator/page_arena.h"
#include "test_object_storage.h"
#include "object_storage_authorization_info.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;

char TestObjectStorage::dir_name_[OB_MAX_URI_LENGTH] = { 0 };
char TestObjectStorage::uri[OB_MAX_URI_LENGTH] = { 0 };

int test_gen_object_meta(
    const char **fragments, 
    const int64_t n_fragments,
    const int64_t n_remained_fragments, 
    const int64_t *expected_start, 
    const int64_t *expected_end,
    const int64_t expected_file_length, 
    ObStorageObjectMeta &appendable_obj_meta)
{
  int ret = OB_SUCCESS;
  dirent entry;
  ListAppendableObjectFragmentOp op(false);
  appendable_obj_meta.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < n_fragments; i++) {
    if (OB_FAIL(databuff_printf(entry.d_name, sizeof(entry.d_name), "%s%s",
        OB_ADAPTIVELY_APPENDABLE_FRAGMENT_PREFIX, fragments[i]))) {
      OB_LOG(WARN, "fail to databuff printf", K(ret), K(i), K(fragments[i]));
    } else if (OB_FAIL(op.func(&entry))) {
      OB_LOG(WARN, "fail to execute op", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(op.gen_object_meta(appendable_obj_meta))) {
    OB_LOG(WARN, "fail to gen object meta", K(ret), K(appendable_obj_meta));
  } else if (ObStorageObjectMetaType::OB_OBJ_SIMULATE_APPEND != appendable_obj_meta.type_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "not expected value", K(ret), K(appendable_obj_meta.type_));
  } else if (expected_file_length != appendable_obj_meta.length_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "not expected value", K(ret), K(expected_file_length), K(appendable_obj_meta.length_));
  } else if (n_remained_fragments != appendable_obj_meta.fragment_metas_.count()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "not expected value", K(ret), K(n_remained_fragments), K(appendable_obj_meta.fragment_metas_.count()));
  } 
  
  for (int64_t i = 0; OB_SUCC(ret) && i < n_remained_fragments; i++) {
    if (!appendable_obj_meta.fragment_metas_[i].is_data() || !appendable_obj_meta.fragment_metas_[i].is_valid() ||
        expected_start[i] != appendable_obj_meta.fragment_metas_[i].start_ || 
        expected_end[i] != appendable_obj_meta.fragment_metas_[i].end_) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "not expected value", K(ret), K(i), K(appendable_obj_meta.fragment_metas_[i]), K(expected_start[i]), K(expected_end[i]));
    }
  }
  return ret;
}

int test_get_needed_fragments(
    const int64_t start, 
    const int64_t end,
    const int64_t n_expected_fragments, 
    const int64_t *expected_start, 
    const int64_t *expected_end,
    ObStorageObjectMeta &appendable_obj_meta)
{
  int ret = OB_SUCCESS;
  ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
  if (OB_FAIL(appendable_obj_meta.get_needed_fragments(start, end, fragments_need_to_read))) {
    OB_LOG(WARN, "fail to get needed fragments", K(ret), K(start), K(end));
  } else if (n_expected_fragments != fragments_need_to_read.count()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "not expected value", K(ret), K(n_expected_fragments), K(fragments_need_to_read.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < n_expected_fragments; i++) {
    if (!fragments_need_to_read[i].is_data() || !fragments_need_to_read[i].is_valid() ||
        expected_start[i] != fragments_need_to_read[i].start_ || 
        expected_end[i] != fragments_need_to_read[i].end_) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "not expected value", K(ret), K(i), K(fragments_need_to_read[i]), K(expected_start[i]), K(expected_end[i]));
    }
  }
  return ret;
}

TEST_P(TestObjectStorage, test_appendable_object_util)
{
  int ret = OB_SUCCESS;
  if (enable_test_ 
      && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE
      && GetParam().extension_ == nullptr) {
    {
      ListAppendableObjectFragmentOp op;
      dirent entry;

      // meta file
      ASSERT_EQ(OB_SUCCESS, databuff_printf(entry.d_name, sizeof(entry.d_name), "%s%s",
                                            OB_ADAPTIVELY_APPENDABLE_FRAGMENT_PREFIX,
                                            OB_ADAPTIVELY_APPENDABLE_SEAL_META));
      ASSERT_EQ(OB_SUCCESS, op.func(&entry));
      // foramt file
      ASSERT_EQ(OB_SUCCESS, databuff_printf(entry.d_name, sizeof(entry.d_name), "%s%s",
                                            OB_ADAPTIVELY_APPENDABLE_FRAGMENT_PREFIX,
                                            OB_ADAPTIVELY_APPENDABLE_FORMAT_META));
      ASSERT_EQ(OB_SUCCESS, op.func(&entry));

      // invalid fragment name
      const char *invalid_fragments[] = {"-2--1", "-1-1", "2--2", "3-2", "xxx"};
      for (int64_t i = 0; i < sizeof(invalid_fragments) / sizeof(char *); i++) {
        ASSERT_TRUE(sizeof(entry.d_name) >= strlen(invalid_fragments[i]) + 1);
        STRCPY(entry.d_name, invalid_fragments[i]);
        ASSERT_EQ(OB_SUCCESS, databuff_printf(entry.d_name, sizeof(entry.d_name), "%s%s",
                                              OB_ADAPTIVELY_APPENDABLE_FRAGMENT_PREFIX,
                                              invalid_fragments[i]));
        ASSERT_EQ(OB_INVALID_ARGUMENT, op.func(&entry));
      }
    }

    {
      // empty appendable object
      const char *fragments[] = {};
      int64_t n_fragments = sizeof(fragments) / sizeof(char *);
      int64_t expected_start[] = {};
      int64_t expected_end[] = {};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 0;
      ObStorageObjectMeta appendable_obj_meta;
      ASSERT_EQ(OB_SUCCESS, test_gen_object_meta(fragments, n_fragments, n_remained_fragments, 
        expected_start, expected_end, expected_file_length, appendable_obj_meta));
      
      ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(-1, 1, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(5, 4, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(5, 5, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(0, 1, fragments_need_to_read));
    }

    {
      // one fragment
      const char *fragments[] = {"10-20.back"};
      int64_t n_fragments = sizeof(fragments) / sizeof(char *);
      int64_t expected_start[] = {10};
      int64_t expected_end[] = {20};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 20;
      ObStorageObjectMeta appendable_obj_meta;
      ASSERT_EQ(OB_SUCCESS, test_gen_object_meta(fragments, n_fragments, n_remained_fragments, 
        expected_start, expected_end, expected_file_length, appendable_obj_meta));
      
      ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 8, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 10, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 15, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(11, 11, fragments_need_to_read));
      
      {
        int64_t start = 10;
        int64_t end = 18;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 15;
        int64_t end = 18;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 19;
        int64_t end = 20;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 19;
        int64_t end = 21;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 20;
        int64_t end = 21;
        int64_t expected_start[] = {};
        int64_t expected_end[] = {};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
    }
    
    {
      // valid fragment name
      const char *valid_fragments[] = {
          OB_ADAPTIVELY_APPENDABLE_FORMAT_META, OB_ADAPTIVELY_APPENDABLE_SEAL_META,
          "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",    // covered by "1-7"
          "0-3", "0-3", "0-1", "1-2", "2-3",                  // covered "0-3"
          "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",    // no gap
          // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
          "15-25", "22-25", "21-24", "26-30",   
          "30-1234567", "10000-1234567", "28-1234566"
      };
      int64_t n_fragments = sizeof(valid_fragments) / sizeof(char *);
      int64_t expected_start[] = {0, 1, 7, 8, 9, 10, 11, 12, 15, 26, 30};
      int64_t expected_end[] = {3, 7, 8, 9, 10, 11, 12, 20, 25, 30, 1234567};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 1234567;
      ObStorageObjectMeta appendable_obj_meta;
      // (ObStorageObjectMetaType::OB_OBJ_SIMULATE_APPEND);
      ASSERT_EQ(OB_SUCCESS, test_gen_object_meta(valid_fragments, n_fragments, n_remained_fragments, 
        expected_start, expected_end, expected_file_length, appendable_obj_meta));
      
      {
        int64_t start = 0;
        int64_t end = 3;
        int64_t expected_start[] = {0};
        int64_t expected_end[] = {3};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 1;
        int64_t end = 4;
        int64_t expected_start[] = {1};
        int64_t expected_end[] = {7};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 0;
        int64_t end = 15;
        int64_t expected_start[] = {0, 1, 7, 8, 9, 10, 11, 12};
        int64_t expected_end[] = {3, 7, 8, 9, 10, 11, 12, 20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
      {
        ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
        ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(15, 30, fragments_need_to_read));      
      }
      {
        int64_t start = 28;
        int64_t end = 2345678;
        int64_t expected_start[] = {26, 30};
        int64_t expected_end[] = {30, 1234567};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start, 
                  expected_end, appendable_obj_meta));
      }
    }
  }
}

TEST_P(TestObjectStorage, test_check_storage_obj_meta)
{
  int ret = OB_SUCCESS;
  if (enable_test_ && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_append_dir = "test_check_storage_obj_meta";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_append_dir, ts));
    
    {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri),
                                            "%s/test_appendable_%ld.back",
                                            dir_uri_, ObTimeUtility::current_time()));
      // util before append
      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
      const char *content = "123456789ABC";
      bool is_obj_exist = true;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);
      is_obj_exist = true;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, true/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);

      // util after append
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(content, strlen(content), 0));
      is_obj_exist = true;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      if (is_adaptive_append_mode(info_base_)) {
        ASSERT_FALSE(is_obj_exist);
      } else {
        ASSERT_TRUE(is_obj_exist);
        is_obj_exist = false;
      }
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, true/*is_adaptive*/, is_obj_exist));
      ASSERT_TRUE(is_obj_exist);

      int64_t file_length = -1;
      if (is_adaptive_append_mode(info_base_)) {
        ASSERT_EQ(OB_OBJECT_NOT_EXIST, util.get_file_length(uri, false/*is_adaptive*/, file_length));
      } else {
        ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, false/*is_adaptive*/, file_length));
        ASSERT_EQ(strlen(content), file_length);
        file_length = -1;
      }
      ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, true/*is_adaptive*/, file_length));
      ASSERT_EQ(strlen(content), file_length);

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);
      is_obj_exist = true;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, true/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);

      // open twice
      ASSERT_EQ(OB_INIT_TWICE, util.open(&info_base_));
      util.close();

      // invalid storage info
      ASSERT_EQ(OB_INVALID_ARGUMENT, util.open(NULL));
    }
  }
}


void test_read_appendable_object(const char *content, const int64_t content_length,
    const int64_t n_run, const int64_t *read_start, const int64_t *read_end,
    ObStorageAdaptiveReader &reader)
{
  int64_t read_size = -1;
  int64_t expected_read_size = -2;
  char buf[content_length];
  for (int64_t i = 0; i < n_run; i++) {
    read_size = -1;
    expected_read_size = MIN(read_end[i], content_length) - MIN(read_start[i], content_length);
    ASSERT_TRUE(read_start[i] <= read_end[i]);
    ASSERT_EQ(OB_SUCCESS,
        reader.pread(buf, read_end[i] - read_start[i], read_start[i], read_size));
    ASSERT_EQ(expected_read_size, read_size);
    ASSERT_EQ(0, memcmp(buf, content + read_start[i], read_size));
  }
}

TEST_P(TestObjectStorage, test_append_rw)
{
  int ret = OB_SUCCESS;
  if (enable_test_ && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_append_dir = "test_append";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_append_dir, ts));
    
    {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri_, ObTimeUtility::current_time()));
      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
      ASSERT_EQ(OB_INVALID_ARGUMENT, appender.pwrite(NULL, 10, 0));
      ASSERT_EQ(OB_INVALID_ARGUMENT, appender.pwrite("1", 0, 0));
      ASSERT_EQ(OB_INVALID_ARGUMENT, appender.pwrite("1", 1, -1));

      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_OBJECT_NOT_EXIST, reader.open(uri, &info_base_));
    }

    {
      ObStorageAppender appender;
      const char write_content[] = "123";
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/a.b/%ld.back",
                                            dir_uri_, ObTimeUtility::current_time()));
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, strlen(write_content), 0));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      ASSERT_EQ(OB_SUCCESS, appender.close());

      ObStorageAdaptiveReader reader;
      char read_buf[5] = {0};
      int64_t read_size = 0;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 5, 0, read_size));
      ASSERT_EQ(strlen(write_content), read_size);
      ASSERT_EQ('2', read_buf[1]);
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }

    {
      ObStorageAppender appender;
      const char write_content[] = "123";
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/a.b/%ld",
                                            dir_uri_, ObTimeUtility::current_time()));
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, strlen(write_content), 0));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      ASSERT_EQ(OB_SUCCESS, appender.close());

      ObStorageAdaptiveReader reader;
      char read_buf[5] = {0};
      int64_t read_size = 0;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 5, 0, read_size));
      ASSERT_EQ(strlen(write_content), read_size);
      ASSERT_EQ('2', read_buf[1]);
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }

    {
      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri_, ObTimeUtility::current_time()));
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
      // first append
      const char first_write[] = "123";
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(first_write, strlen(first_write), 0));
      ASSERT_EQ(strlen(first_write), appender.get_length());

      // second append
      const char second_write[] = "4567";
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(second_write, strlen(second_write), strlen(first_write)));
      ASSERT_EQ(strlen(first_write) + strlen(second_write), appender.get_length());

      // check size
      int64_t file_length = 0;
      ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, true/*is_adaptive*/, file_length));
      ASSERT_EQ(strlen(first_write) + strlen(second_write), file_length);

      // check data and clean
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      ASSERT_EQ(strlen(first_write) + strlen(second_write), reader.get_length());
      char read_buf[5] = {0};
      int64_t read_size = 0;
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 5, 2, read_size));
      ASSERT_EQ('3', read_buf[0]);
      ASSERT_EQ('7', read_buf[4]);
      ASSERT_EQ(5, read_size);
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      ASSERT_EQ(OB_SUCCESS, appender.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
      ASSERT_FALSE(appender.is_opened());
    }
    
    if (is_adaptive_append_mode(info_base_)) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri_, ObTimeUtility::current_time()));

      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
    
      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
      // "15-25", "22-25", "21-24", "26-30", "30-100", "28-100" 
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,26,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        ASSERT_EQ(OB_SUCCESS, appender.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
      }
      ASSERT_EQ(content_length, appender.get_length());

      // read before close
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      ASSERT_EQ(content_length, reader.get_length());
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101};
      ASSERT_EQ(sizeof(read_start), sizeof(read_end));
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
    
      char buf[content_length];
      int64_t read_size = -1;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(content_length, reader.get_length());
      {
        ASSERT_EQ(OB_INVALID_ARGUMENT, reader.pread(buf, 0, 0, read_size));
        ASSERT_EQ(OB_INVALID_ARGUMENT, reader.pread(buf, 1, -1, read_size));
        ASSERT_EQ(OB_INVALID_ARGUMENT, reader.pread(NULL, 1, 0, read_size));
      }
    
      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");
      ASSERT_EQ(OB_SUCCESS, appender.close());
      // open before close, read after close
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender.close());
      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }

    if (is_adaptive_append_mode(info_base_)) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri_, ObTimeUtility::current_time()));

      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
    
      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
      // "15-25", "22-25", "21-24", "26-30", "30-100", "28-100" 
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,26,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        ASSERT_EQ(OB_SUCCESS, appender.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
      }
      ASSERT_EQ(content_length, appender.get_length());

      // read before close
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      ASSERT_EQ(content_length, reader.get_length());
    
      char buf[content_length];
      int64_t read_size = -1;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // fill gap
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(content + 25, 1, 25));
      
      // now gap is filled
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // test appendable put is valid
      // ObStorageUtil s3_util;
      // ObStorageObjectMeta appendable_obj_meta;
      // ASSERT_EQ(OB_SUCCESS, s3_util.open(&info_base_));
      // ASSERT_EQ(OB_SUCCESS, s3_util.list_appendable_file_fragments(uri, appendable_obj_meta));
      // ASSERT_EQ(1, appendable_obj_meta.fragment_metas_.count());
      // ASSERT_EQ(0, appendable_obj_meta.fragment_metas_[0].start_);
      // ASSERT_EQ(9223372036854775807, appendable_obj_meta.fragment_metas_[0].end_);
      // ASSERT_EQ(content_length, appendable_obj_meta.length_);
    
      ASSERT_EQ(OB_SUCCESS, appender.close());
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      // open before close, read after close
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101};
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      // ASSERT_EQ(content_length - 1, reader.get_appendable_object_size());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender.close());

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      // ASSERT_EQ(content_length - 1, reader.get_appendable_object_size());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }

    if (is_adaptive_append_mode(info_base_)) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri_, ObTimeUtility::current_time()));

      ObStorageAppender appender_a;
      ObStorageAppender appender_b;
      ObStorageAppender appender_c;
      ASSERT_EQ(OB_SUCCESS, appender_a.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, appender_b.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, appender_c.open(uri, &info_base_));
    
      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25"
      // "15-25", "22-25", "21-24", "25-30", "30-100", "28-100" 
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,25,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        if (i % 3 == 0) {
          ASSERT_EQ(OB_SUCCESS, appender_a.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        } else if (i % 3 == 1) {
          ASSERT_EQ(OB_SUCCESS, appender_b.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        } else {
          ASSERT_EQ(OB_SUCCESS, appender_c.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        }
      }

      // read before close
      ObStorageAdaptiveReader reader_a;
      ObStorageAdaptiveReader reader_b;
      ASSERT_EQ(OB_SUCCESS, reader_a.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, reader_b.open(uri, &info_base_));
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26, 0};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101, 100};
      ASSERT_EQ(sizeof(read_start), sizeof(read_end));
      int64_t n_run = sizeof(read_start) / sizeof(int64_t);
      std::thread read_thread_a(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_b(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_a.join();
      read_thread_b.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());
    
      // open before close, read after close
      ASSERT_EQ(OB_SUCCESS, appender_a.close());
      ASSERT_EQ(OB_SUCCESS, appender_b.close());
      ASSERT_EQ(OB_SUCCESS, appender_c.close());
      std::thread read_thread_c(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_d(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_c.join();
      read_thread_d.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());
      ASSERT_EQ(OB_SUCCESS, reader_a.close());
      ASSERT_EQ(OB_SUCCESS, reader_b.close());

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader_a.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, reader_b.open(uri, &info_base_));
      std::thread read_thread_e(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_f(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_e.join();
      read_thread_f.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender_a.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, appender_a.seal_for_adaptive());
      std::thread read_thread_g(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_h(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_g.join();
      read_thread_h.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());
      ASSERT_EQ(OB_SUCCESS, reader_a.close());
      ASSERT_EQ(OB_SUCCESS, reader_b.close());
      ASSERT_EQ(OB_SUCCESS, appender_a.close());

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader_a.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, reader_b.open(uri, &info_base_));
      std::thread read_thread_i(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_j(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_i.join();
      read_thread_j.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());
      ASSERT_EQ(OB_SUCCESS, reader_a.close());
      ASSERT_EQ(OB_SUCCESS, reader_b.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }
  }
}

TEST_P(TestObjectStorage, test_timeout)
{
  int ret = OB_SUCCESS;
  if (enable_test_ 
      && info_base_.get_type() == ObStorageType::OB_STORAGE_S3
      && GetParam().extension_ == nullptr) {
    const char *tmp_dir = "test_timeout";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    
    ObObjectStorageInfo info;
    const Config &config = GetParam();
    // 8.8.8.8 is an IP for Google's Public DNS service, not a HTTP service endpoint.
    // This implies that an HTTP request to this IP is not expected to receive a response,
    // thereby serving as a method to trigger and test the timeout behavior of a `curl` request.
    // It is designed to ensure we can test the timeout logic without the need for a real HTTP server.
    ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                           "8.8.8.8",
                                           config.ak_,
                                           config.sk_, config.appid_, config.region_,
                                           "addressing_model=path_style", info));
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info));
    ASSERT_EQ(OB_TIMEOUT, util.del_file(dir_uri_));
  }
}

TEST_P(TestObjectStorage, test_recursive_tag)
{
  int ret = OB_SUCCESS;
  if (enable_test_
      && GetParam().extension_ == nullptr
      && info_base_.get_type() != OB_STORAGE_FILE
      && 0 != STRNCMP(GetParam().storage_type_, "GCS", strlen("GCS"))) {
    const char *tmp_dir = "test_recursive_tag";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));

    ObObjectStorageInfo info;
    const Config &config = GetParam();
    ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                           config.endpoint_,
                                           config.ak_,
                                           config.sk_, config.appid_, config.region_,
                                           "delete_mode=tagging", info));
    
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_recursive_tag", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "test", 4));
    
    // first tag
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri, false/*is_adaptive*/));
    bool is_tagging = false;
    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_TRUE(is_tagging);

    // second tag
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri, false/*is_adaptive*/));
    is_tagging = false;
    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_TRUE(is_tagging);

    util.close();
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri, false/*is_adaptive*/));
  }
}

TEST_P(TestObjectStorage, test_read_single_file)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_read_single_file";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.mkdir(dir_uri_));

    {
      // normal
      ObStorageWriter writer;
      ObStorageReader reader;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_normal", dir_uri_));

      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, writer.write("123456", 6));
      ASSERT_EQ(OB_SUCCESS, writer.close());
      int64_t read_size = -1;
      char read_buf[100];
      memset(read_buf, 0, sizeof(read_buf));
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      // offset == file length, read_size should == 0
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 100, 6, read_size));
      ASSERT_EQ(0, read_size);

      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 100, 1, read_size));
      ASSERT_EQ(5, read_size);
      ASSERT_EQ(0, strncmp(read_buf, "23456", 5));
      
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 100, 0, read_size));
      ASSERT_EQ(6, read_size);
      ASSERT_EQ(0, strncmp(read_buf, "123456", 6));
      ASSERT_EQ(OB_SUCCESS, reader.close());
      
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

      // write an empty object
      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, writer.write("123456", 0));
      ASSERT_EQ(OB_SUCCESS, writer.close());
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
      ASSERT_EQ(0, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }

    {
      // appendable
      ObStorageAppender appender;
      ObStorageAdaptiveReader reader;

      const char *write_content = "0123456789";
      const int64_t first_content_len = 6;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_appendable", dir_uri_));
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, first_content_len, 0));

      int64_t read_size = -1;
      char read_buf[100];
      memset(read_buf, 0, sizeof(read_buf));
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));

      ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, 1, first_content_len));
      ASSERT_EQ(OB_INVALID_ARGUMENT, appender.pwrite(write_content, 0, first_content_len + 1));

      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 100, 0, read_size));
      if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
        ASSERT_EQ(first_content_len + 1, read_size);
      } else {
        ASSERT_EQ(first_content_len, read_size);
      }
      ASSERT_EQ(0, strncmp(read_buf, write_content, first_content_len));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true/*is_adaptive*/));
    }
  }
}

TEST_P(TestObjectStorage, test_multipart_write)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_multipart_write_files";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.mkdir(dir_uri_));
    
    const int64_t content_size = 20 * 1024 * 1024L + 7; // 20M + 7B
    ObArenaAllocator allocator;
    char *write_buf = (char *)allocator.alloc(content_size);
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    for (int64_t i = 0; i < content_size - 1; i++) {
      write_buf[i] = alphanum[ObRandom::rand(0, sizeof(alphanum) - 2)];
    }
    write_buf[content_size - 1] = '\0';
    
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_multipart", dir_uri_));

    {
      // unordered pwrite
      ObStorageMultiPartWriter writer;
      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
      ASSERT_EQ(OB_ERR_UNEXPECTED, writer.pwrite(write_buf, content_size, 1));
      ASSERT_EQ(OB_SUCCESS, writer.pwrite(write_buf, content_size, 0));
      ASSERT_EQ(OB_ERR_UNEXPECTED, writer.pwrite(write_buf, content_size, content_size - 1));
      ASSERT_EQ(OB_ERR_UNEXPECTED, writer.pwrite(write_buf, content_size, content_size + 1));
      ASSERT_EQ(OB_SUCCESS, writer.abort());
      ASSERT_EQ(OB_SUCCESS, writer.close());
    }

    {
      // test abort
      bool is_obj_exist = true;
      ObStorageMultiPartWriter writer;
      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, writer.abort());
      ASSERT_EQ(OB_SUCCESS, writer.close());
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);

      // complete empty writer
      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, writer.complete());
      ASSERT_EQ(OB_SUCCESS, writer.close());
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      ASSERT_TRUE(is_obj_exist);
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, false/*is_adaptive*/));

      // write an empty object
      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, writer.pwrite("", 0, 0));
      ASSERT_EQ(OB_SUCCESS, writer.complete());
      ASSERT_EQ(OB_SUCCESS, writer.close());
      is_obj_exist = false;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      ASSERT_TRUE(is_obj_exist);
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, false/*is_adaptive*/));
    }

    ObStorageMultiPartWriter writer;
    // ObStorageWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
    ASSERT_EQ(OB_SUCCESS, writer.pwrite(write_buf, content_size, 0));
    ASSERT_EQ(content_size, writer.get_length());
    ASSERT_EQ(OB_SUCCESS, writer.complete());
    ASSERT_EQ(OB_SUCCESS, writer.close());
    OB_LOG(INFO, "-----------------------------------------------------------------------------");

    ObStorageReader reader;
    int64_t read_size = -1;
    const int64_t read_start = 1024;
    const int64_t read_buf_size = 1024;
    char read_buf[read_buf_size];
    memset(read_buf, 0, sizeof(read_buf));
    ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base_));
    ASSERT_EQ(content_size, reader.get_length());
    ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, read_buf_size, read_start, read_size));
    ASSERT_EQ(read_buf_size, read_size);
    ASSERT_EQ(0, memcmp(write_buf + read_start, read_buf, read_size));
    ASSERT_EQ(OB_SUCCESS, reader.close());
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
  }
}

int sequential_upload(
    ObStorageDirectMultiPartWriter &writer,
    const char *write_buf,
    const int64_t content_size,
    const int64_t part_size_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_buf)
      || OB_UNLIKELY(!writer.is_opened() || content_size <= 0 || part_size_threshold <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments",
        K(ret), KP(write_buf), K(part_size_threshold), K(content_size));
  } else {
    bool is_full = false;
    bool is_exist = false;
    int64_t part_id = -1;
    int64_t cur_offset = 0;
    while (OB_SUCC(ret) && cur_offset < content_size) {
      int64_t part_size = MIN(part_size_threshold, content_size - cur_offset);
      is_full = false;
      is_exist = false;
      part_id = -1;
      
      if (OB_FAIL(writer.buf_append_part(write_buf + cur_offset, part_size,
                                         500, is_full))) {
        OB_LOG(WARN, "fail to buf_append_part", K(ret), K(cur_offset), K(part_size));
      } else if (is_full) {
        if (OB_FAIL(writer.get_part_id(is_exist, part_id))) {
          OB_LOG(WARN, "fail to get_part_id", K(ret), K(cur_offset), K(part_size));
        } else if (is_exist) {
          if (OB_FAIL(writer.upload_part(write_buf + cur_offset, part_size, part_id))) {
            OB_LOG(WARN, "fail to upload part", K(ret), K(cur_offset), K(part_size), K(part_id));
          }
        }
      }
      cur_offset += part_size;
    }
    
    if (typeid(writer) == typeid(ObStorageBufferedMultiPartWriter)) {
      if (OB_FAIL(writer.get_part_id(is_exist, part_id))) {
        OB_LOG(WARN, "fail to get_part_id", K(ret), K(cur_offset));
      } else if (is_exist) {
        if (OB_FAIL(writer.upload_part(write_buf + cur_offset, 0, part_id))) {
          OB_LOG(WARN, "fail to upload part", K(ret), K(cur_offset), K(part_id));
        }
      }
    }
  }
  return ret;
}

int reverse_upload(
    ObStorageDirectMultiPartWriter &writer,
    const char *write_buf,
    const int64_t content_size,
    const int64_t part_size_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_buf)
      || OB_UNLIKELY(!writer.is_opened() || content_size <= 0 || part_size_threshold <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments",
        K(ret), KP(write_buf), K(part_size_threshold), K(content_size));
  } else {
    bool is_full = false;
    bool is_exist = false;
    int64_t part_id = -1;
    int64_t cur_offset = 0;
    std::vector<ObStorageBufferedMultiPartWriter::PartData> parts;
    while (OB_SUCC(ret) && cur_offset < content_size) {
      int64_t part_size = MIN(part_size_threshold, content_size - cur_offset);
      is_full = false;
      is_exist = false;
      part_id = -1;

      if (OB_FAIL(writer.buf_append_part(write_buf + cur_offset, part_size,
                                         500, is_full))) {
        OB_LOG(WARN, "fail to buf_append_part", K(ret), K(cur_offset), K(part_size));
      } else if (is_full) {
        if (OB_FAIL(writer.get_part_id(is_exist, part_id))) {
          OB_LOG(WARN, "fail to get_part_id", K(ret), K(cur_offset), K(part_size));
        } else if (is_exist) {
          ObStorageBufferedMultiPartWriter::PartData part_data;
          part_data.data_ = const_cast<char *>(write_buf + cur_offset);
          part_data.size_ = part_size;
          parts.push_back(part_data);

          if (OB_UNLIKELY(parts.size() != part_id)) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "part id is unexpected", K(ret), K(part_id), K(parts.size()));
          }
        }
      }
      cur_offset += part_size;
    }
    is_exist = false;
    part_id = -1;
    if (typeid(writer) == typeid(ObStorageBufferedMultiPartWriter)) {
      if (OB_FAIL(writer.get_part_id(is_exist, part_id))) {
        OB_LOG(WARN, "fail to get_part_id", K(ret), K(cur_offset));
      } else if (is_exist) {
        ObStorageBufferedMultiPartWriter::PartData part_data;
        part_data.data_ = const_cast<char *>(write_buf + cur_offset);
        part_data.size_ = 0;
        parts.push_back(part_data);

        if (OB_UNLIKELY(parts.size() != part_id)) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "part id is unexpected", K(ret), K(part_id), K(parts.size()));
        }
      }
    }

    for (int64_t i = parts.size() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_FAIL(writer.upload_part(parts[i].data_, parts[i].size_, i + 1))) {
        OB_LOG(WARN, "fail to upload part", K(ret), K(i), K(parts[i].size_));
      }
    }
  }
  return ret;
}

int random_upload(
    ObStorageDirectMultiPartWriter &writer,
    const char *write_buf,
    const int64_t content_size,
    const int64_t part_size_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_buf)
      || OB_UNLIKELY(!writer.is_opened() || content_size <= 0 || part_size_threshold <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments",
        K(ret), K(writer.is_opened()), KP(write_buf), K(part_size_threshold), K(content_size));
  } else {
    bool is_full = false;
    bool is_exist = false;
    int64_t part_id = -1;
    int64_t cur_offset = 0;
    int64_t thread_num = content_size / part_size_threshold + 1;
    std::thread t[thread_num];
    int i = 0;
    while (OB_SUCC(ret) && cur_offset < content_size) {
      int64_t part_size = MIN(part_size_threshold, content_size - cur_offset);
      is_full = false;
      is_exist = false;
      part_id = -1;

      if (OB_FAIL(writer.buf_append_part(write_buf + cur_offset, part_size,
                                         500, is_full))) {
        OB_LOG(WARN, "fail to buf_append_part", K(ret), K(cur_offset), K(part_size));
      } else if (is_full) {
        if (OB_FAIL(writer.get_part_id(is_exist, part_id))) {
          OB_LOG(WARN, "fail to get_part_id", K(ret), K(cur_offset), K(part_size));
        } else if (is_exist) {
          t[i] = std::thread(&ObStorageDirectMultiPartWriter::upload_part, &writer,
                             write_buf + cur_offset, part_size, part_id);
          i++;
          if (OB_UNLIKELY(i != part_id)) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "part id is unexpected", K(ret), K(part_id), K(i));
          }
        }
      }
      cur_offset += part_size;
    }
    is_exist = false;
    part_id = -1;
    if (typeid(writer) == typeid(ObStorageBufferedMultiPartWriter)) {
      if (OB_FAIL(writer.get_part_id(is_exist, part_id))) {
        OB_LOG(WARN, "fail to get_part_id", K(ret), K(cur_offset));
      } else if (is_exist) {
        t[i] = std::thread(&ObStorageDirectMultiPartWriter::upload_part, &writer,
                           write_buf + cur_offset, 0, part_id);
        i++;
        if (OB_UNLIKELY(i != part_id)) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "part id is unexpected", K(ret), K(part_id), K(i));
        }
      }
    }

    for (int j = 0; j < i; ++j) {
      t[j].join();
    }
  }
  return ret;
}

int check_multipart_object(
    const ObString &uri, 
    ObObjectStorageInfo *storage_info,
    const char *write_buf, 
    const int64_t content_size)
{
  int ret = OB_SUCCESS;
  ObStorageReader reader;
  char *read_buf = nullptr;
  int64_t read_size = -1;
  ObArenaAllocator allocator;

  if (OB_UNLIKELY(uri.empty() || content_size <= 0) 
      || OB_ISNULL(storage_info) || OB_ISNULL(write_buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri), KPC(storage_info), KP(write_buf));
  } else if (OB_FAIL(reader.open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open reader", K(ret), K(uri), KPC(storage_info));
  } else if (OB_UNLIKELY(reader.get_length() != content_size)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected length", K(ret), K(uri), K(reader.get_length()), K(content_size));
  } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(content_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc memory", K(ret), K(content_size));
  } else if (OB_FAIL(reader.pread(read_buf, content_size, 0, read_size))) {
    OB_LOG(WARN, "fail to pread", K(ret), K(uri), K(content_size));
  } else if (OB_UNLIKELY(content_size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected read size", K(ret), K(uri), K(content_size), K(read_size));
  } else if (OB_UNLIKELY(0 != MEMCMP(write_buf, read_buf, content_size))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected data", K(ret), K(uri), K(content_size), K(content_size));
  } else {
    // no need to check close error
    reader.close();
  }
  return ret;
}

int do_complete_check(
    const ObString &uri, 
    ObObjectStorageInfo *storage_info,
    ObStorageDirectMultiPartWriter &writer,
    const char *write_buf,
    const int64_t content_size,
    const int64_t part_size_threshold)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  if (OB_UNLIKELY(writer.is_opened() || part_size_threshold <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments",
        K(ret), K(writer.is_opened()), K(part_size_threshold));
  } else if (OB_UNLIKELY(uri.empty() || content_size <= 0) 
      || OB_ISNULL(storage_info) || OB_ISNULL(write_buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri), KPC(storage_info), KP(write_buf));
  } else if (OB_FAIL(util.open(storage_info))) {
    OB_LOG(WARN, "fail to open util", K(ret), KPC(storage_info));
  }

  OB_LOG(INFO, "========================= sequential =========================");
  if (FAILEDx(writer.open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open", K(ret));
  } else if (OB_FAIL(sequential_upload(writer, write_buf, content_size, part_size_threshold))) {
    OB_LOG(WARN, "fail to sequential_upload", K(ret));
  } else if (OB_UNLIKELY(content_size != writer.get_length())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "fail to check length", K(ret), K(content_size), K(writer.get_length()));
  } else if (OB_FAIL(writer.complete())) {
    OB_LOG(WARN, "fail to complete", K(ret));
  } else if (OB_FAIL(writer.close())) {
    OB_LOG(WARN, "fail to close", K(ret));
  } else if (OB_FAIL(check_multipart_object(uri, storage_info, write_buf, content_size))) {
    OB_LOG(WARN, "fail to check_multipart_object", K(ret));
  } else if (OB_FAIL(util.del_file(uri))) {
    OB_LOG(WARN, "fail to del_file", K(ret));
  }

  OB_LOG(INFO, "========================= reverse =========================");
  if (FAILEDx(writer.open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open", K(ret));
  } else if (OB_FAIL(reverse_upload(writer, write_buf, content_size, part_size_threshold))) {
    OB_LOG(WARN, "fail to reverse_upload", K(ret));
  } else if (OB_UNLIKELY(content_size != writer.get_length())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "fail to check length", K(ret), K(content_size), K(writer.get_length()));
  } else if (OB_FAIL(writer.complete())) {
    OB_LOG(WARN, "fail to complete", K(ret));
  } else if (OB_FAIL(writer.close())) {
    OB_LOG(WARN, "fail to close", K(ret));
  } else if (OB_FAIL(check_multipart_object(uri, storage_info, write_buf, content_size))) {
    OB_LOG(WARN, "fail to check_multipart_object", K(ret));
  } else if (OB_FAIL(util.del_file(uri))) {
    OB_LOG(WARN, "fail to del_file", K(ret));
  }

  OB_LOG(INFO, "========================= random =========================");
  if (FAILEDx(writer.open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open", K(ret));
  } else if (OB_FAIL(random_upload(writer, write_buf, content_size, part_size_threshold))) {
    OB_LOG(WARN, "fail to random_upload", K(ret));
  } else if (OB_UNLIKELY(content_size != writer.get_length())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "fail to check length", K(ret), K(content_size), K(writer.get_length()));
  } else if (OB_FAIL(writer.complete())) {
    OB_LOG(WARN, "fail to complete", K(ret));
  } else if (OB_FAIL(writer.close())) {
    OB_LOG(WARN, "fail to close", K(ret));
  } else if (OB_FAIL(check_multipart_object(uri, storage_info, write_buf, content_size))) {
    OB_LOG(WARN, "fail to check_multipart_object", K(ret));
  } else if (OB_FAIL(util.del_file(uri))) {
    OB_LOG(WARN, "fail to del_file", K(ret));
  }
  
  return ret;
}

TEST_P(TestObjectStorage, test_parallel_multipart_write)
{ 
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_parallel_multipart_write";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.mkdir(dir_uri_));
    
    const int64_t content_size = 40 * 1024 * 1024L + 7;
    ObArenaAllocator allocator;
    char *write_buf = (char *)allocator.alloc(content_size);
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    for (int64_t i = 0; i < content_size - 1; i++) {
      write_buf[i] = alphanum[ObRandom::rand(0, sizeof(alphanum) - 2)];
    }
    write_buf[content_size - 1] = '\0';
    
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_multipart", dir_uri_));
    {
      OB_LOG(INFO, "=================== Test complete and aobrt ===================");
      bool is_full = false;
      bool is_exist = false;
      int64_t part_id = -1;
      ObStorageDirectMultiPartWriter direct_writer;
      ObStorageBufferedMultiPartWriter buffered_writer;
      std::vector<ObStorageDirectMultiPartWriter *> writer_lsit = {&direct_writer, &buffered_writer};
      for (auto writer : writer_lsit) {
        // abort an empty multipart upload
        ASSERT_EQ(OB_SUCCESS, writer->open(uri, &info_base_));
        ASSERT_EQ(OB_SUCCESS, writer->abort());
        ASSERT_EQ(OB_SUCCESS, writer->close());

        // complete an empty multipart upload
        ASSERT_EQ(OB_SUCCESS, writer->open(uri, &info_base_));
        ASSERT_EQ(OB_SUCCESS, writer->complete());
        ASSERT_EQ(OB_SUCCESS, writer->close());
        int64_t len = -1;
        ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, false/*is_adaptive*/, len));
        ASSERT_EQ(0, len);

        // abort
        is_full = false;
        is_exist = false;
        part_id = -1;
        ASSERT_EQ(OB_SUCCESS, writer->open(uri, &info_base_));
        ASSERT_EQ(OB_SUCCESS, writer->buf_append_part("a", 1, 500, is_full));
        ASSERT_EQ(OB_SUCCESS, writer->get_part_id(is_exist, part_id));
        ASSERT_TRUE(is_exist);
        ASSERT_EQ(OB_SUCCESS, writer->upload_part("a", 1, part_id));
        ASSERT_EQ(OB_SUCCESS, writer->abort());
        ASSERT_EQ(OB_SUCCESS, writer->close());

        // abort without upload, so leave a part in map
        is_full = false;
        is_exist = false;
        part_id = -1;
        ASSERT_EQ(OB_SUCCESS, writer->open(uri, &info_base_));
        ASSERT_EQ(OB_SUCCESS, writer->buf_append_part("a", 1, 500, is_full));
        ASSERT_EQ(OB_SUCCESS, writer->get_part_id(is_exist, part_id));
        ASSERT_TRUE(is_exist);
        ASSERT_EQ(OB_SUCCESS, writer->abort());
        ASSERT_EQ(OB_SUCCESS, writer->close());

        // write only one part
        is_full = false;
        is_exist = false;
        part_id = -1;
        ASSERT_EQ(OB_SUCCESS, writer->open(uri, &info_base_));
        ASSERT_EQ(OB_SUCCESS, writer->buf_append_part("a", 1, 500, is_full));
        ASSERT_EQ(OB_SUCCESS, writer->get_part_id(is_exist, part_id));
        ASSERT_TRUE(is_exist);
        ASSERT_EQ(OB_SUCCESS, writer->upload_part("a", 1, part_id));
        ASSERT_EQ(OB_SUCCESS, writer->complete());
        ASSERT_EQ(OB_SUCCESS, writer->close());
        is_exist = false;
        ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
        ASSERT_TRUE(is_exist);
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }

    const int64_t part_size_threshold = ObStorageBufferedMultiPartWriter::PART_SIZE_THRESHOLD;
    {
      OB_LOG(INFO, "=================== Test ObStorageDirectMultiPartWriter ===================");
      ObStorageDirectMultiPartWriter writer;
      ASSERT_EQ(OB_SUCCESS,
          do_complete_check(uri, &info_base_, writer, write_buf, content_size, part_size_threshold));
    }

    {
      OB_LOG(INFO, "=================== Test ObStorageBufferedMultiPartWriter ===================");
      ObStorageBufferedMultiPartWriter writer;
      std::vector<int64_t> part_size_threshold_list = 
          {1, part_size_threshold - 1, part_size_threshold, part_size_threshold + 1};
      for (auto &cur_part_size : part_size_threshold_list) {
        OB_LOG(INFO, "=================== Test ObStorageBufferedMultiPartWriter size = ", K(cur_part_size));
        const int64_t used_size = (cur_part_size == 1 ? 10 : content_size);
        ASSERT_EQ(OB_SUCCESS,
            do_complete_check(uri, &info_base_, writer, write_buf, used_size, cur_part_size));
      }
    }
  }
}

TEST_P(TestObjectStorage, test_del_unmerged_parts)
{
  int ret = OB_SUCCESS;
  if (enable_test_ && false && GetParam().extension_ == nullptr) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_del_unmerged_parts";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    
    const int64_t content_size = 20 * 1024 * 1024L + 7; // 20M + 7B
    ObArenaAllocator allocator;
    char *write_buf = (char *)allocator.alloc(content_size);
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    for (int64_t i = 0; i < content_size - 1; i++) {
      write_buf[i] = alphanum[ObRandom::rand(0, sizeof(alphanum) - 2)];
    }
    write_buf[content_size - 1] = '\0';
    
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s", dir_uri_, tmp_dir));
    ObStorageMultiPartWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
    ASSERT_EQ(OB_SUCCESS, writer.pwrite(write_buf, content_size, 0));
    ASSERT_EQ(content_size, writer.get_length());

    ASSERT_EQ(OB_SUCCESS, util.del_unmerged_parts(uri));
    ASSERT_EQ(OB_OBJECT_STORAGE_IO_ERROR, writer.close());
  }
}

TEST_P(TestObjectStorage, test_wrong_endpoint)
{
  int ret = OB_SUCCESS;
  if (enable_test_ && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
    const char *tmp_dir = "test_wrong_endpoint";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));

    // wrong endpoint
    ObObjectStorageInfo info;
    const Config &config = GetParam();
    if (info_base_.get_type() == ObStorageType::OB_STORAGE_AZBLOB) {
      std::string endpoint = std::string(config.endpoint_);
      int pos = endpoint.find("://") + 3;
      ASSERT_EQ(true, pos < endpoint.size());
      std::string wrong_endpoint = endpoint.substr(0, pos) + "aa." + endpoint.substr(pos);
      ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                             wrong_endpoint.c_str(),
                                             config.ak_,
                                             config.sk_, config.appid_, config.region_,
                                             config.extension_, info));
    } else {
      ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                            (std::string("aa.") + config.endpoint_).c_str(),
                                            config.ak_,
                                            config.sk_, config.appid_, config.region_,
                                            config.extension_, info)); 
    }
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test", dir_uri_));
    ASSERT_EQ(OB_INVALID_OBJECT_STORAGE_ENDPOINT, util.write_single_file(uri, "test", 4));

    // empty bucket
    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      const char *prefix = nullptr;
      ASSERT_EQ(OB_SUCCESS, get_storage_prefix_from_path(config.bucket_, prefix));
      std::string tmp_bucket = prefix;
      tmp_bucket += "xxx";
      ASSERT_EQ(OB_INVALID_ARGUMENT, util.write_single_file(tmp_bucket.c_str(), "test", 4));
    }

    // wrong bucket
    info.reset();
    ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                           config.endpoint_,
                                           config.ak_,
                                           config.sk_, config.appid_, config.region_,
                                           config.extension_, info));
    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
      const char *prefix = nullptr;
      ASSERT_EQ(OB_SUCCESS, get_storage_prefix_from_path(config.bucket_, prefix));
      // add a prefix, need to ensure that this bucket does not exist
      std::string tmp_uri = std::string(prefix) + "wrong-" + std::string(config.bucket_ + strlen(prefix)) + "/"
                            + std::string(dir_name_) + "/test";
      ASSERT_EQ(OB_INVALID_OBJECT_STORAGE_ENDPOINT, util.write_single_file(tmp_uri.c_str(), "test", 4));
    }

    // do not set s3_region
    info.reset();
    ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                           config.endpoint_,
                                           config.ak_,
                                           config.sk_, config.appid_, nullptr,
                                           config.extension_, info));
    if (0 == MEMCMP(config.storage_type_, "S3", strlen("S3"))) {
      ASSERT_EQ(OB_S3_REGION_MISMATCH, util.write_single_file(uri, "test", 4));
      // wrong s3_region
      info.reset();
      ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                             config.endpoint_,
                                             config.ak_,
                                             config.sk_, config.appid_, "xxxx",
                                             config.extension_, info));
      ASSERT_EQ(OB_S3_REGION_MISMATCH, util.write_single_file(uri, "test", 4));
    } else {
      ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "test", 4));
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }
  }
}

TEST_P(TestObjectStorage, test_invalid_bucket_name)
{
  if (enable_test_ && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
    const char *wrong_bucket = nullptr;
    if (info_base_.get_type() == ObStorageType::OB_STORAGE_S3) {
      wrong_bucket = "s3://1";
    } else if (info_base_.get_type() == ObStorageType::OB_STORAGE_AZBLOB) {
      wrong_bucket = "azblob://wrong_bucket";
    }
    char tmp_uri[64] = {0};

    ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_uri, sizeof(tmp_uri), 
        "%s/test_invalid_bucket_name", wrong_bucket));
    ObObjectStorageInfo info;
    const Config &config = GetParam();
    ASSERT_EQ(OB_SUCCESS, set_storage_info(wrong_bucket,
                                           config.endpoint_,
                                           config.ak_,
                                           config.sk_, config.appid_, config.region_,
                                           config.extension_, info));
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info));
    ASSERT_EQ(OB_INVALID_OBJECT_STORAGE_ENDPOINT, util.write_single_file(tmp_uri, "test", 4));
  }
}

TEST_P(TestObjectStorage, test_batch_del_files)
{
  if (enable_test_ && GetParam().extension_ == nullptr) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_batch_del_files";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.mkdir(dir_uri_));
    
    ObArenaAllocator allocator;
    ObArray<ObString> files_to_delete;
    ObArray<int64_t> failed_files_idx;
    ASSERT_EQ(OB_INVALID_ARGUMENT, util.batch_del_files(files_to_delete, failed_files_idx));

    char *tmp_uri;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/a", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, ob_dup_cstring(allocator, uri, tmp_uri));
    ASSERT_EQ(OB_SUCCESS, files_to_delete.push_back(tmp_uri));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123", 3));
    
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/b", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, ob_dup_cstring(allocator, uri, tmp_uri));
    ASSERT_EQ(OB_SUCCESS, files_to_delete.push_back(tmp_uri));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123", 3));
    
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/c", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, ob_dup_cstring(allocator, uri, tmp_uri));
    ASSERT_EQ(OB_SUCCESS, files_to_delete.push_back(tmp_uri));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123", 3));

    // Add an object that does not exist
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/not_exist", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, ob_dup_cstring(allocator, uri, tmp_uri));
    ASSERT_EQ(OB_SUCCESS, files_to_delete.push_back(uri));

    ASSERT_EQ(OB_SUCCESS, util.batch_del_files(files_to_delete, failed_files_idx));
    ASSERT_TRUE(failed_files_idx.empty());
  }
}

TEST_P(TestObjectStorage, test_retry)
{
  // TODO @fangdan: add errsim cases to enable testing retry strategy
  if (enable_test_
      && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE
      && OB_ISNULL(GetParam().extension_)
      && false) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_retry";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    OK(databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld", tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_retry", dir_uri_));
    
    // 1us, 1s, 20s
    std::vector<int64_t> retry_timeout_us {1, 1000LL * 1000LL, 20LL * 1000LL * 1000LL};
    const int64_t disrupt_time_s = 5; // 5s
    for (const int64_t cur_timeout_us : retry_timeout_us) {
      ASSERT_GT(cur_timeout_us, 0);
      OB_STORAGE_MAX_IO_TIMEOUT_US = cur_timeout_us;
      std::thread t(ObObjectStorageUnittestUtil::disrupt_network, disrupt_time_s);
      sleep(1);
      if (cur_timeout_us < disrupt_time_s) {
        EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, "test", 4));
      } else {
        EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, "test", 4));
        EXPECT_EQ(OB_SUCCESS, util.del_file(uri));
      }
      t.join();
    }
  }
}

TEST_P(TestObjectStorage, test_s3_client_map)
{
  int ret = OB_SUCCESS;
  if (enable_test_ 
      && is_use_obdal() == false
      && info_base_.get_type() == ObStorageType::OB_STORAGE_S3) {
    const char *tmp_dir = "test_s3_client_map";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test", dir_uri_));
    
    const Config &config = GetParam();
    ObObjectStorageInfo info_use_http;
    ObObjectStorageInfo info_use_https;
    ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                           (std::string("http://") + config.endpoint_).c_str(),
                                           config.ak_,
                                           config.sk_, config.appid_, config.region_,
                                           config.extension_,
                                           info_use_http));
    ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                           (std::string("https://") + config.endpoint_).c_str(),
                                           config.ak_,
                                           config.sk_, config.appid_, config.region_,
                                           config.extension_,
                                           info_use_https));

    const int64_t init_s3_client_map_size = ObS3Env::get_instance().s3_client_map_.size();
    const int64_t s3_client_idle_duration_us = 1000LL * 1000LL; // 1s

    ASSERT_EQ(OB_INVALID_ARGUMENT, set_max_s3_client_idle_duration_us(-1));
    ASSERT_EQ(OB_INVALID_ARGUMENT, set_max_s3_client_idle_duration_us(0));
    ASSERT_EQ(OB_SUCCESS, set_max_s3_client_idle_duration_us(s3_client_idle_duration_us - 5000));

    {
      // test REACH_TIME_INTERVAL clean
      ob_usleep(s3_client_idle_duration_us);

      // insert two client first
      ObStorageUtil util;
      bool is_exist;
      ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
      util.close();
      ASSERT_EQ(OB_SUCCESS, util.open(&info_use_http));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
      util.close();
      ASSERT_EQ(2, ObS3Env::get_instance().s3_client_map_.size());

      // after s3_client_idle_duration_us, old clients will be cleaned
      ob_usleep(s3_client_idle_duration_us);
      ASSERT_EQ(OB_SUCCESS, util.open(&info_use_https));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
      util.close();
      ASSERT_EQ(1, ObS3Env::get_instance().s3_client_map_.size()); 
    }

    {
      // test client ref cnt
      // before a opened reader/writer close, ref cnt will not dec to 0
      ob_usleep(s3_client_idle_duration_us);
      ObStorageS3Writer writer;
      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
      ASSERT_GE(1, ObS3Env::get_instance().s3_client_map_.size());
      
      ob_usleep(s3_client_idle_duration_us);
      ObStorageUtil util;
      bool is_exist;
      ASSERT_EQ(OB_SUCCESS, util.open(&info_use_http));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
      util.close();
      // info_base_ + info_use_http
      ASSERT_GE(2, ObS3Env::get_instance().s3_client_map_.size());

      // before writer close, ref cnt = 1, so the client cannot be cleaned
      ob_usleep(s3_client_idle_duration_us);
      ASSERT_EQ(OB_SUCCESS, ObS3Env::get_instance().clean_s3_client_map_());
      ASSERT_GE(1, ObS3Env::get_instance().s3_client_map_.size());
      ObS3Client *s3_client = writer.s3_client_;
      ASSERT_EQ(1, s3_client->ref_cnt_);

      // after writer close
      ASSERT_EQ(OB_SUCCESS, writer.close());
      ASSERT_EQ(0, s3_client->ref_cnt_);
      ob_usleep(s3_client_idle_duration_us);
      ASSERT_EQ(OB_SUCCESS, ObS3Env::get_instance().clean_s3_client_map_());
      ASSERT_GE(0, ObS3Env::get_instance().s3_client_map_.size());
    }   
  }
}

TEST_P(TestObjectStorage, test_oss_wrong_endpoint)
{
  int ret = OB_SUCCESS;
  if (enable_test_
      && is_use_obdal() == false
      && GetParam().extension_ == nullptr) {
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest", dir_uri_));
    const Config &config = GetParam();
    std::vector<std::string> invalid_hosts
    {
      AOS_HTTP_PREFIX, AOS_HTTPS_PREFIX, 
      "/", "?", "#", "/Host#", "?Host#", "#Host#",
      std::string(AOS_HTTP_PREFIX) + "/",
      std::string(AOS_HTTP_PREFIX) + "?",
      std::string(AOS_HTTP_PREFIX) + "#",
      std::string(AOS_HTTPS_PREFIX) + "/",
      std::string(AOS_HTTPS_PREFIX) + "?",
      std::string(AOS_HTTPS_PREFIX) + "#",
    };

    ObObjectStorageInfo info;
    ObStorageReader reader;
    for (const std::string &host : invalid_hosts) {
      info.reset();
      ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                             host.c_str(),
                                             config.ak_,
                                             config.sk_, config.appid_, config.region_,
                                             config.extension_, info));
      ASSERT_EQ(OB_INVALID_ARGUMENT, reader.open(uri, &info));
    }
  }
}

TEST_P(TestObjectStorage, test_checksum_error)
{
  int ret = OB_SUCCESS;
  if (enable_test_ 
      && !is_use_obdal()
      && info_base_.get_checksum_type() == ObStorageChecksumType::OB_MD5_ALGO) {
        const char *tmp_dir = "test_read_single_file";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_normal", dir_uri_));

    EventItem item;
    item.trigger_freq_ = 1;
    item.error_code_ = OB_OBJECT_STORAGE_CHECKSUM_ERROR;
    ASSERT_EQ(OB_SUCCESS, EventTable::instance().set_event("EN_OBJECT_STORAGE_CHECKSUM_ERROR", item));

    ObStorageWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
    ASSERT_EQ(OB_OBJECT_STORAGE_IO_ERROR, writer.write("123456", 6));
    ASSERT_EQ(OB_SUCCESS, writer.close());
      
    item.error_code_ = 0;
    ASSERT_EQ(OB_SUCCESS, EventTable::instance().set_event("EN_OBJECT_STORAGE_CHECKSUM_ERROR", item));
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base_));
    ASSERT_EQ(OB_SUCCESS, writer.write("123456", 6));
    ASSERT_EQ(OB_SUCCESS, writer.close());
  }
}

int check_entry_name(opendal_entry *entry, const char *name) {
  int ret = OB_SUCCESS;
  char *entry_name = nullptr;
  if (OB_ISNULL(entry) || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(entry), KP(name));
  } else if (OB_FAIL(ObDalAccessor::obdal_entry_name(entry, entry_name))) {
    OB_LOG(WARN, "fail to get entry name", K(ret));
  } else if (OB_UNLIKELY(strcmp(entry_name, name) != 0)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "entry name is not expected", K(ret), K(entry_name), K(name));
  }

  if (OB_NOT_NULL(entry_name)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_c_char_free(entry_name))) {
      OB_LOG(WARN, "fail to free entry name", K(tmp_ret));
    }
    ret = COVER_SUCC(tmp_ret);
  }

  return ret;
}

TEST_P(TestObjectStorage, test_obdal_list_retry)
{
  int ret = OB_SUCCESS;
  if (enable_test_
      && is_use_obdal()
      && false) {
    ObStorageObDalUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    // first write three file
    const char *dir_name = "test_obdal_list_retry";
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld", dir_name, ObTimeUtility::current_time()));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/a", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123456", 6));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/b", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123456", 6));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/c", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, "123456", 6));

    const int64_t disrupt_time_s = 65; // 65s
    const int64_t disrupt_time2_s = 40; // 40s
    const int64_t max_list_num = 2;
    ObStorageObDalBase obdal_base;
    opendal_lister *lister = nullptr;
    ObObjectStorageTenantGuard guard(OB_SERVER_TENANT_ID, 60 * 1000 * 1000);
    ASSERT_EQ(OB_SUCCESS, obdal_base.open(dir_uri_, &info_base_));
    ASSERT_EQ(OB_SUCCESS, ObDalAccessor::obdal_operator_list(obdal_base.op_, 
                                                             obdal_base.object_.ptr(), 
                                                             max_list_num, 
                                                             true/*recursive*/, 
                                                             ""/*next_token*/, 
                                                             lister));
    opendal_entry *entry = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObDalAccessor::obdal_lister_next(lister, entry));
    ASSERT_EQ(OB_SUCCESS, check_entry_name(entry, "a"));
    ASSERT_EQ(OB_SUCCESS, ObDalAccessor::obdal_entry_free(entry));
    std::thread t(ObObjectStorageUnittestUtil::disrupt_network, disrupt_time_s);
    sleep(1);
    // The second list next is expected not to cause IO, so it should succeed
    ASSERT_EQ(OB_SUCCESS, ObDalAccessor::obdal_lister_next(lister, entry));
    ASSERT_EQ(OB_SUCCESS, check_entry_name(entry, "b"));
    ASSERT_EQ(OB_SUCCESS, ObDalAccessor::obdal_entry_free(entry)); 
    // The third list next is expected to fail after several retries
    ASSERT_EQ(OB_OBJECT_STORAGE_IO_ERROR, ObDalAccessor::obdal_lister_next(lister, entry));
    ASSERT_EQ(nullptr, entry);
    t.join();

    std::thread t2(ObObjectStorageUnittestUtil::disrupt_network, disrupt_time2_s);
    sleep(1);
    ASSERT_EQ(OB_SUCCESS, ObDalAccessor::obdal_lister_next(lister, entry));
    ASSERT_EQ(OB_SUCCESS, check_entry_name(entry, "c"));
    ASSERT_EQ(OB_SUCCESS, ObDalAccessor::obdal_entry_free(entry));
    t2.join();
    ASSERT_EQ(OB_SUCCESS, ObDalAccessor::obdal_lister_free(lister));
  }
}

std::vector<Config> all_configs;
INSTANTIATE_TEST_CASE_P(
  ConfigCombinations,
  TestObjectStorage,
  ::testing::ValuesIn(all_configs),
  ObObjectStorageUnittestUtil::custom_test_name
);

} // end namaspace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "S3",                               /*storage_type*/
      oceanbase::unittest::S3_BUCKET,     /*bucket*/
      oceanbase::unittest::S3_ENDPOINT,   /*enpoint*/
      oceanbase::unittest::S3_AK,         /*ak*/
      oceanbase::unittest::S3_SK,         /*sk*/
      oceanbase::unittest::S3_REGION,     /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr, "checksum_type=md5", "checksum_type=crc32"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "S3",                               /*storage_type*/
      oceanbase::unittest::S3_BUCKET,     /*bucket*/
      oceanbase::unittest::S3_ENDPOINT,   /*enpoint*/
      oceanbase::unittest::S3_AK,         /*ak*/
      oceanbase::unittest::S3_SK,         /*sk*/
      oceanbase::unittest::S3_REGION,     /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
      nullptr,                            /*role_arn*/
      nullptr,                            /*external_id*/
      true,                               /*enable_obdal*/
    },
    {nullptr, "checksum_type=md5", "checksum_type=crc32"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "OBS",                              /*storage_type*/
      oceanbase::unittest::OBS_BUCKET,    /*bucket*/
      oceanbase::unittest::OBS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::OBS_AK,        /*ak*/
      oceanbase::unittest::OBS_SK,        /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "OBS",                              /*storage_type*/
      oceanbase::unittest::OBS_BUCKET,    /*bucket*/
      oceanbase::unittest::OBS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::OBS_AK,        /*ak*/
      oceanbase::unittest::OBS_SK,        /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
      nullptr,                            /*role_arn*/
      nullptr,                            /*external_id*/
      true,                               /*enable_obdal*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "GCS",                              /*storage_type*/
      oceanbase::unittest::GCS_BUCKET,    /*bucket*/
      oceanbase::unittest::GCS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::GCS_AK,        /*ak*/
      oceanbase::unittest::GCS_SK,        /*sk*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "GCS",                              /*storage_type*/
      oceanbase::unittest::GCS_BUCKET,    /*bucket*/
      oceanbase::unittest::GCS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::GCS_AK,        /*ak*/
      oceanbase::unittest::GCS_SK,        /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
      nullptr,                            /*role_arn*/
      nullptr,                            /*external_id*/
      true,                               /*enable_obdal*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "AZBLOB",                              /*storage_type*/
      oceanbase::unittest::AZBLOB_BUCKET,    /*bucket*/
      oceanbase::unittest::AZBLOB_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::AZBLOB_AK,        /*ak*/
      oceanbase::unittest::AZBLOB_SK,        /*sk*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "FILE",                             /*storage_type*/
      oceanbase::common::OB_FILE_PREFIX,  /*bucket*/
      nullptr,                            /*enpoint*/
      nullptr,                            /*ak*/
      nullptr,                            /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr}
  );

  system("rm -f test_object_storage.log*");
  OB_LOGGER.set_file_name("test_object_storage.log", true, true);
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
