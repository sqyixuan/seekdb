// /**
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

// #define private public

// using namespace obsys;
// namespace oceanbase
// {
// namespace common
// {
// class RowkeyHashFunc
// {
// public:
//   uint64_t operator()(const ObRowkey &rowkey, const uint64_t hash)
//   {
//     return rowkey.murmurhash(hash);
//   }
// };

// class RowkeyGenerate
// {
// public:
//   RowkeyGenerate();
//   virtual ~RowkeyGenerate() = default;
//   int init(const int64_t rowkey_col_cnt, const int64_t char_cnt);
//   int get_next_rowkey(ObRowkey &rowkey, ObIAllocator &allocator);

//   char *rowkey_obj_buf_;
//   char *rowkey_str_buf_;
//   int64_t rowkey_col_cnt_;
//   int64_t char_cnt_;
//   int64_t seed_;
//   ObArenaAllocator allocator_;
// };

// RowkeyGenerate::RowkeyGenerate()
//   : rowkey_obj_buf_(NULL),
//     rowkey_str_buf_(NULL),
//     rowkey_col_cnt_(0),
//     char_cnt_(0),
//     seed_(0),
//     allocator_()
// {}

// int RowkeyGenerate::init(const int64_t rowkey_col_cnt, const int64_t char_cnt)
// {
//   int ret = OB_SUCCESS;

//   if (char_cnt < 0 || char_cnt >= rowkey_col_cnt) {
//     ret = OB_INVALID_ARGUMENT;
//     COMMON_LOG(WARN, "invalid argument", K(rowkey_col_cnt), K(char_cnt), K(ret));
//   } else if (OB_ISNULL(rowkey_obj_buf_ = reinterpret_cast<char*>(allocator_.alloc(sizeof(ObObj) * rowkey_col_cnt)))) {
//     ret = OB_ALLOCATE_MEMORY_FAILED;
//     COMMON_LOG(WARN, "invalid argument", K(rowkey_col_cnt), K(char_cnt), K(ret));
//   } else if (OB_ISNULL(rowkey_str_buf_ = reinterpret_cast<char*>(allocator_.alloc(256 * (char_cnt + 1))))) {
//     ret = OB_ALLOCATE_MEMORY_FAILED;
//     COMMON_LOG(WARN, "invalid argument", K(rowkey_col_cnt), K(char_cnt), K(ret));
//   } else {
//     rowkey_col_cnt_ = rowkey_col_cnt;
//     char_cnt_ = char_cnt;
//     seed_ = 0;
//   }

//   return ret;
// }

// int RowkeyGenerate::get_next_rowkey(ObRowkey &rowkey, ObIAllocator &allocator)
// {
//   int ret = OB_SUCCESS;
//   int64_t char_idx = rowkey_col_cnt_ - char_cnt_;
//   ObRowkey local_rowkey;
//   ObObj *rowkey_cells = reinterpret_cast<ObObj*>(rowkey_obj_buf_);

//   for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_col_cnt_; i++) {
//     if (i < char_idx) {
//       rowkey_cells[i].set_int(seed_);
//     } else {
//       char *string_buf = rowkey_str_buf_ + (256 * (i - char_idx));
//        snprintf(string_buf, 256, "%064ld", seed_);
//        rowkey_cells[i].set_varchar(string_buf, static_cast<int32_t>(strlen(string_buf)));
//        rowkey_cells[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
//     }
//   }

//   local_rowkey.assign(rowkey_cells, rowkey_col_cnt_);
//   if (OB_FAIL(local_rowkey.deep_copy(rowkey, allocator))) {
//     COMMON_LOG(WARN, "Failed to deep copy rowkey", K_(rowkey_col_cnt), K_(char_cnt), K(ret));
//   }

//   return ret;
// }

// class TestBloomFilter : public ::testing::Test
// {
// public:
//   static const int64_t TEST_ROWKEY_COUNT = 10000;
//   TestBloomFilter() = default;
//   virtual ~TestBloomFilter() = default;
//   virtual void SetUp();
//   virtual void TearDown();
//   ObArenaAllocator allocator_;
//   ObSEArray<ObRowkey, TEST_ROWKEY_COUNT> rowkeys_;
// };

// void TestBloomFilter::SetUp()
// {
//   int ret = OB_SUCCESS;
//   RowkeyGenerate rowkey_gen;
//   ObRowkey rowkey;
//   int64_t rowkey_col_cnt = 3;
//   int64_t char_cnt = 0;

//   ret = rowkey_gen.init(rowkey_col_cnt, char_cnt);
//   ASSERT_EQ(OB_SUCCESS, ret);
//   for (int64_t i = 0; i < TEST_ROWKEY_COUNT; i++) {
//     ret = rowkey_gen.get_next_rowkey(rowkey, allocator_);
//     ASSERT_EQ(OB_SUCCESS, ret);
//     ret = rowkeys_.push_back(rowkey);
//     ASSERT_EQ(OB_SUCCESS, ret);
//   }
// }

// void TestBloomFilter::TearDown()
// {
//   allocator_.reset();
// }

// class TestBFStress: public lib::ThreadPool
// {
// public:
//   TestBFStress();
//   virtual ~TestBFStress() = default;
//   void init(const int64_t thread_count, const ObIArray<ObRowkey> &rowkeys);
//   void test();
//   void test_2();
//   virtual void run1();
//   const ObIArray<ObRowkey> *rowkeys_;
//   int64_t row_count_;
//   volatile int64_t put_count_;
// };

// TestBFStress::TestBFStress()
//   :rowkeys_(NULL),
//   row_count_(0),
//   put_count_(0)
// {}

// void TestBFStress::init(const int64_t thread_count, const ObIArray<ObRowkey> &rowkeys)
// {
//   row_count_ = rowkeys.count() * 10;
//   rowkeys_ = &rowkeys;
//   put_count_ = 0;
//   set_thread_count(thread_count);
// }

// void TestBFStress::test()
// {
//   int ret = OB_SUCCESS;
//   int64_t rowkey_count = rowkeys_->count();
//   common::ObBloomFilter<ObRowkey, RowkeyHashFunc> bloom_filter;
//   ret = bloom_filter.init(row_count_);
//   ASSERT_EQ(OB_SUCCESS, ret);
//   while(!has_set_stop()) {
//     for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_count; i+=10) {
//       ret = bloom_filter.insert(rowkeys_->at(i));
//       ret = bloom_filter.insert(rowkeys_->at(i + 1));
//       ret = bloom_filter.insert(rowkeys_->at(i + 2));
//       ret = bloom_filter.insert(rowkeys_->at(i + 3));
//       ret = bloom_filter.insert(rowkeys_->at(i + 4));
//       ret = bloom_filter.insert(rowkeys_->at(i + 5));
//       ret = bloom_filter.insert(rowkeys_->at(i + 6));
//       ret = bloom_filter.insert(rowkeys_->at(i + 7));
//       ret = bloom_filter.insert(rowkeys_->at(i + 8));
//       ret = bloom_filter.insert(rowkeys_->at(i + 9));
//       ASSERT_EQ(OB_SUCCESS, ret);
//     }
//     put_count_ += rowkey_count;
//   }
// }

// //void TestBFStress::test_2()
// //{
//   //int ret = OB_SUCCESS;
//   //int64_t rowkey_count = rowkeys_->count();
//   //common::ObBloomFilter<ObRowkey, RowkeyHashFunc> bloom_filter;
//   //ret = bloom_filter.init(row_count_);
//   //ASSERT_EQ(OB_SUCCESS, ret);
//   //while(!has_set_stop()) {
//     //for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_count; i+=10) {
//       //ret = bloom_filter.insert2(rowkeys_->at(i));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 1));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 2));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 3));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 4));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 5));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 6));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 7));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 8));
//       //ret = bloom_filter.insert2(rowkeys_->at(i + 9));
//       //ASSERT_EQ(OB_SUCCESS, ret);
//     //}
//     //put_count_ += rowkey_count;
//   //}
// //}

// void TestBFStress::run1()
// {
//   test();
// }

// TEST_F(TestBloomFilter, stress)
// {
//   TestBFStress bf_stress;
//   int64_t put_cnt = 0;
//   int64_t i = 0;

//   bf_stress.init(10, rowkeys_);
//   bf_stress.start();
//   while(i++ < 30) {
//     sleep(1);
//     put_cnt = bf_stress.put_count_;
//     COMMON_LOG(INFO, "bloomfilter succ put cnt: ", K(put_cnt));
//     bf_stress.put_count_ = 0;
//   }
//   bf_stress.stop();
//   bf_stress.wait();
// }

// //TEST_F(TestBloomFilter, stress2)
// //{
//   //TestBFStress bf_stress;
//   //int64_t put_cnt = 0;
//   //int64_t i = 0;

//   //bf_stress.init(10, rowkeys_);
//   //bf_stress.start();
//   //while(i++ < 30) {
//     //sleep(1);
//     //put_cnt = bf_stress.put_count_;
//     //COMMON_LOG(INFO, "bloomfilter succ put cnt: ", K(put_cnt));
//     //bf_stress.put_count_ = 0;
//   //}
//   //bf_stress.stop();
//   //bf_stress.wait();
// //}

// }
// }

// int main(int argc, char **argv)
// {
//   oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }
