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
// #define protected public
// #include "lib/alloc/alloc_func.h"
// #include "lib/lock/ob_thread_cond.h"
// #include "lib/random/ob_random.h"
// #include "observer/ob_signal_handle.h"
// #include "observer/omt/ob_multi_tenant.h"
// #include "observer/omt/ob_tenant_meta.h"
// #include "share/cache/ob_kv_storecache.h"
// #include "share/ob_common_rpc_proxy.h"
// #include "share/ob_simple_mem_limit_getter.h"
// #include "share/ob_srv_rpc_proxy.h"
// #include "share/ob_tenant_mem_limit_getter.h"
// #include "share/ob_tenant_mgr.h"

// namespace oceanbase {
// namespace common {

// static ObSimpleMemLimitGetter getter;

// int64_t microseconds() {
//   struct timeval tv;
//   gettimeofday(&tv, NULL);
//   return tv.tv_sec * 1000000L + tv.tv_usec;
// }

// class Random {
// public:
//   static uint64_t rand() { return seed_ = (seed_ * 214013L + 2531011L) >> 16; }
//   static uint64_t srand(uint64_t seed) {
//     seed_ = seed;
//     return seed_;
//   }

// private:
//   static __thread uint64_t seed_;
// };

// __thread uint64_t Random::seed_;

// template <int64_t SIZE> struct TestKVCacheKey : public ObIKVCacheKey {
//   TestKVCacheKey(void) : v_(0), tenant_id_(0) { memset(buf_, 0, sizeof(buf_)); }
//   virtual bool operator==(const ObIKVCacheKey &other) const;
//   virtual uint64_t get_tenant_id() const { return tenant_id_; }
//   virtual uint64_t hash() const { return murmurhash(&v_, sizeof(v_), 1234); }
//   virtual int64_t size() const { return sizeof(*this); }
//   virtual int deep_copy(char *buf, const int64_t buf_len,
//                         ObIKVCacheKey *&key) const;
//   uint64_t v_;
//   uint64_t tenant_id_;
//   char buf_[SIZE > sizeof(v_) ? SIZE - sizeof(v_) : 0];
// };

// template <int64_t SIZE> struct TestKVCacheValue : public ObIKVCacheValue {
//   TestKVCacheValue(void) : v_(0) { memset(buf_, 0, sizeof(buf_)); }
//   virtual int64_t size() const { return sizeof(*this); }
//   virtual int deep_copy(char *buf, const int64_t buf_len,
//                         ObIKVCacheValue *&value) const;
//   uint64_t v_;
//   char buf_[SIZE > sizeof(v_) ? SIZE - sizeof(v_) : 0];
// };

// class TestDataGenerator {
// public:
//   TestDataGenerator() { srand((uint32_t)microseconds()); }
//   virtual ~TestDataGenerator() {}
//   virtual uint64_t gen_data() = 0;
// };

// class RandomDataGenerator : public TestDataGenerator {
// public:
//   RandomDataGenerator() {}
//   virtual ~RandomDataGenerator() {}
//   virtual uint64_t gen_data() { return rand(); }
// };

// class LRUDataGenerator : public TestDataGenerator {
// public:
//   LRUDataGenerator();
//   virtual ~LRUDataGenerator();
//   void init(uint64_t step);
//   virtual uint64_t gen_data();

// private:
//   uint64_t step_;
// };

// class HotDataGenerator : public TestDataGenerator {
// public:
//   HotDataGenerator();
//   virtual ~HotDataGenerator();
//   void init(uint64_t max_data, double hot_data_percent,
//             double hot_access_percent);
//   virtual uint64_t gen_data();

// private:
//   uint64_t max_data_;
//   double hot_data_percent_;
//   double hot_access_percent_;
// };

// class DeferExitRunnable : public share::ObThreadPool {
// public:
//   DeferExitRunnable();
//   virtual ~DeferExitRunnable();
//   virtual void run2(int64_t thread_id) = 0;
//   virtual void run1() override;
//   void wait_idle(); // wait all thread idle
//   void trigger_exit();

// private:
//   ObThreadCond cond_;
//   int64_t idle_count_;
//   bool exit_triggered_;
// };

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// class TestKVCacheStress : public DeferExitRunnable {
// public:
//   TestKVCacheStress();
//   virtual ~TestKVCacheStress();
//   int init(
//       ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache,
//       const uint64_t tenant_id, TestDataGenerator *data_generator);
//   virtual void run2(int64_t thread_id) override;

// private:
//   void do_monitor();
//   void do_work();
//   uint64_t tenant_id_;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache_;
//   TestDataGenerator *data_generator_;
//   int64_t hit_cnt_;
//   int64_t miss_cnt_;
// };

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// class TestKVCacheThroughput : public DeferExitRunnable {
// public:
//   TestKVCacheThroughput();
//   virtual ~TestKVCacheThroughput();
//   int init(
//       ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache,
//       const uint64_t tenant_id, double put_percent);
//   virtual void run2(int64_t thread_id) override;

// private:
//   static const int64_t MAX_DATA = 10000000L;
//   uint64_t tenant_id_;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache_;
//   double put_percent_;
// };

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// class TestKVCacheThroughput2 : public DeferExitRunnable {
// public:
//   TestKVCacheThroughput2();
//   virtual ~TestKVCacheThroughput2();
//   int init(
//       ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache,
//       const uint64_t tenant_id, double put_percent);
//   virtual void run2(int64_t thread_id) override;
//   void print_stat() {
//     COMMON_LOG(INFO, "test statistics", "total_put_cnt", put_cnt_.value(),
//                "total_get_hit_cnt", get_hit_cnt_.value(), "total_get_miss_cnt",
//                get_miss_cnt_.value());
//   }

// private:
//   static const int64_t MAX_DATA = 10000000L;
//   uint64_t tenant_id_;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache_;
//   double put_percent_;
//   ObTCCounter get_hit_cnt_;
//   ObTCCounter get_miss_cnt_;
//   ObTCCounter put_cnt_;
// };

// class MbListScanner : public DeferExitRunnable {
// public:
//   MbListScanner(const uint64_t tenant_id);
//   virtual ~MbListScanner();
//   int init(const uint64_t tenant_id);
//   virtual void run2(int64_t thread_id) override;

// private:
//   uint64_t tenant_id_;
// };

// template <int64_t SIZE>
// bool TestKVCacheKey<SIZE>::operator==(const ObIKVCacheKey &other) const {
//   const TestKVCacheKey &other_key =
//       reinterpret_cast<const TestKVCacheKey &>(other);
//   return v_ == other_key.v_ && tenant_id_ == other_key.tenant_id_;
// }

// template <int64_t SIZE>
// int TestKVCacheKey<SIZE>::deep_copy(char *buf, const int64_t buf_len,
//                                     ObIKVCacheKey *&key) const {
//   int ret = OB_SUCCESS;
//   TestKVCacheKey<SIZE> *pkey = NULL;
//   if (NULL == buf || buf_len < size()) {
//     ret = OB_INVALID_ARGUMENT;
//   } else {
//     pkey = new (buf) TestKVCacheKey<SIZE>();
//     pkey->v_ = v_;
//     pkey->tenant_id_ = tenant_id_;
//     if (SIZE > sizeof(v_)) {
//       memcpy(pkey->buf_, buf_, SIZE - sizeof(v_));
//     }
//     key = pkey;
//   }
//   return ret;
// }

// template <int64_t SIZE>
// int TestKVCacheValue<SIZE>::deep_copy(char *buf, const int64_t buf_len,
//                                       ObIKVCacheValue *&value) const {
//   int ret = OB_SUCCESS;
//   TestKVCacheValue<SIZE> *pvalue = NULL;
//   if (NULL == buf || buf_len < size()) {
//     ret = OB_INVALID_ARGUMENT;
//   } else {
//     pvalue = new (buf) TestKVCacheValue<SIZE>();
//     pvalue->v_ = v_;
//     if (SIZE > sizeof(v_)) {
//       memcpy(pvalue->buf_, buf_, SIZE - sizeof(v_));
//     }
//     value = pvalue;
//   }
//   return ret;
// }

// DeferExitRunnable::DeferExitRunnable()
//     : cond_(), idle_count_(0), exit_triggered_(false) {}

// DeferExitRunnable::~DeferExitRunnable() {}

// void DeferExitRunnable::run1() {

//   int64_t thread_id = get_thread_idx();
//   run2(thread_id);

//   {
//     ObThreadCondGuard guard(cond_);
//     ++idle_count_;
//     cond_.broadcast();
//   }

//   while (!exit_triggered_) {
//     usleep(1000); // 1ms
//   }
// }

// void DeferExitRunnable::wait_idle() {
//   ObThreadCondGuard guard(cond_);
//   while (get_thread_count() != idle_count_) {
//     cond_.wait();
//   }
// }

// void DeferExitRunnable::trigger_exit() { exit_triggered_ = true; }

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// TestKVCacheStress<KEY_SIZE, VALUE_SIZE>::TestKVCacheStress()
//     : tenant_id_(0), cache_(NULL), data_generator_(NULL), hit_cnt_(0),
//       miss_cnt_(0) {}

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// TestKVCacheStress<KEY_SIZE, VALUE_SIZE>::~TestKVCacheStress() {}

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// int TestKVCacheStress<KEY_SIZE, VALUE_SIZE>::init(
//     ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache,
//     const uint64_t tenant_id, TestDataGenerator *data_generator) {
//   int ret = OB_SUCCESS;
//   if (NULL == cache || NULL == data_generator) {
//     ret = OB_INVALID_ARGUMENT;
//   } else {
//     cache_ = cache;
//     tenant_id_ = tenant_id;
//     data_generator_ = data_generator;
//   }
//   return ret;
// }

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// void TestKVCacheStress<KEY_SIZE, VALUE_SIZE>::run2(int64_t thread_id) {
//   if (0 == thread_id) {
//     do_monitor();
//   } else {
//     do_work();
//   }
// }

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// void TestKVCacheStress<KEY_SIZE, VALUE_SIZE>::do_monitor() {
//   while (!has_set_stop()) {
//     int64_t hit_cnt = ATOMIC_LOAD(&hit_cnt_);
//     int64_t miss_cnt = ATOMIC_LOAD(&miss_cnt_);
//     ATOMIC_STORE(&hit_cnt_, 0);
//     ATOMIC_STORE(&miss_cnt_, 0);
//     int64_t get_cnt = hit_cnt + miss_cnt;
//     double hit_ratio = 0;

//     if (hit_cnt > 0) {
//       hit_ratio = double(hit_cnt) / double(get_cnt);
//     }
//     printf("hit_ratio:%lf\n", hit_ratio);
//     sleep(1);
//   }
// }

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// void TestKVCacheStress<KEY_SIZE, VALUE_SIZE>::do_work() {
//   int ret = OB_SUCCESS;
//   ObKVCacheHandle handle;
//   TestKVCacheKey<KEY_SIZE> key;
//   TestKVCacheValue<VALUE_SIZE> value;
//   const TestKVCacheValue<VALUE_SIZE> *pvalue = NULL;
//   while (!has_set_stop()) {
//     uint64_t data = data_generator_->gen_data();
//     key.v_ = data;
//     key.tenant_id_ = tenant_id_;
//     value.v_ = data;

//     ret = cache_->get(key, pvalue, handle);

//     if (OB_SUCC(ret)) {
//       ATOMIC_INC(&hit_cnt_);
//       if (pvalue->v_ != key.v_) {
//         COMMON_LOG(ERROR, "CRITICAL ERROR: value mismatch, ", "key:", key.v_,
//                    "value", pvalue->v_);
//         abort();
//       }
//     } else if (OB_ENTRY_NOT_EXIST == ret) {
//       ATOMIC_INC(&miss_cnt_);
//       if (OB_SUCCESS != (ret = cache_->put(key, value))) {
//         COMMON_LOG(WARN, "Fail to put data to cache, ", K(ret));
//       }
//     } else {
//       COMMON_LOG(WARN, "get error, ", K(ret));
//       break;
//     }
//   }
// }

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// TestKVCacheThroughput<KEY_SIZE, VALUE_SIZE>::TestKVCacheThroughput()
//     : tenant_id_(0), cache_(NULL), put_percent_(0) {}

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// TestKVCacheThroughput<KEY_SIZE, VALUE_SIZE>::~TestKVCacheThroughput() {}

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// int TestKVCacheThroughput<KEY_SIZE, VALUE_SIZE>::init(
//     ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache,
//     const uint64_t tenant_id, double put_percent) {
//   int ret = OB_SUCCESS;

//   if (NULL == cache) {
//     ret = OB_INVALID_ARGUMENT;
//   } else {
//     TestKVCacheKey<KEY_SIZE> key;
//     TestKVCacheValue<VALUE_SIZE> value;
//     for (int64_t i = 0; OB_SUCC(ret) && i < MAX_DATA; ++i) {
//       key.v_ = i;
//       key.tenant_id_ = tenant_id;
//       value.v_ = i;
//       if (OB_SUCCESS != (ret = cache->put(key, value))) {
//         COMMON_LOG(ERROR, "Fail to put data to cache,", K(ret));
//       }
//     }

//     if (OB_SUCC(ret)) {
//       tenant_id_ = tenant_id;
//       cache_ = cache;
//       put_percent_ = put_percent;
//       COMMON_LOG(INFO, "TestKVCacheThroughput init success", K(tenant_id),
//                  K(put_percent));
//     }
//   }
//   return ret;
// }

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// void TestKVCacheThroughput<KEY_SIZE, VALUE_SIZE>::run2(int64_t thread_id) {
//   int ret = OB_SUCCESS;
//   const TestKVCacheValue<VALUE_SIZE> *pvalue = NULL;

//   ObKVCacheHandle handle;

//   bool is_put = thread_id < get_thread_count() * put_percent_;
//   COMMON_LOG(INFO, "normal put", K(is_put));

//   while (!has_set_stop()) {
//     int64_t data = ObRandom::rand(0, MAX_DATA - 1);

//     TestKVCacheKey<KEY_SIZE> key;
//     TestKVCacheValue<VALUE_SIZE> value;
//     key.v_ = data;
//     key.tenant_id_ = tenant_id_;
//     value.v_ = data;
//     if (is_put) {
//       if (OB_FAIL(cache_->put(key, value))) {
//         COMMON_LOG(ERROR, "failed to put cache", K(ret));
//       }
//     } else {
//       if (OB_SUCCESS != (ret = cache_->get(key, pvalue, handle))) {
//         if (OB_ENTRY_NOT_EXIST != ret) {
//           COMMON_LOG(ERROR, "Fail to get data from cache,", K(ret));
//         }
//       }
//     }
//   }
// }

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// TestKVCacheThroughput2<KEY_SIZE, VALUE_SIZE>::TestKVCacheThroughput2()
//     : tenant_id_(0), cache_(NULL), put_percent_(0) {}

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// TestKVCacheThroughput2<KEY_SIZE, VALUE_SIZE>::~TestKVCacheThroughput2() {}

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// int TestKVCacheThroughput2<KEY_SIZE, VALUE_SIZE>::init(
//     ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> *cache,
//     const uint64_t tenant_id, double put_percent) {
//   int ret = OB_SUCCESS;

//   if (NULL == cache) {
//     ret = OB_INVALID_ARGUMENT;
//   } else {
//     TestKVCacheKey<KEY_SIZE> key;
//     TestKVCacheValue<VALUE_SIZE> value;
//     for (int64_t i = 0; OB_SUCC(ret) && i < MAX_DATA; ++i) {
//       key.v_ = i;
//       key.tenant_id_ = tenant_id;
//       value.v_ = i;
//       if (OB_SUCCESS != (ret = cache->put(key, value))) {
//         COMMON_LOG(ERROR, "Fail to put data to cache,", K(ret));
//       }
//     }

//     if (OB_SUCC(ret)) {
//       tenant_id_ = tenant_id;
//       cache_ = cache;
//       put_percent_ = put_percent;
//       COMMON_LOG(INFO, "TestKVCacheThroughput2 init success", K(tenant_id),
//                  K(put_percent));
//     }
//   }
//   return ret;
// }

// template <int64_t KEY_SIZE, int64_t VALUE_SIZE>
// void TestKVCacheThroughput2<KEY_SIZE, VALUE_SIZE>::run2(int64_t thread_id) {
//   int ret = OB_SUCCESS;
//   const TestKVCacheValue<VALUE_SIZE> *pvalue = NULL;

//   TestKVCacheKey<KEY_SIZE> key;
//   TestKVCacheValue<VALUE_SIZE> value;
//   ObKVCacheHandle handle;
//   ObKVCacheInstHandle inst_handle;

//   bool is_put = thread_id < get_thread_count() * put_percent_;

//   COMMON_LOG(INFO, "alloc and put", K(is_put));

//   while (!has_set_stop()) {
//     int64_t data = ObRandom::rand(0, MAX_DATA - 1);

//     key.v_ = data;
//     key.tenant_id_ = tenant_id_;
//     value.v_ = data;
//     if (is_put) {
//       ObKVCachePair *pair = nullptr;
//       if (OB_FAIL(cache_->alloc(tenant_id_, sizeof(TestKVCacheKey<KEY_SIZE>),
//                                 sizeof(TestKVCacheValue<VALUE_SIZE>), pair,
//                                 handle, inst_handle))) {
//         COMMON_LOG(ERROR, "failed to alloc cache", K(ret));
//       } else {
//         TestKVCacheKey<KEY_SIZE> *tkey =
//             new (pair->key_) TestKVCacheKey<KEY_SIZE>();
//         TestKVCacheValue<VALUE_SIZE> *tvalue =
//             new (pair->value_) TestKVCacheValue<VALUE_SIZE>();
//         tkey->v_ = data;
//         tkey->tenant_id_ = tenant_id_;
//         tvalue->v_ = data;
//         if (OB_FAIL(cache_->put_kvpair(inst_handle, pair, handle))) {
//           COMMON_LOG(ERROR, "failed to put cache", K(ret));
//         } else {
//           put_cnt_.inc();
//         }
//         handle.reset();
//       }
//     } else {
//       if (OB_SUCCESS != (ret = cache_->get(key, pvalue, handle))) {
//         if (OB_ENTRY_NOT_EXIST != ret) {
//           COMMON_LOG(ERROR, "Fail to get data from cache,", K(ret));
//         }
//         get_miss_cnt_.inc();
//       } else {
//         get_hit_cnt_.inc();
//       }
//     }
//   }
// }

// MbListScanner::MbListScanner(const uint64_t tenant_id)
//     : tenant_id_(tenant_id) {}

// MbListScanner::~MbListScanner() {}

// void MbListScanner::run2(int64_t thread_id) {
//   UNUSED(thread_id);
//   while (!has_set_stop()) {
//     int ret = OB_SUCCESS;
//     ObTenantMBListHandle list_handle;
//     ObDLink *head = NULL;
//     if (OB_FAIL(ObKVGlobalCache::get_instance().insts_.get_mb_list(
//             tenant_id_, list_handle))) {
//       COMMON_LOG(WARN, "get_mb_list failed", K(ret), K_(tenant_id));
//     } else if (NULL == (head = list_handle.get_head())) {
//       ret = OB_ERR_UNEXPECTED;
//       COMMON_LOG(WARN, "head in list_handle is null", K(ret));
//     } else {
//       QClockGuard guard(ObKVCacheStore::get_qclock());
//       ObKVMemBlockHandle *handle =
//           static_cast<ObKVMemBlockHandle *>(link_next(head));
//       for (int64_t i = 0; i < 10 && NULL != handle; ++i) {
//         handle = static_cast<ObKVMemBlockHandle *>(link_next(handle));
//       }
//     }
//   }
// }

// LRUDataGenerator::LRUDataGenerator() : step_(0) {}

// LRUDataGenerator::~LRUDataGenerator() {}

// void LRUDataGenerator::init(uint64_t step) { step_ = step; }

// uint64_t LRUDataGenerator::gen_data() {
//   static __thread uint64_t curr = 0;
//   static __thread uint64_t r = 1;
//   uint64_t data = ++curr;
//   if (data % step_ == 0 && curr / step_ == r) {
//     curr = data - step_;
//     ++r;
//   }
//   return data;
// }

// HotDataGenerator::HotDataGenerator()
//     : max_data_(0), hot_data_percent_(0), hot_access_percent_(0) {}

// HotDataGenerator::~HotDataGenerator() {}

// void HotDataGenerator::init(uint64_t max_data, double hot_data_percent,
//                             double hot_access_percent) {
//   max_data_ = max_data;
//   hot_data_percent_ = hot_data_percent;
//   hot_access_percent_ = hot_access_percent;
// }

// uint64_t HotDataGenerator::gen_data() {
//   uint64_t data = 0;
//   bool is_hot = rand() % 100 < hot_access_percent_ * 100;
//   if (is_hot) {
//     data = rand() % (int64_t)(double(max_data_) * hot_data_percent_);
//   } else {
//     data = (int64_t)(double(max_data_) * hot_data_percent_) +
//            rand() % (int64_t)(double(max_data_) * (1 - hot_data_percent_));
//   }
//   return data;
// }

// class TestKVCache : public ::testing::Test {
// public:
//   TestKVCache();
//   virtual ~TestKVCache();
//   virtual void SetUp();
//   virtual void TearDown();

// private:
//   // disallow copy
//   DISALLOW_COPY_AND_ASSIGN(TestKVCache);

// protected:
//   // function members
// protected:
//   // data members
// };

// TestKVCache::TestKVCache() {}

// TestKVCache::~TestKVCache() {}

// void TestKVCache::SetUp() {
//   ObClockGenerator::init();
//   ObKVGlobalCache::get_instance().init(&getter);

//   CHUNK_MGR.set_limit(5L * 1024L * 1024L * 1024L);
// }

// void TestKVCache::TearDown() {
//   // ObKVGlobalCache::get_instance().destroy();
// }

// TEST_F(TestKVCache, test_memory_isolation) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 1024;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheStress<KEY_SIZE, VALUE_SIZE> stress1;
//   TestKVCacheStress<KEY_SIZE, VALUE_SIZE> stress2;
//   RandomDataGenerator data_generator;

//   ret = cache.init("test", 1);
//   EXPECT_EQ(OB_SUCCESS, ret);
//   ret = stress1.init(&cache, 900, &data_generator);
//   EXPECT_EQ(OB_SUCCESS, ret);
//   ret = stress2.init(&cache, 901, &data_generator);
//   EXPECT_EQ(OB_SUCCESS, ret);
//   uint64_t tenant1 = 900;
//   uint64_t tenant2 = 901;
//   uint64_t tenant1_upper_limit = 1024L * 1024L * 1024L * 3;
//   uint64_t tenant1_lower_limit = 1024L * 1024L * 1024L * 2;
//   uint64_t tenant2_upper_limit = 1024L * 1024L * 1024L * 4;
//   uint64_t tenant2_lower_limit = 1024L * 1024L * 1024L * 3;
//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;

//   getter.reset();
//   ret = getter.add_tenant(tenant1, tenant1_lower_limit, tenant1_upper_limit);
//   ret = getter.add_tenant(tenant2, tenant2_lower_limit, tenant2_upper_limit);

//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(8L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     stress1.set_thread_count(5);
//     stress2.set_thread_count(5);

//     stress1.start();
//     stress2.start();

//     ObArray<ObKVCacheInstHandle> inst_handles;
//     int64_t time = 0;
//     while (time++ < 150) {
//       sleep(1);
//       inst_handles.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//       printf("%lu %lu %lu %lu\n", tenant1_upper_limit / (1024L * 1024L * 1024L),
//              tenant1_lower_limit / (1024L * 1024L * 1024L),
//              tenant2_upper_limit / (1024L * 1024L * 1024L),
//              tenant2_lower_limit / (1024L * 1024L * 1024L));
//       if (75 == time) {
//         tenant1_upper_limit = 1024L * 1024L * 1024L * 4;
//         tenant1_lower_limit = 1024L * 1024L * 1024L * 3;
//         tenant2_upper_limit = 1024L * 1024L * 1024L * 3;
//         tenant2_lower_limit = 1024L * 1024L * 1024L * 2;
//         getter.reset();
//         getter.add_tenant(tenant1, tenant1_lower_limit, tenant1_upper_limit);
//         getter.add_tenant(tenant2, tenant2_lower_limit, tenant2_upper_limit);
//       }
//     }

//     stress1.stop();
//     stress2.stop();
//     stress1.wait_idle();
//     stress2.wait_idle();

//     ObKVGlobalCache::get_instance().destroy();

//     stress1.trigger_exit();
//     stress2.trigger_exit();
//     stress1.wait();
//     stress2.wait();
//   }
// }

// TEST_F(TestKVCache, test_hit_ratio) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 1024 * 1;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheStress<KEY_SIZE, VALUE_SIZE> stress;
//   TestKVCacheStress<KEY_SIZE, VALUE_SIZE> stress2;
//   HotDataGenerator hot_generator;
//   uint64_t tenant_id = 900;
//   int64_t tenant_lower_limit = 1024L * 1024L * 1024L * 2;
//   int64_t tenant_upper_limit = 1024L * 1024L * 1024L * 3;
//   uint64_t max_data =
//       (uint64_t)((double)tenant_upper_limit / (double)VALUE_SIZE / 0.4);

//   RandomDataGenerator rand_generator;

//   hot_generator.init(max_data, 0.2, 0.8);

//   ret = cache.init("test_hit", 1);
//   EXPECT_EQ(OB_SUCCESS, ret);
//   ret = stress.init(&cache, tenant_id, &hot_generator);
//   EXPECT_EQ(OB_SUCCESS, ret);
//   ret = stress2.init(&cache, tenant_id, &rand_generator);

//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;
//   // getter.init(self, rpc_proxy, common_rpc,
//   // rs_mgr, &req_transport,
//   //                      &ObServerConfig::get_instance());
//   getter.reset();
//   getter.add_tenant(tenant_id, tenant_lower_limit, tenant_upper_limit);
//   getter.add_tenant(tenant_id, tenant_lower_limit, tenant_upper_limit);

//   ObArray<ObKVCacheInstHandle> inst_handles_;
//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(4L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     stress.set_thread_count(5);
//     stress2.set_thread_count(2);
//     stress.start();

//     int64_t time = 0;
//     while (time++ < 150) {
//       sleep(1);
//       inst_handles_.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//       if (75 == time) {
//         stress2.start();
//       }
//     }

//     stress.stop();
//     stress2.stop();
//     stress.wait_idle();
//     stress2.wait_idle();

//     ObKVGlobalCache::get_instance().destroy();

//     stress.trigger_exit();
//     stress2.trigger_exit();
//     stress.wait();
//     stress2.wait();
//   }
// }

// TEST_F(TestKVCache, test_lru) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 1024 * 1;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheStress<KEY_SIZE, VALUE_SIZE> stress;
//   uint64_t tenant_id = 900;
//   int64_t tenant_lower_limit = 1024L * 1024L * 1024L * 2;
//   int64_t tenant_upper_limit = 1024L * 1024L * 1024L * 3;

//   LRUDataGenerator lru_generator;
//   lru_generator.init(100000);

//   ret = cache.init("test_hit", 1);
//   EXPECT_EQ(OB_SUCCESS, ret);
//   ret = stress.init(&cache, tenant_id, &lru_generator);
//   EXPECT_EQ(OB_SUCCESS, ret);

//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;

//   getter.reset();
//   getter.add_tenant(tenant_id, tenant_lower_limit, tenant_upper_limit);

//   ObArray<ObKVCacheInstHandle> inst_handles;
//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(4L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     stress.set_thread_count(5);
//     stress.start();

//     int64_t time = 0;
//     while (time++ < 150) {
//       sleep(1);
//       inst_handles.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//     }
//     stress.stop();
//     stress.wait_idle();
//     ObKVGlobalCache::get_instance().destroy();
//     stress.trigger_exit();
//     stress.wait();
//   }
// }

// TEST_F(TestKVCache, test_priority) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 1024 * 1;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache2;
//   TestKVCacheStress<KEY_SIZE, VALUE_SIZE> stress;
//   TestKVCacheStress<KEY_SIZE, VALUE_SIZE> stress2;
//   HotDataGenerator hot_generator;
//   uint64_t tenant_id = 900;
//   int64_t tenant_lower_limit = 1024L * 1024L * 1024L * 2;
//   int64_t tenant_upper_limit = 1024L * 1024L * 1024L * 3;
//   uint64_t max_data =
//       (uint64_t)((double)tenant_upper_limit / (double)VALUE_SIZE / 0.4);

//   hot_generator.init(max_data, 0.2, 0.8);

//   ret = cache.init("test_priority", 1);
//   ASSERT_EQ(OB_SUCCESS, ret);
//   ret = cache2.init("test_priority2", 10);
//   ASSERT_EQ(OB_SUCCESS, ret);
//   ret = stress.init(&cache, tenant_id, &hot_generator);
//   ASSERT_EQ(OB_SUCCESS, ret);
//   ret = stress2.init(&cache2, tenant_id, &hot_generator);

//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;
//   getter.reset();
//   getter.add_tenant(tenant_id, tenant_lower_limit, tenant_upper_limit);

//   ObArray<ObKVCacheInstHandle> inst_handles;
//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(4L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     stress.set_thread_count(5);
//     stress2.set_thread_count(2);
//     stress.start();
//     stress2.start();

//     int64_t time = 0;
//     while (time++ < 150) {
//       sleep(1);
//       inst_handles.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//       if (75 == time) {
//         ret = cache.set_priority(10);
//         ASSERT_EQ(OB_SUCCESS, ret);
//         ret = cache2.set_priority(1);
//         ASSERT_EQ(OB_SUCCESS, ret);
//       }
//     }

//     stress.stop();
//     stress2.stop();
//     stress.wait_idle();
//     stress2.wait_idle();

//     ObKVGlobalCache::get_instance().destroy();

//     stress.trigger_exit();
//     stress2.trigger_exit();
//     stress.wait();
//     stress2.wait();
//   }
// }

// TEST_F(TestKVCache, test_memory_fragment) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 64;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheThroughput<KEY_SIZE, VALUE_SIZE> throughput;

//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;
//   getter.reset();
//   getter.add_tenant(900, 1024L * 1024L * 1024L * 3, 1024L * 1024L * 1024L * 4);

//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(6L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     ret = cache.init("test", 1);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     ret = throughput.init(&cache, 900, 0.5);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     throughput.set_thread_count(16);
//     throughput.start();

//     ObArray<ObKVCacheInstHandle> inst_handles;
//     int64_t time = 0;
//     while (time++ < 150) {
//       sleep(1);
//       inst_handles.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//       if (75 == time) {
//         throughput.stop();
//         throughput.wait_idle();
//         getter.reset();
//         getter.add_tenant(900, 1024 * 1024 * 2, 1024 * 1024 * 2);
//       }
//     }

//     ObKVGlobalCache::get_instance().destroy();
//     throughput.trigger_exit();
//     throughput.wait();
//   }
// }

// TEST_F(TestKVCache, test_read_throughput) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 64;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheThroughput2<KEY_SIZE, VALUE_SIZE> throughput;

//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;
//   getter.reset();
//   getter.add_tenant(900, 1024L * 1024L * 1024L * 6, 1024L * 1024L * 1024L * 7);
//   TestKVCacheKey<KEY_SIZE> key;
//   key.v_ = 2;
//   COMMON_LOG(INFO, "key hash = ", K(key.hash()));

//   ObArray<ObKVCacheInstHandle> inst_handles;
//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(8L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     ret = cache.init("test", 1);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     ret = throughput.init(&cache, 900, 0);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     throughput.set_thread_count(16);
//     throughput.start();

//     int64_t time = 0;
//     while (time++ < 150) {
//       sleep(1);
//       inst_handles.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//     }
//     throughput.stop();
//     throughput.wait_idle();

//     ObKVGlobalCache::get_instance().destroy();
//     throughput.trigger_exit();
//     throughput.wait();
//     throughput.print_stat();
//   }
// }

// TEST_F(TestKVCache, test_rw_throughput) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 1024;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheThroughput<KEY_SIZE, VALUE_SIZE> throughput;
//   MbListScanner list_scanner(900);
//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;
//   getter.reset();
//   getter.add_tenant(900, 1024L * 1024L * 1024L * 4, 1024L * 1024L * 1024L * 5);

//   ObArray<ObKVCacheInstHandle> inst_handles;
//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(6L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     ret = cache.init("test", 1);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     ret = throughput.init(&cache, 900, 1);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     throughput.set_thread_count(16);
//     throughput.start();
//     list_scanner.start();

//     int64_t time = 0;
//     while (time++ < 150) {
//       sleep(1);
//       inst_handles.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//     }
//     throughput.stop();
//     throughput.wait_idle();
//     list_scanner.stop();
//     list_scanner.wait_idle();

//     ObKVGlobalCache::get_instance().destroy();
//     throughput.trigger_exit();
//     throughput.wait();
//     list_scanner.trigger_exit();
//     list_scanner.wait();
//   }
// }

// TEST_F(TestKVCache, test_rw_throughput2) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 1024;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheThroughput2<KEY_SIZE, VALUE_SIZE> throughput;
//   MbListScanner list_scanner(900);
//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;
//   getter.reset();
//   getter.add_tenant(900, 1024L * 1024L * 1024L * 4, 1024L * 1024L * 1024L * 5);

//   ObArray<ObKVCacheInstHandle> inst_handles;
//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(6L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     ret = cache.init("test", 1);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     ret = throughput.init(&cache, 900, 1);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     throughput.set_thread_count(16);
//     throughput.start();
//     list_scanner.start();

//     int64_t time = 0;
//     while (time++ < 150) {
//       sleep(1);
//       inst_handles.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//     }
//     throughput.stop();
//     throughput.wait_idle();
//     list_scanner.stop();
//     list_scanner.wait_idle();

//     ObKVGlobalCache::get_instance().destroy();
//     throughput.trigger_exit();
//     throughput.wait();
//     throughput.print_stat();
//     list_scanner.trigger_exit();
//     list_scanner.wait();
//   }
// }

// TEST_F(TestKVCache, tenant_clean) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 64;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheThroughput<KEY_SIZE, VALUE_SIZE> throughput;

//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;
//   getter.reset();

//   ObArray<ObKVCacheInstHandle> inst_handles;
//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(6L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     ret = cache.init("test", 1);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     ret = throughput.init(&cache, 1234, 0.5);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     throughput.set_thread_count(16);
//     throughput.start();

//     int64_t time = 0;
//     while (time++ < 100) {
//       sleep(1);
//       if (5 == time) {
//         throughput.stop();
//         throughput.wait_idle();
//       }
//       inst_handles.reuse();
//       ObKVGlobalCache::get_instance().print_all_cache_info();
//     }

//     ObKVGlobalCache::get_instance().destroy();
//     throughput.trigger_exit();
//     throughput.wait();
//   }
// }

// TEST_F(TestKVCache, basic) {
//   int ret = OB_SUCCESS;
//   static const int64_t KEY_SIZE = 16;
//   static const int64_t VALUE_SIZE = 64;
//   ObKVCache<TestKVCacheKey<KEY_SIZE>, TestKVCacheValue<VALUE_SIZE>> cache;
//   TestKVCacheThroughput<KEY_SIZE, VALUE_SIZE> throughput;

//   ObAddr self;
//   self.set_ip_addr("127.0.0.1", 8086);
//   rpc::frame::ObReqTransport req_transport(NULL, NULL);
//   obrpc::ObSrvRpcProxy rpc_proxy;
//   obrpc::ObCommonRpcProxy common_rpc;
//   share::ObRsMgr rs_mgr;
//   getter.reset();

//   if (OB_SUCC(ret)) {
//     lib::set_memory_limit(6L * 1024L * 1024L * 1024L);
//     lib::ob_set_reserved_memory(512L * 1024L * 1024L);

//     ret = cache.init("test", 1);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     TestKVCacheKey<KEY_SIZE> key;
//     key.v_ = 1;
//     key.tenant_id_ = 1234;
//     TestKVCacheValue<VALUE_SIZE> value;
//     value.v_ = 1;
//     ret = cache.put(key, value);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     const TestKVCacheValue<VALUE_SIZE> *pvalue;
//     ObKVCacheHandle handle;
//     ret = cache.get(key, pvalue, handle);
//     EXPECT_EQ(OB_SUCCESS, ret);
//     EXPECT_EQ(1, pvalue->v_);
//     COMMON_LOG(INFO, "node size", K(sizeof(ObKVCacheMap::Node)));
//   }
// }

// } // namespace common
// } // namespace oceanbase

// int main(int argc, char **argv) {
//   oceanbase::observer::ObSignalHandle signal_handle;
//   oceanbase::observer::ObSignalHandle::change_signal_mask();
//   signal_handle.start();

//   oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }

