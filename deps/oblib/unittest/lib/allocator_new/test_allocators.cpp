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
#include "test1/allocator_tester.h"
#include "test2/t-test1.h"
#include "test2/t-test2.h"
#include "lib/alloc/malloc_hook.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/rc/context.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
class TestVoid
{
public:
  TestVoid() : attr_()
  {}
  void init(int64_t tenant_id)
  {
    attr_.tenant_id_ = tenant_id;
    attr_.label_ = name();
    data_ = std::malloc(128);
  }
  int64_t id() { return attr_.tenant_id_; }
  static constexpr const char* name() { return "void"; }
  void *alloc(size_t size) {return data_; }
  void free(void* ptr) {};
private:
  ObMemAttr attr_;
  void *data_;
};
class TestMallocHook
{
public:
  TestMallocHook() {};
  void init(int64_t tenant_id)
  {
     attr_.tenant_id_ = tenant_id;
     attr_.label_ = name();
  }
  int64_t id() { return attr_.tenant_id_; }
  static constexpr const char* name() { return "malloc_hook"; }
  int get_index()
  {
    static int g_index =0;
    static thread_local int index = ATOMIC_FAA(&g_index, 1);
    return index;
  }
  void *alloc(size_t size)
  {
    return std::malloc(size);
  }
  void free(void* ptr)
  {
    std::free(ptr);
  }
private:
  ObMemAttr attr_;

};

class TestMalloc
{
public:
  TestMalloc() : attr_()
  {}
  void init(int64_t tenant_id)
  {
    attr_.tenant_id_ = tenant_id;
    attr_.label_ = name();
  }
  int64_t id() { return attr_.tenant_id_; }
  static constexpr const char* name() { return "ob_malloc"; }
  void *alloc(size_t size) { return ob_malloc(size, attr_); }
  void free(void* ptr) { ob_free(ptr); }
private:
  ObMemAttr attr_;
};

class TestVSliceAlloc
{
public:
  TestVSliceAlloc() : attr_(), vslice_()
  {}
  void init(int64_t tenant_id, int64_t blk_size = OB_MALLOC_NORMAL_BLOCK_SIZE, int parallel = 1)
  {
    attr_.tenant_id_ = tenant_id;
    attr_.label_ = name();
    vslice_.init(blk_size, default_blk_alloc, attr_);
    vslice_.set_nway(parallel);
  }
  int64_t id() { return attr_.tenant_id_; }
  static constexpr const char* name() { return "vslice_alloc"; }
  void *alloc(size_t size) { return vslice_.alloc(size); }
  void free(void* ptr) { vslice_.free(ptr); }
private:
  ObMemAttr attr_;
  ObVSliceAlloc vslice_;
};

class TestFIFOAllocator
{
public:

  TestFIFOAllocator() : fifo_allocator_()
  {}
  void init(int64_t tenant_id, int64_t blk_size = OB_MALLOC_NORMAL_BLOCK_SIZE)
  {
    attr_.tenant_id_ = tenant_id;
    attr_.label_ = name();
    fifo_allocator_.init(ObMallocAllocator::get_instance(), blk_size, attr_);
  }
  int64_t id() { return attr_.tenant_id_; }
  static constexpr const char* name() { return "fifo_allocator"; }
  void *alloc(size_t size) { return fifo_allocator_.alloc(size); }
  void free(void* ptr) { fifo_allocator_.free(ptr); }
private:
  ObMemAttr attr_;
  ObFIFOAllocator fifo_allocator_;
};

class TestMemoryContext
{
public:
  TestMemoryContext() : mem_context_(), attr_()
  {}
  ~TestMemoryContext()
  {
    if (NULL != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
    }
  }
  void init(int64_t tenant_id, int64_t blk_size = INTACT_NORMAL_AOBJECT_SIZE, int64_t parallel = 8)
  {
    attr_.tenant_id_ = tenant_id;
    attr_.label_ = name();
    ContextParam param;
    param.set_properties(ALLOC_THREAD_SAFE).set_parallel(parallel).set_mem_attr(attr_).set_ablock_size(blk_size);
    ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param);
  }
  int64_t id() { return attr_.tenant_id_; }
  static constexpr const char* name() { return "mem_context"; }
  void *alloc(size_t size) { return mem_context_->allocf(size, attr_); }
  void free(void* ptr) { mem_context_->free(ptr); }

private:
  MemoryContext mem_context_;
  ObMemAttr attr_;
};

template <class TestAllocator, class... Args>
int run_all_tests(const Args&... args)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = 1002;
  ObMallocAllocator *ma = ObMallocAllocator::get_instance();

  if (OB_FAIL(ma->create_and_add_tenant_allocator(tenant_id))) {
    OB_LOG(ERROR, "create_and_add_tenant_allocator failed", K(ret));
  } else {
    int64_t t0 = ObTimeUtility::current_time();
    run_test0<TestAllocator>(1<<22, 16, tenant_id, args...);
    int64_t t1 = OB_TSC_TIMESTAMP.fast_current_time();
    run_test1<TestAllocator>(64, 16, 1<<20, 100, tenant_id, args...);
    int64_t t2 = OB_TSC_TIMESTAMP.fast_current_time();
    run_test2<TestAllocator>(64, 16, 1<<20, 100, tenant_id, args...);
    int64_t t3 = OB_TSC_TIMESTAMP.fast_current_time();
    fprintf(stderr,"test0=%ld, test1=%ld, test2=%ld\n", t1-t0,t2-t1,t3-t2);

    if (OB_FAIL(ma->recycle_tenant_allocator(tenant_id))) {
      OB_LOG(ERROR, "recycle_tenant_allocator failed", K(ret));
    }
  }
  return ret;
}
TEST(TestAllocator, all)
{
  ASSERT_EQ(OB_SUCCESS, run_all_tests<TestMallocHook>());
  ASSERT_EQ(OB_SUCCESS, run_all_tests<TestMalloc>());
  ASSERT_EQ(OB_SUCCESS, run_all_tests<TestVSliceAlloc>());
  ASSERT_EQ(OB_SUCCESS, run_all_tests<TestFIFOAllocator>());
  ASSERT_EQ(OB_SUCCESS, run_all_tests<TestMemoryContext>());
}
int main(int argc, char **argv)
{
  OB_TSC_TIMESTAMP.init();
  init_malloc_hook();
  enable_malloc_v2(true);
  system("rm -f ./test_allocators.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_allocators.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
