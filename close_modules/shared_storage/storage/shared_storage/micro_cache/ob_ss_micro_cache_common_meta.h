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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_COMMON_META_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_COMMON_META_H_

#include "share/ob_define.h"
#include "share/ob_ptr_handle.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase 
{
namespace storage 
{
struct ObSSMicroCacheStat;

const int64_t SS_MEM_BUF_ALIGNMENT = 4096;
// If we use 'OB_UNIS_VERSION' to serialize struct, but want to serialize it into a fixed-length buf, please add it.
const int64_t SS_SERIALIZE_EXTRA_BUF_LEN = (sizeof(int64_t) + NS_::OB_SERIALIZE_SIZE_NEED_BYTES);
static const uint64_t SS_DATA_DEST_BIT = 48;
static const uint64_t SS_REUSE_VERSION_BIT = 16;
static const uint64_t SS_MAX_REUSE_VERSION = (1 << SS_REUSE_VERSION_BIT) - 1;
static const uint64_t SS_INVALID_REUSE_VERSION = UINT64_MAX;

constexpr int64_t PHY_BLK_MAX_REUSE_TIME = 10 * 60 * 1000 * 1000L; // 10min
constexpr int64_t SS_CACHE_SUPER_BLOCK_CNT = 2;
constexpr int64_t SS_MIN_CACHE_FILE_SIZE = 200 * 1024 * 1024L;
constexpr int64_t SS_META_ALLOCATOR_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;

constexpr int64_t SS_SLOW_TASK_COST_US = 5 * 1000 * 1000L; // 5s
constexpr int64_t SS_MINI_MODE_CACHE_FILE_SIZE = 800 * 1024 * 1024L; // 800MB
constexpr int64_t SS_MINI_MODE_ARC_LIMIT_PCT = 70;
constexpr int64_t SS_ARC_LIMIT_PCT = 84;
constexpr int64_t SS_ARC_LIMIT_SHRINK_PCT = 90; // work_limit = limit * 90%, to reserve space for prewarm
constexpr int64_t SS_ARC_LIMIT_MAX_PREWARM_PCT = 90;
constexpr int64_t SS_CLOSE_EVICTION_DIFF_SIZE = 100L * 1024 * 1024; // if valid_size >= work_limit - 100MB, we think it may evict soon
constexpr double SS_REORGAN_BLK_USAGE_RATIO = 0.85;

constexpr int64_t MAX_MEM_BLK_CNT = 50;           // def_pool_cnt + max_dynamic_cnt <= 50
constexpr int64_t MINI_MODE_BASE_MEM_BLK_CNT = 0; // base count(mini mode) used to calculate def_pool_cnt
constexpr int64_t BASE_MEM_BLK_CNT = 5;           // base count(non-mini mode) used to calculate def_pool_cnt
constexpr int64_t MIN_DYNAMIC_MEM_BLK_CNT = 4;    // min count of mem_block which is dynamically allocated
constexpr int64_t DISK_SIZE_PER_MEM_BLK = (50L * (1L << 30));   // increase one mem_block for every 50GB of disk size
constexpr int64_t MEMORY_SIZE_PER_MEM_BLK = (10L * (1L << 30)); // increase one mem_block for every 10GB of memory size
constexpr int64_t MINI_MODE_MAX_BG_MEM_BLK_CNT = 1; // max count of mem_block(mini_mode) which can be allocated for reorgan_task
constexpr int64_t MAX_BG_MEM_BLK_CNT = 3; // max count of mem_block which can be allocated for reorgan_task

constexpr int64_t MIN_CACHE_META_BLOCK_CNT_PCT = 1;  // min percentage that micro_meta can use in shared_blocks
constexpr int64_t MIN_CACHE_DATA_BLOCK_CNT_PCT = 80; // min percentage that micro_data can use in shared_blocks
constexpr int64_t MIN_REORGAN_BLK_CNT = 7;   // min count of blocks reserved for reorganize_task
constexpr int64_t MAX_REORGAN_BLK_CNT = 30;  // max count of blocks reserved for reorganize_task
// if avg_micro_size is less than 4KB, reorganize_task reserves at most MIN_REORGAN_BLK_CNT blocks
constexpr int64_t REORGAN_MIN_MICRO_SIZE = (1L << 12);
// avg_micro_size increases 1KB, one more block is reserved for reorganize_task, upper limit is MAX_REORGAN_BLK_CNT
constexpr int64_t REORGAN_BLK_SCALING_FACTOR = (1L << 10);
constexpr int64_t MAX_REORGAN_TASK_SCAN_CNT = 1000;

// the persistence cost of each meta is calculated as 85B when estimates micro_ckpt_blk_cnt
constexpr int64_t AVG_MICRO_META_PERSIST_COST = 85;

constexpr int64_t SS_MAX_ARC_HANDLE_OP_CNT = 500;
// when we want to evict/delete some cold micro, we need to get more micro_blocks and choose the coldest from these.
constexpr int64_t SS_MAX_ARC_FETCH_MULTIPLE = 8;
constexpr int64_t SS_MAX_ARC_FETCH_CNT = SS_MAX_ARC_HANDLE_OP_CNT * SS_MAX_ARC_FETCH_MULTIPLE;
static constexpr int64_t SS_DEF_CACHE_EXPIRATION_TIME = 2 * 24 * 3600L; // 2 day

enum class ObSSARCOpType : uint8
{
  SS_ARC_INVALID_OP = 0,
  SS_ARC_NEW_ADD = 1,
  SS_ARC_TASK_EVICT_OP = 2, // evict by arc_task
  SS_ARC_TASK_DELETE_OP = 3, // delete by arc_task
  SS_ARC_HIT_GHOST = 4,
  SS_ARC_HIT_T1 = 5,
  SS_ARC_HIT_T2 = 6,
  SS_ARC_ABNORMAL_DELETE_OP = 7, // delete by other, like 'failure handle', etc.
  SS_ARC_INVALIDATE_OP = 8,
  SS_ARC_EXPIRED_DELETE_OP = 9, // FARM COMPAT WHITELIST
};

enum class ObSSCacheHitType : uint8
{
  SS_CACHE_MISS = 0,
  SS_CACHE_HIT_MEM = 1,
  SS_CACHE_HIT_DISK = 2,
};

enum class ObSSPhyBlockType : uint8
{
  SS_INVALID_BLK_TYPE = 0,
  SS_SUPER_BLK = 1,               // store super_block info
  SS_NORMAL_BLK = 2,              // [Deprecated]
  SS_REORGAN_BLK = 3,             // for background reorganize task
  SS_CACHE_DATA_BLK = 4,          // store micro_block data
  SS_PHY_BLOCK_CKPT_BLK = 5,      // store phy_block checkpoint
  SS_MICRO_META_CKPT_BLK = 6,     // sotre micro_meta checkpoint
};

bool is_ckpt_block_type(const ObSSPhyBlockType type);
ObSSPhyBlockType get_mapping_block_type(const ObSSPhyBlockType type);

enum class ObSSMicroBlockCacheKeyMode : uint8
{
  PHYSICAL_KEY_MODE = 0,
  LOGICAL_KEY_MODE = 1,
  MAX_MODE
};

struct ObSSTabletCacheInfo
{
public:
  common::ObTabletID tablet_id_;
  int64_t t1_size_;
  int64_t t2_size_;

  ObSSTabletCacheInfo() : tablet_id_(common::ObTabletID::INVALID_TABLET_ID), t1_size_(0), t2_size_(0)
  {}

  void reset() { tablet_id_ = common::ObTabletID::INVALID_TABLET_ID; t1_size_ = 0; t2_size_ = 0; }
  bool is_valid() const { return tablet_id_.is_valid(); }
  ObSSTabletCacheInfo &operator=(const ObSSTabletCacheInfo &other);
  void add_micro_size(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size);
  int64_t get_valid_size() const { return t1_size_ + t2_size_; }
  TO_STRING_KV(K_(tablet_id), K_(t1_size), K_(t2_size));
};
typedef common::hash::ObHashMap<common::ObTabletID, ObSSTabletCacheInfo, common::hash::NoPthreadDefendMode> ObSSTabletCacheMap;

struct ObSSLSCacheInfo
{
public:
  static const int64_t OB_SS_LS_CACHE_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_LS_CACHE_INFO_VERSION);
public:
  share::ObLSID ls_id_;
  int64_t tablet_cnt_;
  int64_t t1_size_;
  int64_t t2_size_;

  ObSSLSCacheInfo() : ls_id_(share::ObLSID::INVALID_LS_ID), tablet_cnt_(0), t1_size_(0), t2_size_(0)
  {}

  void reset() { ls_id_ = share::ObLSID::INVALID_LS_ID; tablet_cnt_ = 0; t1_size_ = 0; t2_size_ = 0; }
  bool is_valid() const { return ls_id_.is_valid(); }
  int64_t get_valid_size() const { return t1_size_ + t2_size_; }
  ObSSLSCacheInfo &operator=(const ObSSLSCacheInfo &other);
  TO_STRING_KV(K_(ls_id), K_(tablet_cnt), K_(t1_size), K_(t2_size));
};

/*
 * The super block for micro_cache data_file, to record ckpt entry.
 * It will be persisted into the first or second block in micro_cache data_file.
 */
struct ObSSMicroCacheSuperBlock
{
public:
  static const int32_t SS_SUPER_BLK_MAGIC = 0xAACF87BB;
  static const int32_t DEFAULT_ITEM_CNT = 8;
public:
  int32_t magic_;
  uint64_t tenant_id_; // not persist for compat
  int64_t cache_file_size_;
  int64_t modify_time_us_;
  int64_t micro_ckpt_time_us_;
  common::ObSEArray<int64_t, DEFAULT_ITEM_CNT> micro_ckpt_entry_list_;
  common::ObSEArray<int64_t, DEFAULT_ITEM_CNT> blk_ckpt_entry_list_;
  common::ObSEArray<ObSSLSCacheInfo, DEFAULT_ITEM_CNT> ls_info_list_; // not persist
  common::ObSEArray<ObSSTabletCacheInfo, DEFAULT_ITEM_CNT> tablet_info_list_; // not persist
  ObSSMicroCacheSuperBlock() { reset(); }
  ObSSMicroCacheSuperBlock(const uint64_t tenant_id, const int64_t cache_file_size);
  ~ObSSMicroCacheSuperBlock() { reset(); }
  void reset();

  bool is_valid() const;
  int assign(const ObSSMicroCacheSuperBlock &other);
  int assign_by_ckpt(const bool is_micro_ckpt, const ObSSMicroCacheSuperBlock &other);
  int update_cache_file_size(const int64_t new_cache_file_size);
  void update_modify_time();
  void update_micro_ckpt_time();
  bool exist_checkpoint() const;
  bool is_valid_checkpoint() const;
  bool is_existed_in_micro_ckpt_entry_list(const int64_t blk_idx) const;
  bool is_existed_in_blk_ckpt_entry_list(const int64_t blk_idx) const;
  void clear_ckpt_entry_list();
  int add_ls_cache_info(const ObSSLSCacheInfo &ls_cache_info);
  int get_ls_cache_info(const share::ObLSID &ls_id, ObSSLSCacheInfo &ls_cache_info);
  int get_tablet_cache_info(const common::ObTabletID &tablet_id, ObSSTabletCacheInfo &tablet_cache_info);
  void get_ckpt_entry_list(const bool is_micro_ckpt, const ObSEArray<int64_t, DEFAULT_ITEM_CNT> *&entry_list) const;
  bool exist_ls_cache_info_list() const { return ls_info_list_.count() > 0; }
  int exist_ls_cache_info(const share::ObLSID &ls_id, int64_t &idx);
  int exist_tablet_cache_info(const common::ObTabletID &tablet_id, int64_t &idx);
  bool exist_tablet_cache_info_list() const { return tablet_info_list_.count() > 0; }

  TO_STRING_KV(K_(magic), K_(tenant_id), K_(cache_file_size), K_(modify_time_us), K_(micro_ckpt_time_us),
    "micro_entry_cnt", micro_ckpt_entry_list_.count(), K_(micro_ckpt_entry_list), 
    "blk_entry_cnt", blk_ckpt_entry_list_.count(), K_(blk_ckpt_entry_list),
    "ls_info_cnt", ls_info_list_.count(), K_(ls_info_list), 
    "tablet_info_cnt", tablet_info_list_.count(), K_(tablet_info_list));

public:
  static const int64_t OB_SS_MICRO_CACHE_SUPER_BLOCK_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_SUPER_BLOCK_VERSION);

private:
  void set_list_attr(const uint64_t tenant_id);
};

struct ObSSPhyBlockIdx 
{
public:
  int64_t block_idx_;
  ObSSPhyBlockIdx() : block_idx_(-1) {}
  ObSSPhyBlockIdx(const int64_t block_idx) : block_idx_(block_idx) {}
  int64_t hash() const
  {
    return static_cast<int64_t>(block_idx_);
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  bool operator==(const ObSSPhyBlockIdx &other) const
  {
    return block_idx_ == other.block_idx_;
  }

  TO_STRING_KV(K_(block_idx));
};

/*
 * To save each phy_block's state. 
 */
struct ObSSPhyBlockReuseInfo;
struct ObSSPhysicalBlock 
{
public:
  common::SpinRWLock lock_;
  uint32_t ref_cnt_;   // when read/write this block, need to inc ref_cnt firstly.
  union {
    uint64_t block_state_;
    struct {
      uint64_t reuse_version_ : 16;    // can't larger than SS_MAX_REUSE_VERSION
      uint64_t valid_len_ : 22;        // promise that not more than 2MB
      uint64_t is_free_ : 1;           // mark this block can be reused or not
      uint64_t is_sealed_ : 1;         // mark this block finish persisting micro block data // FARM COMPAT WHITELIST
      uint64_t gc_reuse_version_ : 16; // reuse_version can't be equal to gc_reuse_version // FARM COMPAT WHITELIST
      uint64_t block_type_ : 4; // FARM COMPAT WHITELIST
      uint64_t reserved_ : 4;
    };
  };
  int64_t alloc_time_us_;
  ObSSPhysicalBlock();
  // Don't use these two interface casually. This is for meeting 2DArray requirement.
  ObSSPhysicalBlock(const ObSSPhysicalBlock &other) 
    : lock_(), ref_cnt_(other.ref_cnt_ + 1), block_state_(other.block_state_),
      alloc_time_us_(other.alloc_time_us_)
  {}
  ObSSPhysicalBlock &operator=(const ObSSPhysicalBlock &other);

  ~ObSSPhysicalBlock() { reset(); }

  void reset();
  void reuse();

  uint32_t get_ref_count() const;
  uint64_t get_next_reuse_version() const;
  void update_gc_reuse_version(const uint64_t gc_reuse_version);
  void get_reuse_info(ObSSPhyBlockReuseInfo &reuse_info) const;
  void set_reuse_info(const uint64_t reuse_version, const uint64_t gc_reuse_version);
  void inc_ref_count();
  void dec_ref_count();
  bool has_no_ref() const;

  bool is_empty() const;
  bool can_reuse() const;
  bool can_reorganize(const int64_t block_size) const;
  bool is_sealed() const;
  bool is_free() const;
  int try_free(bool &succ_free);
  int set_first_used(const ObSSPhyBlockType block_tyep);
  void set_used_and_sealed(const ObSSPhyBlockType block_type);
  void set_sealed(const uint64_t valid_len); // set as sealed while update valid_len
  void set_reusable(const ObSSPhyBlockType block_type); // If one phy_blk is allocated but fail to use it, need to mark it reusable
  bool is_cache_data_block() const;
  uint64_t get_valid_len() const;
  uint64_t get_reuse_version() const;
  uint64_t get_gc_reuse_version() const;
  void set_valid_len(const uint64_t valid_len);
  int inc_valid_len(const int64_t phy_blk_idx, const int64_t delta_len);
  int dec_valid_len(const int64_t phy_blk_idx, const int64_t delta_len);
  ObSSPhyBlockType get_block_type() const;

  TO_STRING_KV(K_(ref_cnt), K_(reuse_version), K_(valid_len), K_(is_free), K_(is_sealed), K_(gc_reuse_version),
      K_(block_type), K_(alloc_time_us));

public:
  static const int64_t OB_SS_PHYSICAL_BLOCK_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_PHYSICAL_BLOCK_VERSION);

private:
  void try_delete_without_lock();
  uint64_t inner_get_next_reuse_version() const;
  void inner_reuse();
};

/*
 * To solve 'decrease phy_block's ref_cnt after finishing io', we wrap phy_block ptr as a handle.
 * When this handle destroy, it will decrease its' phy_block's ref_cnt automaticlly.
 * 
 * So why we need to inc/dec phy_block's ref_cnt？
 * Cuz we need to handle the conflict between 'read phy_block data' and 'reuse phy_block'. When we
 * access one phy_block, we should add its ref_cnt first. If one phy_block's ref_cnt > 0, don't allow
 * to reuse it.
 */
typedef ObPtrHandle<ObSSPhysicalBlock> ObSSPhysicalBlockHandle;

struct ObSSPhyBlockCommonHeader
{
public:
  static const int32_t SS_PHY_BLK_COMMON_HEADER_MAGIC = 0x010035AC;
  static const int32_t SS_PHY_BLK_COMMON_HEADER_VERSION = 1;

public:
  int32_t header_size_; // fixed size of struct
  int32_t version_;     // header version
  int32_t magic_;       // magic number
  int32_t payload_size_; // size of payload
  int32_t payload_checksum_; // crc64 of payload
  union {
    int32_t attr_;
    struct {
      int32_t blk_type_ : 8;
      int32_t reserved_ : 24;
    };
  };

  ObSSPhyBlockCommonHeader();
  ~ObSSPhyBlockCommonHeader() {}

  void reset();
  bool is_valid() const;
  void set_payload_size(const int32_t payload_size) { payload_size_ = payload_size; }
  void set_block_type(const ObSSPhyBlockType blk_type) { blk_type_ = static_cast<int32_t>(blk_type); }
  void calc_payload_checksum(const char *buf, const int32_t buf_size);
  int check_payload_checksum(const char *buf, const int32_t buf_size);
  bool is_cache_data_blk() const { return blk_type_ == static_cast<int32_t>(ObSSPhyBlockType::SS_CACHE_DATA_BLK); }
  bool is_super_blk() const { return blk_type_ == static_cast<int32_t>(ObSSPhyBlockType::SS_SUPER_BLK); }
  bool is_ckpt_blk() const { return is_ckpt_block_type(static_cast<ObSSPhyBlockType>(blk_type_)); }

  int serialize(char *buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  static int64_t get_serialize_size()
  {
    return sizeof(ObSSPhyBlockCommonHeader);
  }

  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(payload_size), K_(payload_checksum), K_(blk_type), K_(attr));
};

/*
 * When we persist a batch of micro_block data into phy_block(disk), we need to serialize this header
 * and persist it into this phy_block from where offset = 0. 
 */
struct ObSSNormalPhyBlockHeader 
{
public:
  static const int32_t SS_NORMAL_PHY_BLK_HEADER_MAGIC = 0x000087AA;

public:
  int32_t magic_;
  int32_t payload_checksum_; // not calc it, cuz we just calc crc in common_header
  int32_t payload_offset_;
  int32_t payload_size_;
  int32_t micro_count_; // count of micro_blocks stored in cur phy_block
  int32_t micro_index_offset_;
  int32_t micro_index_size_;

  ObSSNormalPhyBlockHeader();
  ~ObSSNormalPhyBlockHeader() { reset(); }

  void reset();
  bool is_valid() const;
  static int64_t get_fixed_serialize_size();

  TO_STRING_KV(K_(magic), K_(payload_checksum), K_(payload_offset), K_(payload_size), K_(micro_count),
    K_(micro_index_offset), K_(micro_index_size));

public:
  static const int64_t OB_SS_NORMAL_PHY_BLOCK_HEADER_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_NORMAL_PHY_BLOCK_HEADER_VERSION);
};

/*
 * For phy_block checkpoint
 */
struct ObSSPhyBlockPersistInfo
{
public:
  int64_t blk_idx_;
  uint64_t reuse_version_;

  ObSSPhyBlockPersistInfo() : blk_idx_(-1), reuse_version_(SS_INVALID_REUSE_VERSION) {}
  ObSSPhyBlockPersistInfo(const int64_t blk_idx, const uint64_t reuse_version)
    : blk_idx_(blk_idx), reuse_version_(reuse_version) {}

  bool is_valid() const { return (blk_idx_ >= SS_CACHE_SUPER_BLOCK_CNT && reuse_version_ >= 1 && 
                          reuse_version_ <= SS_MAX_REUSE_VERSION); }
  static int64_t get_max_serialize_size() { return (sizeof(int64_t) + 1) * 2 + SS_SERIALIZE_EXTRA_BUF_LEN; }
  TO_STRING_KV(K_(blk_idx), K_(reuse_version));

public:
  static const int64_t OB_SS_PHY_BLOCK_PERSIST_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_PHY_BLOCK_PERSIST_INFO_VERSION);
};

struct ObSSPhyBlockReuseInfo
{
public:
  int64_t blk_idx_;
  struct {
    uint64_t reuse_version_ : 16;
    uint64_t next_reuse_version_ : 16;
    uint64_t gc_reuse_version_ : 16;
    uint64_t need_reuse_ : 1;
    uint64_t reserved_ : 15;
  };

  ObSSPhyBlockReuseInfo() { reset(); }
  ~ObSSPhyBlockReuseInfo() {}
  void reset() { blk_idx_ = 0; reuse_version_ = 0; next_reuse_version_ = 0; gc_reuse_version_ = 0;
                 need_reuse_ = 0; reserved_ = 0; }
  bool reach_gc_reuse_version() const { return next_reuse_version_ == gc_reuse_version_; }
  bool is_valid() const { return (blk_idx_ >= SS_CACHE_SUPER_BLOCK_CNT && reuse_version_ >= 1 && 
                          reuse_version_ <= SS_MAX_REUSE_VERSION); }

  TO_STRING_KV(K_(blk_idx), K_(reuse_version), K_(next_reuse_version), K_(gc_reuse_version), K_(need_reuse));
};

struct ObSSMicroBlockId
{
public:
  blocksstable::MacroBlockId macro_id_;
  int32_t offset_;
  int32_t size_;

  ObSSMicroBlockId(const blocksstable::MacroBlockId &macro_id, const int64_t offset, const int64_t size)
    : macro_id_(macro_id), offset_(offset), size_(size)
  {}
  ObSSMicroBlockId()
    : macro_id_(), offset_(0), size_(0)
  {}

  OB_INLINE void reset()
  {
    macro_id_.reset();
    offset_ = 0;
    size_ = 0;
  }
  OB_INLINE bool is_valid() const { return macro_id_.is_valid() && offset_ > 0 && size_ > 0; }
  OB_INLINE bool operator == (const ObSSMicroBlockId &other) const
  {
    return (macro_id_ == other.macro_id_) && (offset_ == other.offset_) && (size_ == other.size_);
  }
  const blocksstable::MacroBlockId &get_macro_id() const { return macro_id_; }
  int32_t get_offset() const { return offset_; }
  int32_t get_size() const { return size_; }
  
  TO_STRING_KV(K_(macro_id), K_(offset), K_(size));

public:
  static const int64_t OB_SS_MICRO_BLOCK_ID_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_ID_VERSION);
};

struct ObSSMicroBlockCacheKey
{
public:
  ObSSMicroBlockCacheKeyMode mode_;
  union {
    ObSSMicroBlockId micro_id_;
    blocksstable::ObLogicMicroBlockId logic_micro_id_;
  };
  int64_t micro_crc_;

  ObSSMicroBlockCacheKey();
  explicit ObSSMicroBlockCacheKey(const ObSSMicroBlockId &micro_block_id);
  ObSSMicroBlockCacheKey(const blocksstable::ObLogicMicroBlockId &logic_micro_id,
                         const int64_t micro_crc);
  ObSSMicroBlockCacheKey(const ObSSMicroBlockCacheKey &other);
  virtual ~ObSSMicroBlockCacheKey() {}
  void reset();
  bool is_logic_key() const { return ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == mode_; }
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  ObSSMicroBlockCacheKey &operator=(const ObSSMicroBlockCacheKey &other);
  bool operator==(const ObSSMicroBlockCacheKey &other) const;
  const ObSSMicroBlockId &get_micro_id() const { return micro_id_; }
  const blocksstable::ObLogicMicroBlockId &get_logic_micro_id() const { return logic_micro_id_; }
  bool is_major_macro_key() const;
  common::ObTabletID get_major_macro_tablet_id() const;

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  static const int64_t OB_SS_MICRO_BLOCK_CACHE_KEY_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_CACHE_KEY_VERSION);
};

struct ObSSMicroBlockCacheKeyMeta
{
public:
  static const int64_t OB_SS_MICRO_BLOCK_CACHE_KEY_META_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_CACHE_KEY_META_VERSION);
public:
  ObSSMicroBlockCacheKeyMeta();
  ObSSMicroBlockCacheKeyMeta(const ObSSMicroBlockCacheKey &micro_key,
                             const uint32_t data_crc,
                             const int32_t data_size,
                             const bool is_in_l1);
  ObSSMicroBlockCacheKeyMeta(const ObSSMicroBlockCacheKeyMeta &other);
  virtual ~ObSSMicroBlockCacheKeyMeta() {}
  int assign(const ObSSMicroBlockCacheKeyMeta &other);
  bool operator==(const ObSSMicroBlockCacheKeyMeta &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  bool is_valid() const;
  TO_STRING_KV(K_(micro_key), K_(data_crc), K_(data_size), K_(is_in_l1));

public:
  ObSSMicroBlockCacheKey micro_key_;
  uint32_t data_crc_;
  int32_t data_size_;
  bool is_in_l1_;
};

/*
 * Cuz a phy_block may store multi micro_blocks which belong to different macro_blocks, 
 * thus we need to save these micro_blocks' info as index, include micro_block_key and
 * each micro_block's data size.
 * 
 * It will be persisted into phy_block. 
 */
struct ObSSMicroBlockIndex
{
public:
  ObSSMicroBlockCacheKey micro_key_;
  int32_t size_;

  ObSSMicroBlockIndex() : micro_key_(), size_(0) {}
  ObSSMicroBlockIndex(const ObSSMicroBlockCacheKey &micro_key, const int32_t size)
    : micro_key_(micro_key), size_(size) {}
  ~ObSSMicroBlockIndex() { reset(); }

  bool is_valid() const { return micro_key_.is_valid() && size_ > 0; }
  void reset() { micro_key_.reset(); size_ = 0; }
  const ObSSMicroBlockCacheKey &get_micro_key() const { return micro_key_; }
  int32_t get_size() const { return size_; }
  ObSSMicroBlockIndex &operator=(const ObSSMicroBlockIndex &other);
  bool operator==(const ObSSMicroBlockIndex &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  TO_STRING_KV(K_(micro_key), K_(size));

public:
  static const int64_t OB_SS_MICRO_BLOCK_INDEX_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_INDEX_VERSION);
};

struct ObSSPhyBlockIdxRange
{
public:
  static const int64_t OB_SS_PHYSICAL_BLOCK_RANGE_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_PHYSICAL_BLOCK_RANGE_VERSION);
public:
  ObSSPhyBlockIdxRange();
  ObSSPhyBlockIdxRange(const int64_t start_blk_idx, const int64_t end_blk_idx);
  virtual ~ObSSPhyBlockIdxRange() {}
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(start_blk_idx), K_(end_blk_idx));

public:
  int64_t start_blk_idx_;
  int64_t end_blk_idx_;
};

/*
 * Currently, only used in ObSSMemBlock
 */
struct ObSSMemMicroInfo 
{
public:
  int32_t offset_; // the offset of micro_block's data in mem_block(not micro_block_id's offset)
  int32_t size_;   // the micro_block data size

  ObSSMemMicroInfo() : offset_(0), size_(0) {}
  ObSSMemMicroInfo(const int32_t offset, const int32_t size) : offset_(offset), size_(size) {}
  ObSSMemMicroInfo(const ObSSMemMicroInfo &other) : offset_(other.offset_), size_(other.size_) {}
  ~ObSSMemMicroInfo() { reset(); }

  bool is_valid() const { return (offset_ > 0) && (size_ > 0); }
  void reset() { offset_ = 0; size_ = 0; }

  TO_STRING_KV(K_(offset), K_(size));
};

/*
 * To solve conflict between 'read mem_block data' with 'persist_mem_block'.
 * If we are reading a micro_block data from mem_block, we need to firstly add this
 * mem_block's ref_cnt. If a mem_block's ref_cnt > 0, don't allow to persist data of
 * this mem_block into phy_block. Cuz if we finish persist_task, we will release this
 * mem_block, thus the read request will go wrong.
 */
struct ObSSMemBlock;
typedef ObPtrHandle<ObSSMemBlock> ObSSMemBlockHandle;

/*
 * If we want to cache one micro_block data, we will store it in memory firstly.
 * Only when we hold about 2MB micro block data, we will persist these micro_blocks
 * data into physical_block(disk).
 * 
 * The content in one physical block, it will be like below format:
 * " header | micro1-micro2-micro3...microN | micro_idxN...micro_idx3-micro_idx2-micro_idx1"
 * Data of micro_block is written after 'header'; MicroIndex is written from tail by reverse order.
 * 
 * That means, the micro_block data will be less than @block_size in one physical block.
 */
struct ObSSMemBlockPool;
struct ObSSMemBlock 
{
public:
  typedef common::hash::ObHashMap<ObSSMicroBlockCacheKey, ObSSMemMicroInfo> MicroPhyInfoMap;
  bool is_fg_;             // true: used for foreground micro_block cache; false: used for reorganize_task
  uint32_t ref_cnt_;       // to control the conflicts between read_mem_data & persist_mem_data
  uint32_t micro_count_;   // count of micro_block that successfully add data into mem_block
  uint32_t handled_count_; // count of micro_block that have tried add_or_update meta after adding data
  uint32_t data_size_;     // total size of micro_block data, which is stored in data_buf_
  uint32_t index_size_;    // total size of micro_block index, which is stored in index_buf_
  uint32_t data_buf_size_; // the capacity of data_buf_
  uint32_t index_buf_size_;// the capacity of index_buf_
  uint32_t reserved_size_; // used to store phy_block_header, etc.
  uint32_t reuse_version_; // mem_block's reuse_version, when free, will inc this version

  // Definition of valid micro_block: micro blocks that really increases the valid_len of mem_block
  uint32_t valid_val_;     // length of total valid micro_block in this mem_block
  uint32_t valid_count_;   // count of valid micro_block
  char *data_buf_;         // used to store micro_blocks' data, capacity = data_buf_size_
  char *index_buf_;        // used to store micro_blocks' index, capacity = index_buf_size_
  MicroPhyInfoMap micro_offset_map_; // save each micro_block's data_offset & data_size
  ObSSMemBlockPool &mem_blk_pool_;

  ObSSMemBlock(ObSSMemBlockPool &pool);
  ~ObSSMemBlock() { destroy(); }
  int init(const uint64_t tenant_id, char *data_buf, const uint32_t data_buf_size, 
           char *index_buf, const uint32_t index_buf_size);
  void destroy();
  void reset();
  void reuse();

  bool exist(const ObSSMicroBlockCacheKey &micro_key) const;
  bool is_valid() const;
  void inc_micro_count();
  void inc_handled_count();
  // Why we need to check if mem_block is completed before persisting it?
  // Because it's not an atomic operation to add data and meta for a micro_block. When mem_block is filled with data,
  // it will be put into the sealed queue, and then the persist_task can get and process this mem_block. But at this
  // time, some of the micro_block's meta may not been added or updated.
  bool is_completed() const;
  bool has_no_ref() const;
  void add_valid_micro_block(const uint32_t delta_size);
  bool is_fg_mem_blk() const;
  void set_is_fg(const bool is_fg);
  uint32_t get_payload_size() const; // micro_data_size + micro_index_size
  uint32_t get_micro_index_offset() const;
  int calc_write_location(const ObSSMicroBlockCacheKey &micro_key, const int32_t size, int32_t &data_offset, 
                          int32_t &idx_offset);
  int write_micro_data(const ObSSMicroBlockCacheKey &micro_key, const char *micro_data, const int32_t size, 
                       const int32_t data_offset, const int32_t idx_offset, uint32_t &crc);
  int get_micro_data(const ObSSMicroBlockCacheKey &micro_key, const int32_t size, const uint32_t crc, char *&buf) const;
  int handle_when_sealed();

  void inc_ref_count();
  void dec_ref_count();
  int try_free();
  int try_free(bool &succ_free);
  
  bool is_reuse_version_match(const uint32_t reuse_version) const;
  int get_all_micro_keys(common::ObIArray<ObSSMicroBlockCacheKey> &micro_keys) const;

  TO_STRING_KV(K_(is_fg), K_(ref_cnt), K_(micro_count), K_(handled_count), K_(data_size), K_(index_size),
      K_(data_buf_size), K_(index_buf_size), K_(reserved_size), K_(reuse_version), K_(valid_val), K(valid_count_),
      KP_(data_buf));

  // for test
  int get_micro_data(const ObSSMicroBlockCacheKey &micro_key, char *buf, const int32_t size, const uint32_t crc) const;
private:
  uint32_t inner_get_next_reuse_version() const;
  void calc_reserved_size();
  bool is_large_micro(const int32_t delta_size) const; 
  bool has_enough_space(const int32_t delta_size, const int32_t idx_size) const;
  bool check_size_valid() const;
  int check_micro_data_crc(const int32_t data_pos, const int32_t size, const uint32_t crc) const;
};

/*
 * To improve add_micro_block_data performance, we will use this pool to allocate
 * some mem_block ahead of time. When current mem_block doesn't have enough space
 * store micro_block data, we will seal current mem_block, and allocate new one.
 * 
 * When the sealed mem_block's data is persisted into disk, this sealed mem_block
 * can be free.
 */
class ObSSMemBlockPool
{
public:
  ObSSMemBlockPool(ObSSMicroCacheStat &cache_stat);
  ~ObSSMemBlockPool() { destroy(); }

  // @def_count, count of pre_created mem_blocks. When free, it will be put into free_list
  // @max_count, equal to @def_count + max_extra_count_, for extra mem_block, when free, will be destroyed.
  // @max_count - @max_bg_mem_blk_cnt = 'max_fg_mem_blk_cnt'
  int init(const uint64_t tenant_id, const uint32_t block_size, const int64_t def_count, const int64_t max_cnt,
           const int64_t max_bg_mem_blk_cnt);
  void destroy();

  int alloc(ObSSMemBlock *&mem_block, const bool is_fg);
  int free(ObSSMemBlock *mem_block);

  bool is_alloc_dynamiclly(ObSSMemBlock *mem_block);
  OB_INLINE int64_t get_max_count() const { return def_count_ + max_extra_count_; }
  OB_INLINE int64_t get_fg_max_count() const { return get_max_count() - max_bg_count_; }
  OB_INLINE int64_t get_bg_max_count() const { return max_bg_count_; }
  int64_t get_free_mem_blk_cnt(const bool is_fg) const;

private:
  int pre_create_mem_blocks();
  void create_mem_block_on_fail(bool is_fg);
  int create_dynamic_mem_block(ObSSMemBlock *&mem_block);
  int destroy_free_list();
  int inner_alloc(ObSSMemBlock *&mem_block, char *data_buf, char *index_buf);
  void inner_destroy(ObSSMemBlock *mem_block);
  bool is_pre_created_mem_blk(ObSSMemBlock *mem_block) const;
  int check_free_mem_blk_enough(const bool is_fg);

private:
  int64_t DEF_WAIT_TIMEOUT_MS = 10 * 1000; // 10s

private:
  bool is_inited_;
  uint32_t block_size_;
  uint64_t tenant_id_;
  int64_t def_count_;        // count of pre created mem_blocks, will use @total_data_buf
  int64_t max_extra_count_;  // maximum count of mem_blocks that can be dynamically created
  int64_t used_extra_count_; // count of mem_blocks that have been dynamically created
  int64_t used_fg_count_;    // count of mem_blocks that have been used for foreground micro_block cache
  int64_t max_bg_count_;     // maximum count of mem_blocks that can be used for reorganize_task
  int64_t used_bg_count_;    // count of mem_blocks that have been used for reoranize_task
  common::ObThreadCond cond_;// to control bg_mem_block produce and consume
  char *total_data_buf_;     // a contiguous memory space, will be splited into several parts.
  common::ObFixedQueue<ObSSMemBlock> free_list_;
  int32_t mem_blk_data_buf_size_;
  int32_t mem_blk_index_buf_size_;
  ObSSMicroCacheStat &cache_stat_;
};

/*
 * Each micro_block_meta will represents a cached micro_block
 * For a cached micro_block, there may exists the following state:
 * 1. Cached in memory: that means the micro_block data is stored in memory;
 * 2. Cached in disk:   that means the micro_block data is stored in phy_block;
 * 3. Cached ghost:     that means the micro_block data may not exist, only exist meta.
 * 
 * For managing memory, the micro_block_key will be saved with micro_block_meta together.
 */
struct ObSSMicroBaseInfo;
struct ObSSMicroBlockMeta 
{
public:
  union {
    uint64_t first_val_;
    struct {
      uint64_t reuse_version_ : SS_REUSE_VERSION_BIT; // reuse_version of phy_block which cur micro_block persisted into
      uint64_t data_dest_ : SS_DATA_DEST_BIT;         // offset in cache_data_file when persisted or mem_block pointer
    };
  };

  union {
    uint64_t second_val_;
    struct {
      uint64_t access_time_ : 32; // record the access time(/s), support LRU cache algo
      uint64_t length_ : 21;      // micro_block data size(max_size=2^21-1, actually, enough for a micro_block) 
      uint64_t is_in_l1_ : 1;     // 0: in ARC L2; 1: in ARC L1
      uint64_t is_in_ghost_ : 1;  // 0: in ARC T1/T2; 1: in ARC B1/B2
      uint64_t is_persisted_ : 1; // 0: data in memory; 1: data in disk
      uint64_t is_reorganizing_ : 1; // whether in reorganize_task
      uint64_t reserved_ : 7;
    };
  };
  uint32_t ref_cnt_;
  uint32_t crc_;
  ObSSMicroBlockCacheKey micro_key_;

  ObSSMicroBlockMeta();
  ~ObSSMicroBlockMeta() { destroy(); }

  void destroy();
  void reset();
  void reuse();
  void mark_invalid();
  int init(const ObSSMicroBlockCacheKey &micro_key, const ObSSMemBlockHandle &mem_blk_handle, const uint32_t length,
      const uint32_t crc);
  int validate_micro_meta(const ObSSMicroBlockCacheKey &micro_key, const ObSSMemBlockHandle &mem_blk_handle,
      const uint32_t length, const uint32_t crc);
  int update_reorganizing_state(const ObSSMemBlockHandle &mem_blk_handle);
  // micro_meta's each member value is valid
  bool is_valid_field() const;
  // is_valid_field && (mem_block or phy_block) reuse_version match
  bool is_valid() const;
  // is_valid_field && !is_persisted && the same mem_blk && mem_blk's reuse_version match
  bool is_valid(const ObSSMemBlockHandle &mem_blk_handle) const;
  // is_valid_filed && is_persisted && phy_blk's reuse_version match
  bool is_persisted_valid() const;
  bool is_valid_for_ckpt(const bool print_log = false) const;
  bool is_expired(const int64_t expiration_time = SS_DEF_CACHE_EXPIRATION_TIME) const;
  bool can_evict() const;
  bool can_delete() const;
  bool can_reorganize() const;
  bool is_valid_reorganizing_state() const;
  ObSSMemBlock* get_mem_block() const;
  OB_INLINE bool is_in_ghost() const { return is_in_ghost_; }
  OB_INLINE bool is_in_l1() const { return is_in_l1_; }
  OB_INLINE bool is_persisted() const { return is_persisted_; }
  OB_INLINE bool is_reorganizing() const { return is_reorganizing_; }
  OB_INLINE int32_t length() const { return length_; }
  OB_INLINE uint64_t data_dest() const { return data_dest_; }
  OB_INLINE uint64_t reuse_version() const { return reuse_version_; }
  OB_INLINE uint32_t crc() const { return crc_; }
  OB_INLINE uint32_t ref_cnt() const { return ref_cnt_; }
  OB_INLINE uint64_t access_time() const { return access_time_; }
  void update_access_time();
  void update_access_time(const int64_t delta_time_s);
  const ObSSMicroBlockCacheKey &get_micro_key() const { return micro_key_; }
  ObSSMicroBlockCacheKey &micro_key() { return micro_key_; }
  void get_micro_base_info(ObSSMicroBaseInfo &micro_info) const;

  bool has_no_ref() const;
  void inc_ref_count();
  void dec_ref_count();
  void try_free();
  void free();

  uint64_t get_heat_val() const;
  void transfer_arc_seg(const ObSSARCOpType &op_type);
  bool check_reorganizing_state() const;
  // It is close to the max serialized size, but not very accurate, cuz we allow micro_meta_ckpt failed if there
  // not exist enough disk_space.
  static int64_t get_max_serialize_size() 
  { 
    return (sizeof(uint64_t) + sizeof(uint32_t) + 2) * 2 + sizeof(ObSSMicroBlockCacheKey) + SS_SERIALIZE_EXTRA_BUF_LEN; 
  }

  // NOTICE: won't consider 'ref_cnt' in operator= and operator==
  ObSSMicroBlockMeta &operator=(const ObSSMicroBlockMeta &other);
  bool operator==(const ObSSMicroBlockMeta &other) const;

  TO_STRING_KV(K_(first_val), K_(reuse_version), K_(data_dest), K_(access_time), K_(length), K_(is_in_l1),
    K_(is_in_ghost), K_(is_persisted), K_(is_reorganizing), K_(ref_cnt), K_(crc), K_(micro_key));

public:
  static const int64_t OB_SS_MICRO_BLOCK_META_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_META_VERSION);

private:
  void inner_set_mem_block(const ObSSMemBlockHandle &mem_blk_handle);
  void inner_validate_micro_meta(const ObSSMicroBlockCacheKey &micro_key, const ObSSMemBlockHandle &mem_blk_handle,
      const uint32_t length, const uint32_t crc);
  uint32_t inner_get_cur_time_s() const;
  bool is_mem_block_match() const;
  bool is_mem_block_match(const ObSSMemBlockHandle &mem_blk_handle) const;
  bool is_phy_block_match() const;
  bool is_phy_block_match(uint64_t &phy_blk_reuse_version) const;
};

typedef ObPtrHandle<ObSSMicroBlockMeta> ObSSMicroBlockMetaHandle;

struct ObSSMicroBaseInfo
{
public:
  bool is_in_l1_;
  bool is_in_ghost_;
  bool is_persisted_;
  int32_t size_;
  uint32_t crc_;
  uint64_t data_dest_;
  uint64_t reuse_version_;

  ObSSMicroBaseInfo() { reset(); }
  ~ObSSMicroBaseInfo() {}
  void reset();
  ObSSMicroBaseInfo &operator=(const ObSSMicroBaseInfo &other);
  bool is_valid() const { return size_ > 0; }

  TO_STRING_KV(K_(is_in_l1), K_(is_in_ghost), K_(is_persisted), K_(size), K_(crc), K_(data_dest), K_(reuse_version));
};

/*
 * DO NOT modify the internal ref_cnt_ of micro_meta to prevent it from being  
 * recycled by SSMicroMetaAlloc when ref_cnt_ drops to 0.
 */
 struct ObSSMicroMetaSnapshot
{
  public:
    ObSSMicroBlockMeta micro_meta_;
  
    ObSSMicroMetaSnapshot() { reset(); }
    ~ObSSMicroMetaSnapshot() {}
    ObSSMicroMetaSnapshot &operator=(const ObSSMicroMetaSnapshot &other);
    void reset() { micro_meta_.reset(); }
    int init(const ObSSMicroBlockMeta& micro_meta);
    bool is_in_l1() const { return micro_meta_.is_in_l1(); }
    bool is_in_ghost() const { return micro_meta_.is_in_ghost(); }
    uint64_t data_dest() const { return micro_meta_.data_dest(); }
    uint64_t reuse_version() const { return micro_meta_.reuse_version(); }
    int32_t length() const { return micro_meta_.length(); }
    uint64_t heat_val() const { return micro_meta_.get_heat_val(); }
    bool can_evict() const { return micro_meta_.can_evict(); }
    bool can_delete() const { return micro_meta_.can_delete(); }
    const ObSSMicroBlockCacheKey &micro_key() const { return micro_meta_.get_micro_key(); }
    TO_STRING_KV(K_(micro_meta));
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_COMMON_META_H_ */
