#ifndef __COMMON_H__
#define __COMMON_H__

#include <assert.h>
#include <infiniband/verbs.h>
#include <libmemcached/memcached.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <boost/version.hpp>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <xmmintrin.h>

#include "Debug.h"
#include "HugePageAlloc.h"
#include "Rdma.h"
#include "WRLock.h"

// CONFIG_ENABLE_EMBEDDING_LOCK and CONFIG_ENABLE_CRC
// **cannot** be ON at the same time

// #define CONFIG_ENABLE_EMBEDDING_LOCK
// #define CONFIG_ENABLE_CRC

// #define LEARNED 1
// #define PAGE_OFFSET 1

#define LATENCY_WINDOWS 1000000

#define STRUCT_OFFSET(type, field) \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

#define UNUSED(x) (void)(x)

#define MAX_MACHINE 5

#define MEMORY_NODE_NUM 5

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define MESSAGE_SIZE 96  // byte

#define POST_RECV_PER_RC_QP 128

#define RAW_RECV_CQ_COUNT 512

// { app thread
#define MAX_APP_THREAD 96

#define APP_MESSAGE_NR 96

// }

// { dir thread

#define DIR_MESSAGE_NR 512
// }

#define NR_DIRECTORY 4

#define LOCK_VERSION 1

// From SMART
#define POLL_CQ_MAX_CNT_ONCE 8
#define MAX_CORO_NUM 2
#define ALLOC_ALLIGN_BIT 8

#define ROUND_UP(x, n) (((x) + (1 << (n)) - 1) & ~((1 << (n)) - 1))
#define ROUND_DOWN(x, n) ((x) & ~((1 << (n)) - 1))

void bindCore(uint16_t core);
char *getIP();
char *getMac();

inline int bits_in(std::uint64_t u) {
  auto bs = std::bitset<64>(u);
  return bs.count();
}

#include <boost/coroutine/all.hpp>

using CoroYield = boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall = boost::coroutines::symmetric_coroutine<void>::call_type;

using CheckFunc = std::function<bool()>;
using CoroQueue = std::queue<std::pair<uint16_t, CheckFunc>>;
struct CoroContext {
  CoroYield *yield;
  CoroCall *master;
  CoroQueue *busy_waiting_queue;
  int coro_id;
};

// namespace define {
// constexpr uint64_t kMaxLeafSplit = 10;

// constexpr uint64_t kMaxNumofInternalInsert = 5000;
// }  // namespace define

namespace define {

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t dsmSize = 96;  // GB  [CONFIG]
constexpr uint64_t kMemorySideHashSize = 24 * GB;
constexpr uint64_t kChunkSize = 32 * MB;

// RDMA buffer
constexpr uint64_t rdmaBufferSize = 2;  // GB  [CONFIG]
constexpr int64_t aligned_cache = ~((1ULL << 6) - 1);
constexpr int64_t kPerThreadRdmaBuf =
    (rdmaBufferSize * define::GB / MAX_APP_THREAD) & aligned_cache;
constexpr int64_t kSmartPerCoroRdmaBuf =
    (kPerThreadRdmaBuf / MAX_CORO_NUM) & aligned_cache;
constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

// for store root pointer
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0, "XX");

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = 128 * 1024;

// number of locks
// we do not use 16-bit locks, since 64-bit locks can provide enough
// concurrency. if you want to use 16-bit locks, call *cas_dm_mask*
#ifdef LOCK_VERSION
constexpr uint64_t kNumOfLock = kLockChipMemSize / (sizeof(uint64_t) * 2);
#else
constexpr uint64_t kNumOfLock = kLockChipMemSize / sizeof(uint64_t);
#endif

// For SMART
constexpr uint64_t kLocalLockNum =
    4 * MB;  // tune to an appropriate value (as small as possible without
             // affect the performance)
constexpr uint64_t kOnChipLockNum = kLockChipMemSize * 8;  // 1bit-lock

// level of tree
constexpr uint64_t kMaxLevelOfTree = 7;

// To align with MAX_CORO_NUM
constexpr uint16_t kMaxCoro = 8;
// constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

constexpr uint8_t kMaxHandOverTime = 8;

constexpr int kIndexCacheSize = 512;  // MB
}  // namespace define

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

// For Tree
using Key = uint64_t;
using Value = uint64_t;
constexpr Key kKeyMin = std::numeric_limits<Key>::min();
constexpr Key kKeyMax = std::numeric_limits<Key>::max();
constexpr Value kValueNull = 0;
constexpr uint32_t kInternalPageSize = 1024;
constexpr uint32_t kLeafPageSize = 1024;

using TS = uint64_t;

constexpr TS kTSMax = std::numeric_limits<TS>::max();
constexpr TS kTSMin = std::numeric_limits<TS>::min();

constexpr TS NULL_TS = kTSMax;
constexpr TS START_TS = kTSMin;
constexpr Value kValueMin = 1;
constexpr Value kValueMax = std::numeric_limits<Value>::max();

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

namespace XMD {

void yield(int count); 

// -------------------------------------------------------------------------------------
// ensure is similar to assert except that it is never out compiled
#define always_check(e)                                                \
  do {                                                                 \
    if (__builtin_expect(!(e), 0)) {                                   \
      std::stringstream ss;                                            \
      ss << __func__ << " in " << __FILE__ << ":" << __LINE__ << '\n'; \
      ss << " msg: " << std::string(#e);                               \
      throw std::runtime_error(ss.str());                              \
    }                                                                  \
  } while (0)

#define ENSURE_ENABLED 1
#ifdef ENSURE_ENABLED
#define ensure(e) always_check(e);
#else
#define ensure(e) \
  do {            \
  } while (0);
#endif

using u64 = uint64_t;

#define NUMA_1_CPU_s1 24
#define NUMA_1_CPU_s2 72

#define KEY_PAGE

// CONFIG_ENABLE_EMBEDDING_LOCK and CONFIG_ENABLE_CRC
// **cannot** be ON at the same time

// #define CONFIG_ENABLE_EMBEDDING_LOCK
// #define CONFIG_ENABLE_CRC

#define RING_SUB(a, b, n) ((a + n - b) % n)
#define RING_ADD(a, b, n) ((a + b) % n)

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

// #define STRING_KEYS
// consider for extend variable length key values
#ifdef STRING_KEYS

using Key = std::array<uint8_t, kKeyLen>;

template <std::size_t N>
constexpr std::array<uint8_t, N> fillArray(uint8_t value) {
  std::array<uint8_t, N> arr = {};
  for (std::size_t i = 0; i < N; ++i) {
    arr[i] = value;
  }
  return arr;
}

template <std::size_t N>
static inline Key str2key(const std::string &key) {
  // assert(key.size() <= define::keyLen);
  Key res = fillArray<N>(0);
  assert(key.size() <= N);
  std::copy(key.begin(), key.end(), res.begin() + (N - key.size()));
  return res;
}

template <std::size_t N>
static inline Key u642key(uint64_t key) {
  Key res = fillArray<N>(0);
  assert(sizeof(key) <= N);
  for (int i = N - 8; i < N; i++) {
    res[i] = static_cast<uint8_t>((key >> (8 * (7 - i))) & 0xFF);
  }
  return res;
}

template <std::size_t N>
void printArray(const std::array<uint8_t, N> &arr) {
  std::cout << "{ ";
  for (std::size_t i = 0; i < N; ++i) {
    std::cout << static_cast<int>(arr[i]);
    if (i < N - 1) {
      std::cout << ", ";
    }
  }
  std::cout << " }" << std::endl;
}

template <std::size_t N>
std::array<uint8_t, N> arrayPlus(const std::array<uint8_t, N> &arr,
                                 uint64_t value) {
  std::array<uint8_t, N> result = arr;
  uint64_t carry = value;

  for (std::size_t i = 0; i < N; ++i) {
    uint64_t temp = static_cast<uint64_t>(result[i]) + (carry & 0xFF);
    result[i] = static_cast<uint8_t>(temp & 0xFF);
    carry = (carry >> 8) + (temp >> 8);  // Shift carry for next byte
    if (carry == 0) {
      break;  // No need to continue if there's no carry left
    }
  }
  return result;
}

template <std::size_t N>
std::array<uint8_t, N> arraySub(const std::array<uint8_t, N> &arr,
                                uint64_t value) {
  std::array<uint8_t, N> result = arr;
  int64_t borrow = value;

  for (std::size_t i = 0; i < N; ++i) {
    int64_t temp = static_cast<int64_t>(result[i]) - (borrow & 0xFF);
    if (temp < 0) {
      result[i] = static_cast<uint8_t>(temp + 256);
      borrow = 1;
    } else {
      result[i] = static_cast<uint8_t>(temp);
      borrow = 0;
    }
    borrow >>= 8;  // Shift borrow for next byte
    if (borrow == 0) {
      break;  // No need to continue if there's no borrow left
    }
  }
  return result;
}

constexpr Key kKeyMin = fillArray<kKeyLen>(0);
constexpr Key kKeyMax = fillArray<kKeyLen>(255);

using PValue = union {
  Value value;
  uint8_t _padding[ksimulatedValLen];
};

#endif

struct KVTS {
  Key k;
  Value v;
  TS ts;

  KVTS() {}
  KVTS(Key k, Value v, TS ts) : k(k), v(v), ts(ts) {}

  inline void self_print() const {
    // print Key
    printf("K %lu,", k);
    printf("V %lu, TS %lu\n", v, ts);
  }

  inline bool operator>(const KVTS &rhs) const {
    return k != rhs.k ? k > rhs.k : ts > rhs.ts;
  }

  inline bool operator<(const KVTS &rhs) const {
    return k != rhs.k ? k < rhs.k : ts < rhs.ts;
  }

  inline bool operator==(const KVTS &rhs) const {
    return k == rhs.k && ts == rhs.ts;
  }
} __attribute__((packed));

// Note: our RNICs can read 1KB data in increasing address order (but not for
// 4KB)
constexpr uint32_t kInternalPageSize = 1024;
constexpr uint32_t kLeafPageSize = 1024;

#if defined(SINGLE_KEY)
constexpr uint32_t kMcPageSize = 1024;
#elif defined(KEY_PAGE)
constexpr uint32_t kMcPageSize = 4096 - sizeof(struct ibv_grh);
#elif defined(FILTER_PAGE)
constexpr uint32_t kMcPageSize = 1024
#endif

constexpr uint32_t kRecvMcPageSize = kMcPageSize + sizeof(struct ibv_grh);

// for core binding
constexpr uint16_t mcCmaCore = 95;
constexpr uint16_t filterCore = 94;
constexpr uint16_t rpcCore = 93;
constexpr uint16_t kMaxRpcCoreNum = 1;
constexpr uint16_t dirCore = rpcCore - kMaxRpcCoreNum;
constexpr uint16_t kMaxRwCoreNum = 4;

constexpr uint16_t batchCore = 24 + kMaxRwCoreNum;
constexpr uint16_t kMaxBatchInsertCoreNum = 8;

constexpr uint16_t multicastSendCore = batchCore + kMaxBatchInsertCoreNum;
constexpr uint16_t kMaxMulticastSendCoreNum = 1;

constexpr uint16_t rate_limit_core =
    multicastSendCore + kMaxMulticastSendCoreNum;

// for Rpc
constexpr int kMcMaxPostList = 128;
constexpr int kMcMaxRecvPostList = (MAX_MACHINE - 1) * kMcMaxPostList;
constexpr int kpostlist = 32;

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

constexpr bool is_prime(int n) {
  if (n <= 1) return false;
  if (n <= 3) return true;
  if (n % 2 == 0 || n % 3 == 0) return false;
  for (int i = 5; i * i <= n; i += 6) {
    if (n % i == 0 || n % (i + 2) == 0) return false;
  }
  return true;
}

constexpr int closest_prime(int num) {
  if (is_prime(num)) return num;

  int lower_prime = num - 1;
  while (!is_prime(lower_prime)) lower_prime--;

  int upper_prime = num + 1;
  while (!is_prime(upper_prime)) upper_prime++;

  return (num - lower_prime < upper_prime - num) ? lower_prime : upper_prime;
}

template <typename T>
class HugePages {
  T *memory;
  size_t size;           // in bytes
  size_t highWaterMark;  // max index
 public:
  HugePages(size_t size) : size(size) {
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (p == MAP_FAILED) throw std::runtime_error("mallocHugePages failed");
    memory = static_cast<T *>(p);
    highWaterMark = (size / sizeof(T));
  }

  size_t get_size() { return highWaterMark; }

  inline operator T *() { return memory; }

  inline T &operator[](size_t index) const { return memory[index]; }
  ~HugePages() { munmap(memory, size); }
};

struct myClock {
  using duration = std::chrono::nanoseconds;
  using rep = duration::rep;
  using period = duration::period;
  static constexpr bool is_steady = false;

  static uint64_t get_ts() {
    timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts)) throw 1;
    uint64_t timestamp =
        (uint64_t)(ts.tv_sec * 1000000000) + (uint64_t)(ts.tv_nsec);
    return timestamp;
  }
};

// olc lock
namespace cutil {
using ull_t = unsigned long long;

inline bool is_locked(ull_t version) { return ((version & 0b10) == 0b10); }
inline bool is_obsolete(ull_t version) { return ((version & 1) == 1); }

// the following API may be reimplemented in node.cuh
inline cutil::ull_t read_lock_or_restart(
    const std::atomic<cutil::ull_t> &version_lock_obsolete,
    bool &need_restart) {
  cutil::ull_t version = version_lock_obsolete.load();
  if (cutil::is_locked(version) || cutil::is_obsolete(version)) {
    need_restart = true;
  }
  return version;
}

inline void read_unlock_or_restart(
    const std::atomic<cutil::ull_t> &version_lock_obsolete,
    cutil::ull_t start_read, bool &need_restart) {
  // TODO: should we use spinlock to await?
  need_restart = (start_read != version_lock_obsolete.load());
}

inline void check_or_restart(
    const std::atomic<cutil::ull_t> &version_lock_obsolete,
    cutil::ull_t start_read, bool &need_restart) {
  read_unlock_or_restart(version_lock_obsolete, start_read, need_restart);
}

inline void upgrade_to_write_lock_or_restart(
    std::atomic<cutil::ull_t> &version_lock_obsolete, cutil::ull_t &version,
    bool &need_restart) {
  // if (version == atomicCAS(&version_lock_obsolete, version, version + 0b10))
  // {
  bool success =
      version_lock_obsolete.compare_exchange_strong(version, version + 0b10);
  if (success) {
    version = version + 0b10;
  } else {
    need_restart = true;
  }
}

inline void write_unlock(std::atomic<cutil::ull_t> &version_lock_obsolete) {
  version_lock_obsolete.fetch_add(0b10);
}

inline void write_unlock(std::atomic<cutil::ull_t> *version_lock_obsolete) {
  version_lock_obsolete->fetch_add(0b10);
}

inline void write_unlock_obsolete(
    std::atomic<cutil::ull_t> &version_lock_obsolete) {
  version_lock_obsolete.fetch_add(0b11);
}
}  // namespace cutil

// namespace memcached_util {

// static const std::string SERVER_ADDR = "10.16.94.136";
// static const std::string SERVER_PORT = "2378";

// std::string trim(const std::string &s);

// void memcached_Connect(memcached_st *&memc);

// void memcachedSet(struct memcached_st *memc, const char *key, uint32_t klen,
//                   const char *val, uint32_t vlen);

// char *memcachedGet(struct memcached_st *memc, const char *key, uint32_t klen,
//                    size_t *v_size = nullptr);

// uint64_t memcachedFetchAndAdd(struct memcached_st *memc, const char *key,
//                               uint32_t klen);

// void memcached_barrier(struct memcached_st *memc, const std::string
// &barrierKey,
//                        uint64_t num_server);
// };  // namespace memcached_util

}  // namespace XMD

#endif /* __COMMON_H__ */