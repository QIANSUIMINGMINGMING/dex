#ifndef __COMMON_H__
#define __COMMON_H__

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <atomic>
#include <bitset>
#include <limits>
#include <queue>
#include <utility>

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

#define STRUCT_OFFSET(type, field)                                             \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

#define UNUSED(x) (void)(x)

#define MAX_MACHINE 5

#define MEMORY_NODE_NUM 5

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define MESSAGE_SIZE 96 // byte

#define POST_RECV_PER_RC_QP 128

#define RAW_RECV_CQ_COUNT 512

// { app thread
#define MAX_APP_THREAD 48

#define APP_MESSAGE_NR 96

// }

// { dir thread

#define DIR_MESSAGE_NR 512
// }

#define NR_DIRECTORY 1

#define LOCK_VERSION 1

// From SMART
#define POLL_CQ_MAX_CNT_ONCE 8
#define MAX_CORO_NUM 8

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


#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

// 定义sched_attr结构
struct sched_attr {
  uint32_t size;

  uint32_t sched_policy;
  uint64_t sched_flags;

  // SCHED_NORMAL, SCHED_BATCH
  int32_t sched_nice;

  // SCHED_FIFO, SCHED_RR
  uint32_t sched_priority;

  // SCHED_DEADLINE
  uint64_t sched_runtime;
  uint64_t sched_deadline;
  uint64_t sched_period;
};

// 定义SCHED_DEADLINE的系统调用号
#ifndef SYS_sched_setattr
#if defined(__x86_64__)
#define SYS_sched_setattr 314
#define SYS_sched_getattr 315
#elif defined(__i386__)
#define SYS_sched_setattr 351
#define SYS_sched_getattr 352
#else
#error "Unknown architecture"
#endif
#endif

// 实现sched_setattr函数
static int sched_setattr(pid_t pid, const struct sched_attr *attr,
                         unsigned int flags);
// 实现sched_getattr函数（可选，用于调试）
static int sched_getattr(pid_t pid, struct sched_attr *attr, unsigned int size,
                         unsigned int flags);

int set_sched_deadline(pid_t pid, uint64_t runtime_ns, uint64_t deadline_ns,
                       uint64_t period_ns);

namespace define {

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t dsmSize = 64; // GB  [CONFIG]
constexpr uint64_t kChunkSize = 32 * MB;

// RDMA buffer
constexpr uint64_t rdmaBufferSize = 2; // GB  [CONFIG]
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
    4 * MB; // tune to an appropriate value (as small as possible without affect
            // the performance)
constexpr uint64_t kOnChipLockNum = kLockChipMemSize * 8; // 1bit-lock

// level of tree
constexpr uint64_t kMaxLevelOfTree = 7;

// To align with MAX_CORO_NUM
constexpr uint16_t kMaxCoro = 8;
// constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

constexpr uint8_t kMaxHandOverTime = 8;

constexpr int kIndexCacheSize = 512; // MB
} // namespace define

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

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

#endif /* __COMMON_H__ */