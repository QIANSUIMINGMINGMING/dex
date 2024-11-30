
#include "XMD/libfadecuckoo/faded_cuckoohash_map.hh"
#include "XMD/request_cache_v2.h"
// #include "data_structure_bench.h"
#include <city.h>

#include <cstdint>

#include "Common.h"
#include "cache/node_wr.h"
#include "uniform.h"
#include "uniform_generator.h"
#include "zipf.h"

namespace DSBench {
constexpr int MAX_TEST_NUM = 200000000;
constexpr int MAX_WARM_UP_NUM = 200000000;
uint64_t kKeySpace;
constexpr double zipfian = 0.99;

uint64_t op_mask = (1ULL << 56) - 1;
// uint64_t uniform_test_keys[MAX_TEST_NUM + MAX_WARM_UP_NUM]{0};
// uint64_t uniform_test_values[MAX_TEST_NUM + MAX_WARM_UP_NUM]{0};

// uint64_t zipf_test_keys[MAX_TEST_NUM + MAX_WARM_UP_NUM]{0};
// uint64_t zipf_test_values[MAX_TEST_NUM + MAX_WARM_UP_NUM]{0};
uint64_t* uniform_test_keys;
uint64_t* zipf_test_keys;

struct zipf_gen_state state;
uniform_key_generator_t* uniform_generator = nullptr;

enum op_type : uint8_t { Insert, Update, Lookup, Delete, Range };
enum workload_type : uint8_t { Zipf, Uniform };

void init_random() {
  uniform_generator = new uniform_key_generator_t(kKeySpace);
  mehcached_zipf_init(&state, kKeySpace, zipfian,
                      (rdtsc() & (0x0000ffffffffffffull)) ^ 0);
}

inline Key to_key(uint64_t k) {
  return (CityHash64((char*)&k, sizeof(k)) + 1) % kKeySpace;
}

uint64_t generate_range_key(workload_type type) {
  // static int counter = 0;
  uint64_t key = 0;
  while (true) {
    if (type == workload_type::Uniform) {
      uint64_t dis = uniform_generator->next_id();
      key = to_key(dis);
    } else {
      uint64_t dis = mehcached_zipf_next(&state);
      key = to_key(dis);
    }
    if (key >= 0 && key < kKeySpace) {
      break;
    }
  }
  return key;
}

void generate_workload(int op_num, int warm_num, int thread_num, int read_ratio,
                       int insert_ratio, int update_ratio, int delete_ratio,
                       int range_ratio) {
  // Generate workload for bulk_loading
  kKeySpace = warm_num + op_num * insert_ratio * 0.01;

  uint64_t* space_array = new uint64_t[kKeySpace];
  for (uint64_t i = 0; i < kKeySpace; ++i) {
    space_array[i] = i;
  }

  //   for (size_t i = 0; i < warm_num; i++) {
  //     uniform_test_values[i] = i;
  //     zipf_test_values[i] = i;
  //   }

  int per_thread_op_num = op_num / thread_num;
  int per_thread_warm_num = warm_num / thread_num;
  //   std::mt19937 gen(0xc70f6907UL);
  //   std::shuffle(&space_array[0], &space_array[kKeySpace - 1], gen);

  init_random();
  for (size_t i = 0; i < warm_num; i++) {
    uniform_test_keys[i] = space_array[i];
  }

  for (size_t i = 0; i < warm_num; i++) {
    zipf_test_keys[i] = space_array[i];
  }

  srand((unsigned)time(NULL));
  UniformRandom rng(rand() ^ 0);
  uint32_t random_num;
  auto insertmark = read_ratio + insert_ratio;
  auto updatemark = insertmark + update_ratio;
  auto deletemark = updatemark + delete_ratio;
  auto rangemark = deletemark + range_ratio;
  assert(rangemark == 100);

  std::cout << "node warmup num = " << warm_num << std::endl;
  std::cout << "kReadRatio =" << read_ratio << std::endl;
  std::cout << "insertmark =" << insertmark << std::endl;
  std::cout << "updatemark =" << updatemark << std::endl;
  std::cout << "deletemark =" << deletemark << std::endl;
  std::cout << "rangemark =" << rangemark << std::endl;

  int insert_counter = 0;

  for (size_t i = 0; i < op_num; i++) {
    random_num = rng.next_uint32() % 100;
    uint64_t key = generate_range_key(workload_type::Uniform);
    if (random_num < read_ratio) {
      key = key | (static_cast<uint64_t>(op_type::Lookup) << 56);
    } else if (random_num < insertmark) {
      // key = key | (static_cast<uint64_t>(op_type::Insert) << 56);
      // To guarantee insert key are new keys
      if (insert_counter < op_num) {
        key = (space_array[warm_num + insert_counter] |
               (static_cast<uint64_t>(op_type::Insert) << 56));
        ++insert_counter;
      }
    } else if (random_num < updatemark) {
      key = key | (static_cast<uint64_t>(op_type::Update) << 56);
    } else if (random_num < deletemark) {
      key = key | (static_cast<uint64_t>(op_type::Delete) << 56);
    } else {
      key = key | (static_cast<uint64_t>(op_type::Range) << 56);
    }
    uniform_test_keys[i + warm_num] = key;
  }

  for (size_t i = 0; i < op_num; i++) {
    random_num = rng.next_uint32() % 100;
    uint64_t key = generate_range_key(workload_type::Zipf);
    if (random_num < read_ratio) {
      key = key | (static_cast<uint64_t>(op_type::Lookup) << 56);
    } else if (random_num < insertmark) {
      // key = key | (static_cast<uint64_t>(op_type::Insert) << 56);
      // To guarantee insert key are new keys
      if (insert_counter < op_num) {
        key = (space_array[warm_num + insert_counter] |
               (static_cast<uint64_t>(op_type::Insert) << 56));
        ++insert_counter;
      }
    } else if (random_num < updatemark) {
      key = key | (static_cast<uint64_t>(op_type::Update) << 56);
    } else if (random_num < deletemark) {
      key = key | (static_cast<uint64_t>(op_type::Delete) << 56);
    } else {
      key = key | (static_cast<uint64_t>(op_type::Range) << 56);
    }
    zipf_test_keys[i + warm_num] = key;
  }

  std::cout << "node op_num = " << op_num << std::endl;
  delete[] space_array;
  std::cout << "Finish all workload generation" << std::endl;
}
}  // namespace DSBench

constexpr int MAX_THREAD_NUM = MAX_APP_THREAD;
int thread_num = MAX_THREAD_NUM;
uint64_t total_tp[MAX_THREAD_NUM]{0};

std::atomic<int> warm_up_ok{0};
std::atomic<bool> one_finish;
std::atomic<int> execute_op_num{0};
std::atomic<int> workers{0};

libcuckoo::cuckoohash_map<uint64_t, uint64_t> map(100000000);
libfadecuckoo::cuckoohash_map<uint64_t, uint64_t> fadedmap(100000000);

void thread_run(int id, int op_num, int warm_num) {
  bindCore(id);
  int thread_op_num = op_num / thread_num;
  int thread_warm_num = warm_num / thread_num;
  uint64_t* thread_warm_array =
      DSBench::uniform_test_keys + id * thread_warm_num;
  uint64_t* thread_op_array_uniform =
      DSBench::uniform_test_keys + warm_num + id * thread_op_num;
  uint64_t* thread_op_array_zipf =
      DSBench::zipf_test_keys + warm_num + id * thread_op_num;

  uint64_t* thread_op_array;
  if (true) {
    thread_op_array = thread_op_array_uniform;
  } else {
    thread_op_array = thread_op_array_zipf;
  }

  int i = 0;
  while (i < thread_warm_num) {
    map.insert(thread_warm_array[i], thread_warm_array[i] + 1);
    i++;
  }
  i = 0;
  warm_up_ok.fetch_add(1);
  while (warm_up_ok.load() < thread_num);
  auto start = std::chrono::high_resolution_clock::now();
  uint64_t old_tss[100];
  int tss_idx = 0;
  int store_tss_idx = 0;
  // old_tss[tss_idx] = XMD::myClock::get_ts();
  while (i < thread_op_num && one_finish.load() == false) {
    if (i % 5000 == 0 && id == 0) {
      old_tss[tss_idx] = XMD::myClock::get_ts();
      tss_idx = RING_ADD(tss_idx, 1, 100);

      if (i / 60000 > 0) {
        fadedmap.oldest_TS.store(old_tss[store_tss_idx]);
        store_tss_idx = RING_ADD(store_tss_idx, 1, 100);
      }
    }
    uint64_t key = thread_op_array[i];
    DSBench::op_type cur_op = static_cast<DSBench::op_type>(key >> 56);
    key = key & DSBench::op_mask;
    switch (cur_op) {
      case DSBench::op_type::Lookup: {
        Value v;
        map.find(key, v);
        // fadedmap.find(key, v);
      } break;
      case DSBench::op_type::Insert: {
        Value v = key + 1;
        uint64_t ts = XMD::myClock::get_ts();
        map.insert(key, v);
        // fadedmap.insert(key, ts, v);
      } break;

      case DSBench::op_type::Update: {
        Value v = key;
        uint64_t ts = XMD::myClock::get_ts();
        map.update(key, v);
        // fadedmap.update(key, ts, v);
      } break;
    }
    i++;
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();

  // The one who first finish should terminate all threads
  // To avoid the straggling thread
  if (!one_finish.load()) {
    one_finish.store(true);
    thread_op_num = 0;
  }

  workers.fetch_sub(1);

  uint64_t throughput = i / (static_cast<double>(duration) / std::pow(10, 6));
  total_tp[id] = throughput;  // (ops/s)
  // total_time[id] = static_cast<uint64_t>(duration);
  // std::cout << "Success ratio = "
  //           << success_counter / static_cast<double>(counter) << std::endl;
  execute_op_num.fetch_add(i);
}

DSM* dsm;
std::thread th[MAX_THREAD_NUM];
XMD::RequestCache_v3::RequestCache* cache;
constexpr uint64_t defaultCacheSize = 128 * define::MB;

void thread_run_rc(int id, int op_num, int warm_num) {
  bindCore(id);
  dsm->registerThread();
  int thread_op_num = op_num / thread_num;
  int thread_warm_num = warm_num / thread_num;
  uint64_t* thread_warm_array =
      DSBench::uniform_test_keys + id * thread_warm_num;
  uint64_t* thread_op_array_uniform =
      DSBench::uniform_test_keys + warm_num + id * thread_op_num;
  uint64_t* thread_op_array_zipf =
      DSBench::zipf_test_keys + warm_num + id * thread_op_num;

  uint64_t* thread_op_array;
  if (true) {
    thread_op_array = thread_op_array_uniform;
  } else {
    thread_op_array = thread_op_array_zipf;
  }

  int i = 0;
  while (i < thread_warm_num) {
    XMD::KVTS kvts;
    kvts.k = thread_warm_array[i];
    kvts.ts = XMD::myClock::get_ts();
    kvts.v = thread_warm_array[i] + 1;
    cache->insert(kvts);
    i++;
  }
  i = 0;
  warm_up_ok.fetch_add(1);
  while (warm_up_ok.load() < thread_num);
  auto start = std::chrono::high_resolution_clock::now();
  // old_tss[tss_idx] = XMD::myClock::get_ts();
  while (i < thread_op_num && !one_finish.load()) {
    uint64_t key = thread_op_array[i];
    DSBench::op_type cur_op = static_cast<DSBench::op_type>(key >> 56);
    key = key & DSBench::op_mask;
    switch (cur_op) {
      case DSBench::op_type::Lookup: {
        Value v;
        cache->lookup(key, v);
      } break;
      case DSBench::op_type::Insert: {
        XMD::KVTS kvts;
        kvts.k = key;
        kvts.ts = XMD::myClock::get_ts();
        kvts.v = key + 1;
        cache->insert(kvts);
        // fadedmap.insert(key, ts, v);
      } break;

      case DSBench::op_type::Update: {
        XMD::KVTS kvts;
        kvts.k = key;
        kvts.ts = XMD::myClock::get_ts();
        kvts.v = key + 2;
        cache->insert(kvts);
        // fadedmap.update(key, ts, v);
      } break;
    }
    i++;
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();

  // The one who first finish should terminate all threads
  // To avoid the straggling thread
  if (!one_finish.load()) {
    one_finish.store(true);
    thread_op_num = 0;
  }

  workers.fetch_sub(1);

  uint64_t throughput = i / (static_cast<double>(duration) / std::pow(10, 6));
  total_tp[id] = throughput;  // (ops/s)
  // total_time[id] = static_cast<uint64_t>(duration);
  // std::cout << "Success ratio = "
  //           << success_counter / static_cast<double>(counter) << std::endl;
  execute_op_num.fetch_add(i);
}

int main() {
  int CNodeCount = 1;
  DSMConfig config;
  config.machineNR = 2;
  config.memThreadCount = 1;
  config.computeNR = CNodeCount;
  config.index_type = 3;
  dsm = DSM::getInstance(config);
  cachepush::global_dsm_ = dsm;
  // #Worker-threads in this CNode
  uint64_t op_num = 100000;
  uint64_t warm_num = 100000;
  DSBench::kKeySpace = warm_num + op_num;
  DSBench::init_random();
  uint16_t node_id = dsm->getMyNodeID();
  if (node_id < CNodeCount) {
    cache = new XMD::RequestCache_v3::RequestCache(dsm, defaultCacheSize,
                                                   node_id, CNodeCount);
    DSBench::zipf_test_keys = new uint64_t[op_num + warm_num];
    DSBench::uniform_test_keys = new uint64_t[op_num + warm_num];
    std::thread th[MAX_THREAD_NUM];
    DSBench::generate_workload(op_num, warm_num, thread_num, 50, 25, 25, 0, 0);
    std::thread batch_update_th = std::thread(XMD::RequestCache_v3::RequestCache::period_batch, cache);
    std::thread batch_check_th = std::thread(XMD::RequestCache_v3::RequestCache::period_check,cache);

    for (size_t i = 0; i < thread_num; i++) {
      th[i] = std::thread(thread_run_rc, i, op_num, warm_num);
    }

    for (size_t i = 0; i < thread_num; i++) {
      th[i].join();
    }

    uint64_t total_tp_sum = 0;
    for (size_t i = 0; i < thread_num; i++) {
      total_tp_sum += total_tp[i];
    }

    std::cout << "total throughput" << total_tp_sum << std::endl;
  }
  std::cout << "Before barrier finish" << std::endl;
  dsm->barrier("finish");
}

//   int i = 0;
//   dsm->registerThread();

//   auto start = std::chrono::high_resolution_clock::now();
//   while (i < 1000000) {
//     XMD::KVTS kvts;
//     kvts.k = i;
//     kvts.ts = XMD::myClock::get_ts();
//     kvts.v = i + 1;
//     cache->insert(kvts);
//     i++;
//   }
//   auto end = std::chrono::high_resolution_clock::now();
//   auto duration =
//       std::chrono::duration_cast<std::chrono::microseconds>(end - start)
//           .count();
//   std::cout << "Insert time = " << duration << std::endl;
//   // throughput
//   std::cout << "Throughput = "
//             << i / (static_cast<double>(duration) / std::pow(10, 6))
//             << std::endl;
// }

// int main() {
// //   DSBench::init_random();
//   int op_num = 5000000;
//   int warm_num = 5000000;
//   // map = libcuckoo::cuckoohash_map<uint64_t, uint64_t>(100000000);
//   // fadedmap = libfadecuckoo::cuckoohash_map<uint64_t,
//   uint64_t>(100000000);

//   DSBench::zipf_test_keys = new uint64_t[op_num + warm_num];
//   DSBench::uniform_test_keys = new uint64_t[op_num + warm_num];
//   std::thread th[MAX_THREAD_NUM];
//   DSBench::generate_workload(op_num, warm_num, thread_num, 50, 25, 25, 0,
//   0); for (size_t i=0; i< thread_num; i++) {
//     th[i] = std::thread(thread_run, i, op_num, warm_num);
//   }

//   for (size_t i=0; i< thread_num; i++) {
//     th[i].join();
//   }

//   uint64_t total_tp_sum = 0;
//   for (size_t i = 0; i < thread_num; i++) {
//     total_tp_sum += total_tp[i];
//   }

//   std::cout << "total throughput" << total_tp_sum << std::endl;

//   return 0;
// }