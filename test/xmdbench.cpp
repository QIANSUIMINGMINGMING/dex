#include "Timer.h"
// #include "Tree.h"
#include <city.h>
#include <numa.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../util/system.hpp"
#include "gaussian_generator.h"
#include "sherman_wrapper.h"
#include "smart/smart_wrapper.h"
#include "tree/leanstore_tree.h"
#include "uniform.h"
#include "uniform_generator.h"
#include "zipf.h"
#include "XMD/BatchForest.h"

// TODO bindCore
namespace sherman {

extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];

}  // namespace sherman
int kMaxThread = 32;
std::thread th[MAX_APP_THREAD];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS]{0};
uint64_t latency_th_all[LATENCY_WINDOWS]{0};
uint64_t tp[MAX_APP_THREAD][8];

// uint64_t total_tp[MAX_APP_THREAD];
uint64_t total_time[MAX_APP_THREAD];

std::mutex mtx;
std::condition_variable cv;
uint32_t kReadRatio;
uint32_t kInsertRatio;  // hybrid read ratio
uint32_t kUpdateRatio;
uint32_t kDeleteRatio;
uint32_t kRangeRatio;
int kThreadCount;
int totalThreadCount;
int memThreadCount;
int kNodeCount;
int CNodeCount;
std::vector<Key> sharding;
uint64_t cache_mb;
// uint64_t kKeySpace = 128 * define::MB; // 268M KVs; 107M
uint64_t kKeySpace;
uint64_t threadKSpace;
uint64_t partition_space;
uint64_t left_bound = 0;
uint64_t right_bound = 0;
uint64_t op_num = 0;  // Total operation num

uint64_t per_node_op_num = 0;
uint64_t per_node_warmup_num = 0;
uint64_t per_thread_op_num = 0;
uint64_t per_thread_warmup_num = 0;

uint64_t *bulk_array = nullptr;
uint64_t bulk_load_num = 0;
uint64_t warmup_num = 0;  // 10M for warmup
int node_id = 0;
double zipfian;
uint64_t *insert_array = nullptr;
uint64_t insert_array_size = 0;
int tree_index = 0;
int check_correctness = 0;
int time_based = 1;
int early_stop = 1;
bool partitioned = false;
double rpc_rate = 0;
double admission_rate = 1;
struct zipf_gen_state state;
// int uniform_workload = 0;
uniform_key_generator_t *uniform_generator = nullptr;
gaussian_key_generator_t *gaussian_generator = nullptr;
enum WorkLoadType {
  uniform,      // 0
  zipf,         // 1
  gaussian_01,  // 2
  gaussian_001  // 3
};
WorkLoadType workload_type;

// std::vector<double> admission_rate_vec = {1,   0.8,  0.6,  0.4,   0.2,
//                                           0.1, 0.05, 0.01, 0.001, 0};
std::vector<double> admission_rate_vec = {1, 0.8, 0.4, 0.2, 0.1, 0.05, 0.01, 0};
// std::vector<double> admission_rate_vec = {0.1};
std::vector<double> rpc_rate_vec = {1};
// std::vector<double> rpc_rate_vec = {0.9, 0.99, 0.999, 1};
//  std::vector<double> total_num = {100, 100, 80, 60, 50, 40, 20, 10, 5};

std::vector<uint64_t> throughput_vec;
std::vector<uint64_t> straggler_throughput_vec;

int auto_tune = 0;
int run_times = 1;
int cur_run = 0;

uint64_t *workload_array = nullptr;
uint64_t *warmup_array = nullptr;
enum op_type : uint8_t { Insert, Update, Lookup, Delete, Range };
uint64_t op_mask = (1ULL << 56) - 1;

tree_api<Key, Value> *tree;
DSM *dsm;

inline Key to_key(uint64_t k) {
  // return k;
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kKeySpace;
}

inline Key to_partition_key(uint64_t k) {
  return (CityHash64((char *)&k, sizeof(k)) + 1) % partition_space;
}

std::atomic<int64_t> warmup_cnt{0};
std::atomic<uint64_t> worker{0};
std::atomic<uint64_t> execute_op{0};
std::atomic_bool ready{false};
std::atomic_bool one_finish{false};
std::atomic_bool ready_to_report{false};

void reset_all_params() {
  warmup_cnt.store(0);
  worker.store(0);
  ready.store(false);
  one_finish.store(false);
  ready_to_report.store(false);
}

void generate_index() {
  numa_set_preferred(1);
  switch (tree_index) {
    case 0:  // DEX
    {
      // First set partition info
      int cluster_num = CNodeCount;
      sharding.push_back(std::numeric_limits<Key>::min());
      for (int i = 0; i < cluster_num - 1; ++i) {
        sharding.push_back((threadKSpace * kMaxThread) + sharding[i]);
        std::cout << "CNode " << i << ", left bound = " << sharding[i]
                  << ", right bound = " << sharding[i + 1] << std::endl;
      }
      sharding.push_back(std::numeric_limits<Key>::max());
      std::cout << "CNode " << cluster_num - 1
                << ", left bound = " << sharding[cluster_num - 1]
                << ", right bound = " << sharding[cluster_num] << std::endl;
      assert(sharding.size() == cluster_num + 1);
      tree = new cachepush::BTree<Key, Value>(
          dsm, 0, cache_mb, rpc_rate, admission_rate, sharding, cluster_num);
      partitioned = true;
    } break;

    case 1:  // Sherman
    {
      tree = new sherman_wrapper<Key, Value>(dsm, 0, cache_mb);
      // First insert one million ops to it to make sure the multi-thread
      // bulkloading can succeeds; otherwise, sherman has concurrency
      // bulkloading bug
      if (dsm->getMyNodeID() == 0) {
        for (uint64_t i = 1; i < 1024000; ++i) {
          tree->insert(to_key(i), i * 2);
        }
      }
    } break;

    case 2:  // SMART
    {
      tree = new smart_wrapper<Key, Value>(dsm, 0, cache_mb);
    } break;

    case 3:  // XMD
    {
      tree = new BatchForest<Key, Value>(dsm);
      break;
    }
  }
  numa_set_localalloc();
}

void bulk_load() {
  // Only one compute node is allowed to do the bulkloading
  tree->bulk_load(bulk_array, bulk_load_num);
  if (partitioned && dsm->getMyNodeID() == 0) {
    assert(sharding.size() == (CNodeCount + 1));
    std::vector<Key> bound;
    for (int i = 0; i < CNodeCount - 1; ++i) {
      bound.push_back(sharding[i + 1]);
    }
    tree->set_shared(bound);
    tree->get_basic();
  }
  tree->set_bound(left_bound, right_bound);
  std::cout << "Left bound = " << left_bound
            << ", right bound = " << right_bound << std::endl;
  delete[] bulk_array;
  printf("node %d finish its bulkload\n", dsm->getMyNodeID());
}

void init_key_generator() {
  if (workload_type == WorkLoadType::uniform) {
    uniform_generator = new uniform_key_generator_t(kKeySpace);
  } else if (workload_type == WorkLoadType::zipf) {
    mehcached_zipf_init(&state, kKeySpace, zipfian,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ node_id);
  } else if (workload_type == WorkLoadType::gaussian_01) {
    gaussian_generator =
        new gaussian_key_generator_t(0.4 * kKeySpace, 0.1 * kKeySpace);
  } else if (workload_type == WorkLoadType::gaussian_001) {
    gaussian_generator =
        new gaussian_key_generator_t(0.4 * kKeySpace, 0.04 * kKeySpace);
  } else {
    assert(false);
  }
}

static int key_id = 0;

uint64_t generate_key() {
  // static int counter = 0;
  uint64_t key = 0;
  while (true) {
    if (workload_type == WorkLoadType::uniform) {
      uint64_t dis = uniform_generator->next_id();
      key = to_key(dis);
    } else if (workload_type == WorkLoadType::zipf) {
      uint64_t dis = mehcached_zipf_next(&state);
      key = to_key(dis);
    } else if (workload_type == WorkLoadType::gaussian_01 ||
               workload_type == WorkLoadType::gaussian_001) {
      key = gaussian_generator->next_id();
    } else {
      assert(false);
    }
    if (key >= 0 && key < kKeySpace) {
      if (key_id < 10) {
        std::cout << key << std::endl;
      }
      key_id++;
      break;
    }
  }
  return key;
}

uint64_t generate_key_range() {
  // static int counter = 0;
  uint64_t key = 0;
  while (true) {
    if (workload_type == WorkLoadType::uniform) {
      uint64_t dis = uniform_generator->next_id();
      key = to_key(dis);
    } else if (workload_type == WorkLoadType::zipf) {
      uint64_t dis = mehcached_zipf_next(&state);
      key = to_key(dis);
    } else if (workload_type == WorkLoadType::gaussian_01 ||
               workload_type == WorkLoadType::gaussian_001) {
      key = gaussian_generator->next_id();
    } else {
      assert(false);
    }
    if (key >= left_bound && key < right_bound) {
      break;
    }
  }
  return key;
}

void generate_workload() {
  // Generate workload for bulk_loading
  uint64_t *space_array = new uint64_t[kKeySpace];
  for (uint64_t i = 0; i < kKeySpace; ++i) {
    space_array[i] = i;
  }
  bulk_array = new uint64_t[bulk_load_num];
  uint64_t thread_warmup_insert_num =
      (kInsertRatio / 100.0) * (op_num / totalThreadCount);
  uint64_t warmup_insert_key_num = thread_warmup_insert_num * kThreadCount;

  uint64_t thread_workload_insert_num =
      (kInsertRatio / 100.0) * (op_num / totalThreadCount);
  uint64_t workload_insert_key_num = thread_warmup_insert_num * kThreadCount;
  uint64_t *insert_array = nullptr;

  if (partitioned) {
    left_bound = sharding[node_id];
    right_bound = sharding[node_id + 1];
    auto cluster_num = CNodeCount;
    if (node_id == (cluster_num - 1)) {
      right_bound = kKeySpace;
    }
    partition_space = right_bound - left_bound;

    // std::cout << "Node ID = " << node_id << std::endl;
    // std::cout << "Left bound = " << left_bound << std::endl;
    // std::cout << "Right bound = " << right_bound << std::endl;
    uint64_t accumulated_bulk_num = 0;
    for (int i = 0; i < cluster_num; i++) {
      uint64_t left_b = sharding[i];
      uint64_t right_b = (i == (cluster_num - 1)) ? kKeySpace : sharding[i + 1];
      std::mt19937 gen(0xc70f6907UL);
      std::shuffle(&space_array[left_b], &space_array[right_b - 1], gen);
      uint64_t bulk_num_per_node =
          static_cast<uint64_t>(static_cast<double>(right_b - left_b + 1) /
                                kKeySpace * bulk_load_num);
      if (i == cluster_num - 1) {
        bulk_num_per_node = bulk_load_num - accumulated_bulk_num;
        // std::cout << "bulk_num_per_node = " << bulk_num_per_node <<
        // std::endl; std::cout << "right_b - left_b = " << right_b - left_b <<
        // std::endl; std::cout << "right_b = " << right_b << std::endl;
        // std::cout << "left_b = " << left_b << std::endl;
        assert(bulk_num_per_node <= (right_b - left_b + 1));
      } else {
        bulk_num_per_node =
            std::min<uint64_t>(bulk_num_per_node, right_b - left_b + 1);
      }
      std::cout << "Bulkload num in node " << i << " = " << bulk_num_per_node
                << std::endl;
      memcpy(&bulk_array[accumulated_bulk_num], &space_array[left_b],
             sizeof(uint64_t) * bulk_num_per_node);
      accumulated_bulk_num += bulk_num_per_node;
      if (left_b == left_bound) {
        insert_array = space_array + left_b + bulk_num_per_node;
        assert((left_b + bulk_num_per_node + warmup_insert_key_num +
                workload_insert_key_num) <= right_b);
      }
    }
    assert(accumulated_bulk_num == bulk_load_num);
  } else {
    partition_space = kKeySpace;
    left_bound = 0;
    right_bound = kKeySpace;
    std::mt19937 gen(0xc70f6907UL);
    std::shuffle(&space_array[0], &space_array[kKeySpace - 1], gen);
    memcpy(&bulk_array[0], &space_array[0], sizeof(uint64_t) * bulk_load_num);

    uint64_t regular_node_insert_num =
        static_cast<uint64_t>(thread_warmup_insert_num * kMaxThread) +
        static_cast<uint64_t>(workload_insert_key_num * kMaxThread);
    insert_array =
        space_array + bulk_load_num + regular_node_insert_num * node_id;
    assert((bulk_load_num + regular_node_insert_num * node_id +
            warmup_insert_key_num + workload_insert_key_num) <= kKeySpace);
  }
  std::cout << "First key of bulkloading = " << bulk_array[0] << std::endl;
  std::cout << "Last key of bulkloading = " << bulk_array[bulk_load_num - 1]
            << std::endl;

  init_key_generator();

  // srand((unsigned)time(NULL));
  // UniformRandom rng(rand());
  UniformRandom rng(rdtsc() ^ node_id);
  uint32_t random_num;
  auto insertmark = kReadRatio + kInsertRatio;
  auto updatemark = insertmark + kUpdateRatio;
  auto deletemark = updatemark + kDeleteRatio;
  auto rangemark = deletemark + kRangeRatio;
  assert(rangemark == 100);

  // auto updatemark = insertmark + kUpdateRatio;
  std::cout << "node warmup insert num = " << warmup_insert_key_num
            << std::endl;
  warmup_array = new uint64_t[warmup_num];
  std::cout << "kReadRatio =" << kReadRatio << std::endl;
  std::cout << "insertmark =" << insertmark << std::endl;
  std::cout << "updatemark =" << updatemark << std::endl;
  std::cout << "deletemark =" << deletemark << std::endl;
  std::cout << "rangemark =" << rangemark << std::endl;

  uint64_t i = 0;
  uint64_t insert_counter = 0;
  per_node_warmup_num = 0;

  if (kInsertRatio == 100) {
    // "Load workload" => need to guarantee all keys are new key
    assert(workload_type == WorkLoadType::uniform);
    while (i < warmup_insert_key_num) {
      uint64_t key = (insert_array[insert_counter] |
                      (static_cast<uint64_t>(op_type::Insert) << 56));
      warmup_array[i] = key;
      ++insert_counter;
      ++i;
    }
    per_node_warmup_num = insert_counter;
    assert(insert_counter <= warmup_insert_key_num);
  } else {
    // DEX
    if (partitioned) {
      std::cout << "left bound: " << left_bound << std::endl;
      std::cout << "right bound: " << right_bound << std::endl;
      while (i < warmup_num) {
        i++;
        random_num = rng.next_uint32() % 100;
        uint64_t key = generate_key();
        if (key < left_bound || key >= right_bound) {
          continue;
        }
        if (key)
          if (random_num < kReadRatio) {
            key = key | (static_cast<uint64_t>(op_type::Lookup) << 56);
          } else if (random_num < insertmark) {
            key = key | (static_cast<uint64_t>(op_type::Insert) << 56);
          } else if (random_num < updatemark) {
            key = key | (static_cast<uint64_t>(op_type::Update) << 56);
          } else if (random_num < deletemark) {
            key = key | (static_cast<uint64_t>(op_type::Delete) << 56);
          } else {
            key = key | (static_cast<uint64_t>(op_type::Range) << 56);
          }
        warmup_array[per_node_warmup_num] = key;
        per_node_warmup_num++;
      }
    } else {
      per_node_warmup_num = (warmup_num / totalThreadCount) * kThreadCount;
      while (i < per_node_warmup_num) {
        random_num = rng.next_uint32() % 100;
        uint64_t key = generate_key_range();
        if (key)
          if (random_num < kReadRatio) {
            key = key | (static_cast<uint64_t>(op_type::Lookup) << 56);
          } else if (random_num < insertmark) {
            key = key | (static_cast<uint64_t>(op_type::Insert) << 56);
          } else if (random_num < updatemark) {
            key = key | (static_cast<uint64_t>(op_type::Update) << 56);
          } else if (random_num < deletemark) {
            key = key | (static_cast<uint64_t>(op_type::Delete) << 56);
          } else {
            key = key | (static_cast<uint64_t>(op_type::Range) << 56);
          }
        warmup_array[i] = key;
        ++i;
      }
    }
  }
  std::cout << "node warmup num: " << per_node_warmup_num << std::endl;

  std::mt19937 gen(0xc70f6907UL);
  if (per_node_warmup_num > 0) {
    std::shuffle(&warmup_array[0], &warmup_array[per_node_warmup_num - 1], gen);
  }
  std::cout << "Finish warmup workload generation" << std::endl;

  workload_array = new uint64_t[op_num];
  i = 0;
  insert_array = insert_array + insert_counter;
  insert_counter = 0;
  std::unordered_map<uint64_t, uint64_t> key_count;

  per_node_op_num = 0;

  if (kInsertRatio == 100) {
    assert(workload_type == WorkLoadType::uniform);
    while (i < workload_insert_key_num) {
      uint64_t key = (insert_array[insert_counter] |
                      (static_cast<uint64_t>(op_type::Insert) << 56));
      workload_array[i] = key;
      ++insert_counter;
      ++i;
    }
    per_node_op_num = insert_counter;
    assert(insert_counter <= workload_insert_key_num);
  } else {
    // DEX
    if (partitioned) {
      while (i < op_num) {
        i++;
        random_num = rng.next_uint32() % 100;
        uint64_t key = generate_key();
        if (key < left_bound || key >= right_bound) {
          continue;
        }
        if (key)
          if (random_num < kReadRatio) {
            key = key | (static_cast<uint64_t>(op_type::Lookup) << 56);
          } else if (random_num < insertmark) {
            key = key | (static_cast<uint64_t>(op_type::Insert) << 56);
          } else if (random_num < updatemark) {
            key = key | (static_cast<uint64_t>(op_type::Update) << 56);
          } else if (random_num < deletemark) {
            key = key | (static_cast<uint64_t>(op_type::Delete) << 56);
          } else {
            key = key | (static_cast<uint64_t>(op_type::Range) << 56);
          }
        workload_array[per_node_op_num] = key;
        per_node_op_num++;
      }
    } else {
      per_node_op_num = (op_num / totalThreadCount) * kThreadCount;
      while (i < per_node_op_num) {
        random_num = rng.next_uint32() % 100;
        uint64_t key = generate_key_range();
        if (key)
          if (random_num < kReadRatio) {
            key = key | (static_cast<uint64_t>(op_type::Lookup) << 56);
          } else if (random_num < insertmark) {
            key = key | (static_cast<uint64_t>(op_type::Insert) << 56);
          } else if (random_num < updatemark) {
            key = key | (static_cast<uint64_t>(op_type::Update) << 56);
          } else if (random_num < deletemark) {
            key = key | (static_cast<uint64_t>(op_type::Delete) << 56);
          } else {
            key = key | (static_cast<uint64_t>(op_type::Range) << 56);
          }
        workload_array[i] = key;
        ++i;
      }
    }
  }
  std::cout << "node op num: " << per_node_op_num << std::endl;
  // std::shuffle(&workload_array[0], &workload_array[node_op_num - 1], gen);
  per_thread_op_num = per_node_op_num / kThreadCount;
  per_thread_warmup_num = per_node_warmup_num / kThreadCount;
  std::cout << "thread op num: " << per_thread_op_num;
  std::cout << "and thread warm num: " << per_thread_warmup_num << std::endl;

  // std::vector<std::pair<uint64_t, uint64_t>> keyValuePairs;
  // for (const auto &entry : key_count) {
  //   keyValuePairs.push_back(entry);
  // }
  // std::sort(keyValuePairs.begin(), keyValuePairs.end(),
  //           [](const auto &a, const auto &b) { return a.second > b.second;
  //           });
  // for (int i = 0; i < 20; ++i) {
  //   std::cout << i << " key: " << keyValuePairs[i].first
  //             << " counter: " << keyValuePairs[i].second << std::endl;
  // }
  delete[] space_array;
  std::cout << "Finish all workload generation" << std::endl;
}

void thread_run(int id) {
  // Interleave the thread binding
  bindCore(id);
  // numa_set_localalloc();
  //  std::cout << "Before register the thread" << std::endl;
  dsm->registerThread();
  tp[id][0] = 0;
  // total_tp[id] = 0;
  total_time[id] = 0;
  uint64_t my_id = kMaxThread * node_id + id;
  worker.fetch_add(1);
  printf("I am %lu\n", my_id);

  // auto idx = cur_run % rpc_rate_vec.size();
  // cachepush::decision.clear();
  // cachepush::decision.set_total_num(total_num[idx]);
  // Every thread set its own warmup/workload range
  uint64_t *thread_workload_array = workload_array + id * per_thread_op_num;
  uint64_t *thread_warmup_array = warmup_array + id * per_thread_warmup_num;
  // uint64_t *thread_workload_array = new uint64_t[thread_op_num];
  // uint64_t *thread_warmup_array = new uint64_t[thread_warmup_num];
  // memcpy(thread_workload_array, thread_workload_array_in_global,
  //        sizeof(uint64_t) * thread_op_num);
  // memcpy(thread_warmup_array, thread_warmup_array_in_global,
  //        sizeof(uint64_t) * thread_warmup_num);
  size_t counter = 0;
  size_t success_counter = 0;
  uint32_t scan_num = 100;
  std::pair<Key, Value> *result = new std::pair<Key, Value>[scan_num];

  while (counter < per_thread_warmup_num) {
    uint64_t key = thread_warmup_array[counter];
    op_type cur_op = static_cast<op_type>(key >> 56);
    key = key & op_mask;
    switch (cur_op) {
      case op_type::Lookup: {
        Value v = key;
        auto flag = tree->lookup(key, v);
        if (flag) ++success_counter;
      } break;

      case op_type::Insert: {
        Value v = key + 1;
        auto flag = tree->insert(key, v);
        if (flag) ++success_counter;
      } break;

      case op_type::Update: {
        Value v = key;
        auto flag = tree->update(key, v);
        if (flag) ++success_counter;
      } break;

      case op_type::Delete: {
        auto flag = tree->remove(key);
        if (flag) ++success_counter;
      } break;

      case op_type::Range: {
        auto flag = tree->range_scan(key, scan_num, result);
        if (flag) ++success_counter;
      } break;

      default:
        std::cout << "OP Type NOT MATCH!" << std::endl;
    }
    ++counter;
  }
  //}

  warmup_cnt.fetch_add(1);
  if (id == 0) {
    std::cout << "Thread_op_num = " << per_thread_op_num << std::endl;
    while (warmup_cnt.load() != kThreadCount);
    // delete[] warmup_array;
    // if (cur_run == (run_times - 1)) {
    //   delete[] warmup_array;
    //   delete[] workload_array;
    // }
    printf("node %d finish warmup\n", dsm->getMyNodeID());
    if (auto_tune) {
      auto idx = cur_run % rpc_rate_vec.size();
      assert(idx >= 0 && idx < rpc_rate_vec.size());
      tree->set_rpc_ratio(rpc_rate_vec[idx]);
      std::cout << "RPC ratio = " << rpc_rate_vec[idx] << std::endl;
    } else {
      tree->set_rpc_ratio(rpc_rate);
    }
    dsm->clear_rdma_statistic();
    tree->clear_statistic();
    dsm->barrier(std::string("warm_finish") + std::to_string(cur_run),
                 CNodeCount);
    ready.store(true);
    warmup_cnt.store(0);
  }

  // Sync to the main thread
  while (!ready_to_report.load());

  // std::cout << "My thread ID = " << dsm->getMyThreadID() << std::endl;
  // std::cout << "Thread op num = " << thread_op_num << std::endl;

  // Start the real execution of the workload
  counter = 0;
  success_counter = 0;
  auto start = std::chrono::high_resolution_clock::now();
  Timer thread_timer;
  while (counter < per_thread_op_num) {
    uint64_t key = thread_workload_array[counter];
    op_type cur_op = static_cast<op_type>(key >> 56);
    key = key & op_mask;
    thread_timer.begin();
    switch (cur_op) {
      case op_type::Lookup: {
        Value v = key;
        auto flag = tree->lookup(key, v);
        if (flag) ++success_counter;
      } break;

      case op_type::Insert: {
        Value v = key + 1;
        auto flag = tree->insert(key, v);
        if (flag) ++success_counter;
      } break;

      case op_type::Update: {
        Value v = key;
        auto flag = tree->update(key, v);
        if (flag) ++success_counter;
      } break;

      case op_type::Delete: {
        auto flag = tree->remove(key);
        if (flag) ++success_counter;
      } break;

      case op_type::Range: {
        auto flag = tree->range_scan(key, scan_num, result);
        if (flag) ++success_counter;
      } break;

      default:
        std::cout << "OP Type NOT MATCH!" << std::endl;
    }
    auto us_10 = thread_timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[id][us_10]++;
    tp[id][0]++;
    ++counter;
    // if (counter % 1000000 == 0) {
    //   std::cout << "Thread ID = " << id << "--------------------------------"
    //             << std::endl;
    //   cachepush::decision.show_statistic();
    //   std::cout << "-------------------------------------" << std::endl;
    // }
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();

  total_time[id] = static_cast<uint64_t>(duration);
  // The one who first finish should terminate all threads
  // To avoid the straggling thread
  if (early_stop && !one_finish.load()) {
    one_finish.store(true);
    per_thread_op_num = 0;
  }

  worker.fetch_sub(1);

  // uint64_t throughput =
  //     counter / (static_cast<double>(duration) / std::pow(10, 6));
  // total_tp[id] = throughput;  // (ops/s)
  // total_time[id] = static_cast<uint64_t>(duration);
  // std::cout << "Success ratio = "
  //           << success_counter / static_cast<double>(counter) << std::endl;
  execute_op.fetch_add(counter);
  // if (cachepush::total_sample_times != 0) {
  //   std::cout << "Node search time(ns) = "
  //             << cachepush::total_nanoseconds / cachepush::total_sample_times
  //             << std::endl;
  //   std::cout << "Sample times = " << cachepush::total_sample_times
  //             << std::endl;
  // }
  // std::cout << "Real rpc ratio = " << tree->get_rpc_ratio() << std::endl;
  uint64_t first_not_found_key = 0;
  bool not_found = false;
#ifdef CHECK_CORRECTNESS
  if (check_correctness && id == 0) {
    while (worker.load() != 0);
    uint64_t success_counter = 0;
    uint64_t num_not_found = 0;
    for (uint64_t i = left_bound; i < right_bound; i++) {
      Key k = i;
      Value v = i;
      //  std::cout << "Check k " << k << std::endl;
      auto flag = tree->lookup(k, v);
      if (flag && (v == (k + 1) || (v == k)))
        ++success_counter;
      else {
        // std::cout << "KEY " << k << std::endl;
        //  exit(0);
        if (!not_found) {
          first_not_found_key = i;
          not_found = true;
        }
        ++num_not_found;
      }
    }
    std::cout << "Validation CHECK: Success counter = " << success_counter
              << std::endl;
    std::cout << "Validation CHECK: Not found counter = " << num_not_found
              << std::endl;
    std::cout << "First not found key = " << first_not_found_key << std::endl;
    std::cout << "Validation CHECK: Success_counter/kKeySpace = "
              << success_counter / static_cast<double>(kKeySpace) << std::endl;
  }
#endif
}

void parse_args(int argc, char *argv[]) {
  if (argc != 23) {
    printf("argc = %d\n", argc);
    printf(
        "Usage: ./benchmark kNodeCount kReadRatio kInsertRatio kUpdateRatio "
        "kDeleteRatio kRangeRatio "
        "totalThreadCount memThreadCount "
        "cacheSize(MB) uniform_workload zipfian_theta bulk_load_num "
        "warmup_num op_num "
        "check_correctness(0=no, 1=yes) time_based(0=no, "
        "1=yes) early_stop(0=no, 1=yes) "
        "index(0=cachepush, 1=sherman) rpc_rate admission_rate "
        "auto_tune(0=false, 1=true) kMaxThread"
        " \n");
    exit(-1);
  }

  kNodeCount = atoi(argv[1]);
  kReadRatio = atoi(argv[2]);
  kInsertRatio = atoi(argv[3]);
  kUpdateRatio = atoi(argv[4]);
  kDeleteRatio = atoi(argv[5]);
  kRangeRatio = atoi(argv[6]);
  assert((kReadRatio + kInsertRatio + kUpdateRatio + kDeleteRatio +
          kRangeRatio) == 100);

  totalThreadCount = atoi(argv[7]);  // Here is total thread count
  memThreadCount = atoi(argv[8]);    // #threads in memory node

  cache_mb = atoi(argv[9]);
  workload_type = static_cast<WorkLoadType>(atoi(argv[10]));
  zipfian = atof(argv[11]);
  bulk_load_num = atoi(argv[12]) * 1000 * 1000;
  warmup_num = atoi(argv[13]) * 1000 * 1000;
  op_num = atoi(argv[14]) * 1000 * 1000;  // Here is total op_num => need to be
                                          // distributed across the bechmark
  check_correctness = atoi(argv[15]);     // Whether we need to validate the
                                       // corretness of the tree after running
  time_based = atoi(argv[16]);
  early_stop = atoi(argv[17]);

  tree_index = atoi(argv[18]);
  rpc_rate = atof(argv[19]);        // RPC rate for DEX
  admission_rate = atof(argv[20]);  // Admission control ratio for DEX
  auto_tune = atoi(argv[21]);  // Whether needs the parameter tuning phase: run
                               // multiple times for a single operation

  kMaxThread = atoi(argv[22]);
  // How to make insert ready?
  kKeySpace = bulk_load_num +
              ceil((op_num + warmup_num) * (kInsertRatio / 100.0)) + 1000;

  // Get thread_key_space
  threadKSpace = kKeySpace / totalThreadCount;

  CNodeCount = (totalThreadCount % kMaxThread == 0)
                   ? (totalThreadCount / kMaxThread)
                   : (totalThreadCount / kMaxThread + 1);
  std::cout << "Compute node count = " << CNodeCount << std::endl;
  printf(
      "kNodeCount %d, kReadRatio %d, kInsertRatio %d, kUpdateRatio %d, "
      "kDeleteRatio %d, kRangeRatio %d, "
      "totalThreadCount %d, memThreadCount %d "
      "cache_size %lu, workload_type %u, zipfian %lf, bulk_load_num %lu, "
      "warmup_num %lu, "
      "op_num "
      "%lu, check_correctness %d, time_based %d, early_stop %d, index %d, "
      "rpc_rate "
      "%lf, "
      "admission_rate %lf, auto_tune %d\n",
      kNodeCount, kReadRatio, kInsertRatio, kUpdateRatio, kDeleteRatio,
      kRangeRatio, totalThreadCount, memThreadCount, cache_mb, workload_type,
      zipfian, bulk_load_num, warmup_num, op_num, check_correctness, time_based,
      early_stop, tree_index, rpc_rate, admission_rate, auto_tune);
  std::cout << "kMaxThread = " << kMaxThread << std::endl;
  std::cout << "KeySpace = " << kKeySpace << std::endl;
}

int main(int argc, char *argv[]) {
  numa_set_preferred(1);
  parse_args(argc, argv);

  DSMConfig config;
  config.machineNR = kNodeCount;
  config.memThreadCount = memThreadCount;
  config.computeNR = CNodeCount;
  config.index_type = tree_index;
  dsm = DSM::getInstance(config);
  cachepush::global_dsm_ = dsm;
  // #Worker-threads in this CNode
  node_id = dsm->getMyNodeID();
  if (node_id == (CNodeCount - 1)) {
    kThreadCount = totalThreadCount - ((CNodeCount - 1) * kMaxThread);
  } else {
    kThreadCount = kMaxThread;
  }

  double collect_throughput = 0;
  uint64_t total_throughput = 0;
  double total_max_time = 0;
  double total_cluster_max_time = 0;
  uint64_t total_cluster_tp = 0;
  uint64_t straggler_cluster_tp = 0;
  uint64_t collect_times = 0;

  if (node_id < CNodeCount) {
    dsm->registerThread();
    generate_index();

    dsm->barrier("bulkload", CNodeCount);
    dsm->resetThread();
    generate_workload();
    bulk_load();

    while(true) {
      
    }

    if (auto_tune) {
      run_times = admission_rate_vec.size() * rpc_rate_vec.size();
    }

    while (cur_run < run_times) {
      // Reset benchmark parameters
      collect_throughput = 0;
      total_throughput = 0;
      total_max_time = 0;
      total_cluster_tp = 0;
      total_cluster_max_time = 0;
      straggler_cluster_tp = 0;
      collect_times = 0;

      dsm->resetThread();
      dsm->registerThread();
      tree->reset_buffer_pool(true);
      dsm->barrier(std::string("benchmark") + std::to_string(cur_run),
                   CNodeCount);
      tree->get_newest_root();
      // In warmup phase, we do not use RPC
      tree->set_rpc_ratio(0);
      if (auto_tune) {
        auto idx = cur_run / rpc_rate_vec.size();
        assert(idx >= 0 && idx < admission_rate_vec.size());
        tree->set_admission_ratio(admission_rate_vec[idx]);
        std::cout << "Admission rate = " << admission_rate_vec[idx]
                  << std::endl;
      }

      // Reset all parameters in thread_run
      dsm->resetThread();
      reset_all_params();
      std::cout << node_id << " is ready for the benchmark" << std::endl;

      for (int i = 0; i < kThreadCount; i++) {
        th[i] = std::thread(thread_run, i);
      }

      // Warmup
      auto start = std::chrono::high_resolution_clock::now();
      while (!ready.load()) {
        sleep(2);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::seconds>(end - start)
                .count();
        if (time_based && duration >= 30) {
          per_thread_warmup_num = 0;
        }
      }

      // Main thread is used to collect the statistics
      timespec s, e;
      uint64_t pre_tp = 0;
      uint64_t pre_ths[MAX_APP_THREAD];
      for (int i = 0; i < MAX_APP_THREAD; ++i) {
        pre_ths[i] = 0;
      }

      ready_to_report.store(true);
      clock_gettime(CLOCK_REALTIME, &s);
      bool start_generate_throughput = false;

      std::cout << "Start collecting the statistic" << std::endl;
      start = std::chrono::high_resolution_clock::now();
      // System::profile("dex-test", [&]() {
      int iter = 0;
      while (true) {
        sleep(2);
        clock_gettime(CLOCK_REALTIME, &e);
        int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                           (double)(e.tv_nsec - s.tv_nsec) / 1000;

        uint64_t all_tp = 0;
        for (int i = 0; i < kThreadCount; ++i) {
          all_tp += tp[i][0];
        }

        // Throughput in current phase (for very two seconds)
        uint64_t cap = all_tp - pre_tp;
        pre_tp = all_tp;

        for (int i = 0; i < kThreadCount; ++i) {
          auto val = tp[i][0];
          pre_ths[i] = val;
        }

        uint64_t all = 0;
        uint64_t hit = 0;
        for (int i = 0; i < MAX_APP_THREAD; ++i) {
          all += (sherman::cache_hit[i][0] + sherman::cache_miss[i][0]);
          hit += sherman::cache_hit[i][0];
        }

        clock_gettime(CLOCK_REALTIME, &s);
        double per_node_tp = cap * 1.0 / microseconds;

        // FIXME(BT): use static counter for increment, need fix
        // uint64_t cluster_tp =
        //     dsm->sum((uint64_t)(per_node_tp * 1000), CNodeCount);
        uint64_t cluster_tp =
            dsm->sum_with_prefix(std::string("sum-") + std::to_string(cur_run) +
                                     std::string("-") + std::to_string(iter),
                                 (uint64_t)(per_node_tp * 1000), CNodeCount);

        // uint64_t cluster_tp = 0;
        printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);
        // save_latency(iter);

        if (dsm->getMyNodeID() == 0) {
          printf("cluster throughput %.3f\n", cluster_tp / 1000.0);

          if (cluster_tp != 0) {
            start_generate_throughput = true;
          }

          // Means this Cnode already finish the workload
          if (start_generate_throughput && cluster_tp == 0) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::seconds>(end - start)
                    .count();
            std::cout << "The time duration = " << duration << " seconds"
                      << std::endl;
            break;
          }

          if (start_generate_throughput) {
            ++collect_times;
            collect_throughput += cluster_tp / 1000.0;
            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::seconds>(end - start)
                    .count();
            if (time_based && duration > 60) {
              std::cout << "Running time is larger than " << 60 << "seconds"
                        << std::endl;
              per_thread_op_num = 0;
              break;
            }
          }

          if (tree_index == 1) {
            printf("cache hit rate: %lf\n", hit * 1.0 / all);
          } else if (tree_index == 2) {
            tree->get_statistic();
          }
        } else {
          if (cluster_tp != 0) {
            start_generate_throughput = true;
          }

          if (start_generate_throughput && per_node_tp == 0) break;

          if (start_generate_throughput) {
            ++collect_times;
            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::seconds>(end - start)
                    .count();
            if (time_based && duration > 60) {
              per_thread_op_num = 0;
              break;
            }
          }

          if (tree_index == 1) {
            printf("cache hit rate: %lf\n", hit * 1.0 / all);
          } else if (tree_index == 2) {
            tree->get_statistic();
          }
        }
        ++iter;
      }  // while(true) loop
         //});

      sleep(2);
      while (worker.load() != 0) {
        sleep(2);
      }

      for (int i = 0; i < kThreadCount; i++) {
        th[i].join();
      }

      // for (int i = 0; i < kThreadCount; ++i) {
      //   total_max_time = std::max_element(total_time, total_time + k)
      // }
      total_max_time = *std::max_element(total_time, total_time + kThreadCount);
      total_cluster_max_time = dsm->max_total(total_max_time, CNodeCount);
      std::cout << "XMD node max time: " << total_max_time;
      std::cout << "XMD cluster max time: " << total_cluster_max_time;

      uint64_t XMDsetting_node_throughput =
          execute_op.load() /
          (static_cast<double>(total_cluster_max_time) / std::pow(10, 6));
      uint64_t XMDsetting_cluster_throughput =
          dsm->sum_total(XMDsetting_node_throughput, CNodeCount, false);
      

      std::cout << "XMD node throughput: " << (static_cast<double>(XMDsetting_node_throughput)/std::pow(10,6)) << " MOPS" << std::endl;
      std::cout << "XMD cluster throughput: " << (static_cast<double>(XMDsetting_cluster_throughput)/std::pow(10,6)) <<" MOPS"<<std::endl;

      // uint64_t max_time = 0;
      // for (int i = 0; i < kThreadCount; ++i) {
      //   max_time = std::max<uint64_t>(max_time, total_time[i]);
      // }

      total_cluster_tp = dsm->sum_total(total_throughput, CNodeCount, false);
      straggler_cluster_tp =
          dsm->min_total(total_throughput / kThreadCount, CNodeCount);
      straggler_cluster_tp = straggler_cluster_tp * totalThreadCount;
      // op_num /
      // (static_cast<double>(straggler_cluster_tp) / std::pow(10, 6));
#ifdef CHECK_CORRECTNESS
      if (check_correctness) {
        // std::cout << "------#RDMA read = " << dsm->num_read_rdma <<
        // std::endl; std::cout << "------#RDMA write = " << dsm->num_write_rdma
        // << std::endl; std::cout << "------#RDMA cas = " << dsm->num_cas_rdma
        // << std::endl;
        dsm->resetThread();
        dsm->registerThread();
        tree->validate();
      }
#endif
      throughput_vec.push_back(total_cluster_tp);
      straggler_throughput_vec.push_back(straggler_cluster_tp);
      std::cout << "Round " << cur_run
                << " (max_throughput): " << total_cluster_tp / std::pow(10, 6)
                << " Mops/s" << std::endl;
      std::cout << "Round " << cur_run << " (straggler_throughput): "
                << straggler_cluster_tp / std::pow(10, 6) << " Mops/s"
                << std::endl;
      ++cur_run;
    }  // multiple run loop
  }

  std::cout << "Before barrier finish" << std::endl;
  dsm->barrier("finish");
}