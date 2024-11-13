#pragma once
#include <city.h>

#include <cstdint>

#include "Common.h"
#include "uniform.h"
#include "uniform_generator.h"
#include "zipf.h"

namespace DSBench {
constexpr int MAX_TEST_NUM = 1000000000;
constexpr int MAX_WARM_UP_NUM = 1000000000;

constexpr double zipfian = 0.99;
constexpr uint64_t op_mask = (1ULL << 56) - 1;

enum op_type : uint8_t { Insert, Update, Lookup, Delete, Range };
enum workload_type : uint8_t { Zipf, Uniform };

void init_random();

inline Key to_key(uint64_t k);
uint64_t generate_range_key(workload_type type);

void generate_workload(int op_num, int warm_num, int thread_num, int read_ratio,
                       int insert_ratio, int update_ratio, int delete_ratio,
                       int range_ratio);
}  // namespace DSBench
