#include <city.h>

#include <boost/circular_buffer.hpp>


#include "XMD/mc_agent.h"
#include "zipf.h"

// parameters
uint64_t kKeySpace = 64 * define::MB;
double zipfan = 0;
double kReadRatio = 50;

std::unique_ptr<XMD::multicast::multicastCM> mcm;

std::thread th[MAX_APP_THREAD];
bool is_end = false;

constexpr uint64_t psn_numbers = 10000;
// constexpr uint64_t  
// DEFINE_int64(psn_numbers, 100, "The number of psn to be used");
// DEFINE_int64(rate_limit_node_id, 0, "The node id to be used");
// DEFINE_int64(number_per_10_us, 1000,
//              "The number of messages to be sent per 10 us");

std::unique_ptr<XMD::multicast::TransferObjBuffer> tob;

struct Request {
  bool is_search;
  Key k;
  Value v;
};

inline Key to_key(uint64_t k) {
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kKeySpace;
}

void thread_run(int id) {
  bindCore(id + XMD::multicastSendCore);
  unsigned int seed = rdtsc();
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, kKeySpace, zipfan,
                      (rdtsc() & (0x0000ffffffffffffull)) ^ id);
  int tob_pos = 0;
  mcm->print_self();

  while (!is_end) {
    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = to_key(dis);
    Value v;
    if (rand_r(&seed) % 100 < kReadRatio) {  // GET
    } else {
      v = 23;
      TS ts = XMD::myClock::get_ts();
      tob->insert(key, ts, v, tob_pos);
      tob_pos++;
      if (tob_pos == XMD::multicast::kMcCardinality) {
        tob->emit();
        tob_pos = 0;
      }
    }
  }
}

// void rate_limitor() {
//   bindCore(rate_limit_core);
//   while (true) {
//   }
// }

int main(int argc, char **argv) {
  // test the if the package is lost
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  printf("rate limit validate experiments start\n");
  printf("page size %d, cardinality %d\n", kMcPageSize,
         rdmacm::multicast::kMcCardinality);

  mcm = std::make_unique<rdmacm::multicast::multicastCM>();
  tob = std::make_unique<TransferObjBuffer>();

  for (int i = 0; i < kMaxMulticastSendCoreNum; i++) {
    th[i] = std::thread(thread_run, i);
  }

  for (int i = 0; i < kMaxMulticastSendCoreNum; i++) {
    th[i].join();
  }

  uint64_t loss_packages =
      rdmacm::multicast::check_package_loss(FLAGS_psn_numbers);
  double loss_rate = (double)loss_packages / FLAGS_psn_numbers;
  printf("loss rate %f\n", loss_rate);
  printf("test complete\n");
  return 0;
}