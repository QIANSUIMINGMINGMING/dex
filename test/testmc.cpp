#include <city.h>

#include "XMD/mc_agent.h"
#include "zipf.h"

// parameters
uint64_t kKeySpace = 64 * define::MB;
double zipfan = 0;
double kReadRatio = 50;

XMD::multicast::multicastCM *mcm;

std::thread th[MAX_APP_THREAD];
std::thread recv_thread;

constexpr uint64_t psn_numbers = 100000;

std::atomic<uint64_t> recv_psn;

// std::unique_ptr<XMD::multicast::TransferObjBuffer> tob;
XMD::multicast::TransferObjBuffer* tob;

int send_thread_num = XMD::multicast::TransferObjBuffer::default_thread_num;

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

  int thread_pack = (XMD::multicast::kMcCardinality * psn_numbers) / send_thread_num;
  int i = 0;

  auto start = std::chrono::high_resolution_clock::now();
  while (XMD::multicast::global_psn.load() < psn_numbers) {
    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = to_key(dis);
    Value v;
    if (rand_r(&seed) % 100 < kReadRatio) {  // GET
    } else {
      v = 23;
      TS ts = XMD::myClock::get_ts();
      // tob->insert(key, ts, v, tob_pos);
      tob->packKVTS(XMD::KVTS(key, ts, v), id);
      // tob->packKVTS(XMD::KVTS(key, ts, v));
    }
    i++;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  std::cout << "throughput" << (psn_numbers / (duration)) * XMD::kMcPageSize << "Mops" << std::endl;
}

int main(int argc, char **argv) {
  // test the if the package is lost
  printf("rate limit validate experiments start\n");
  printf("page size %d, cardinality %d\n", XMD::kMcPageSize,
         XMD::multicast::kMcCardinality);

  DSMConfig config;
  config.machineNR = 2;
  config.memThreadCount = 1;
  config.computeNR = 2;
  config.index_type = 3;
  DSM *dsm = DSM::getInstance(config);

  mcm = new XMD::multicast::multicastCM(dsm, 1);
  mcm->print_self();
  // mcm2 = std::make_unique<rdmacm::multicast::multicastCM>(dsm);

  dsm->barrier("init-mc");

  tob = new XMD::multicast::TransferObjBuffer(mcm, 0);
  recv_thread = std::thread(XMD::multicast::TransferObjBuffer::fetch_thread_run, tob);

  dsm->barrier("init-recv");

  for (int i = 0; i < send_thread_num; i++) {
    th[i] = std::thread(thread_run, i);
  }

  for (int i = 0; i < send_thread_num; i++) {
    th[i].join();
  }

  tob->print_recv();

  // for (int i = 0;i< psn_numbers;i++) {
  //   XMD::multicast::printTransferObj(recv_objs[i]);
  // }
  // for (int i = 0; i < kMaxMulticastSendCoreNum; i++) {
  //   th[i].join();
  // }

  // uint64_t loss_packages =
  //     rdmacm::multicast::check_package_loss(FLAGS_psn_numbers);
  // double loss_rate = (double)loss_packages / FLAGS_psn_numbers;
  // printf("loss rate %f\n", loss_rate);
  printf("test complete\n");

  while (true) {}
  return 0;
}