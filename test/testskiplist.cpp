#include "XMD/ChronoSkipList.h"
using namespace XMD;

MonotonicBufferRing<SkipListNode> *allocator;
std::atomic<bool> one_stop;

double tp[100];
std::thread th[100];

void thread_run(int id) { 
  bindCore(id);
  int i = 0;

  auto start = std::chrono::high_resolution_clock::now();
  while( i < 10000000 / 36) {
    // if (one_stop.load()){
    //   break;
    // }
    size_t start;
    allocator->alloc(170, start);
    i+=170;
  }
  // one_stop.store(true);
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  tp[id] = (double)i / (double)duration;
  std::cout << "tp " << id << "i " << i << "throughput "<<tp[id]<< std::endl;
}

int main() {
  // Create an allocator
  size_t buffer_size = 20000000;  // Adjust as needed
  allocator = new MonotonicBufferRing<SkipListNode>(buffer_size);

  for (size_t i = 0; i< 36; i++) {
    th[i] = std::thread(thread_run, i);
  }

  for (size_t i =0; i < 36;i++) {
    th[i].join();
  }

  double throughput = 0;
  for (size_t i = 0; i < 36; i++)
  {
    throughput += tp[i];
  }

  std::cout << "Throughput: "<< throughput << std::endl;
  

  // // Create a SkipList
  // ;
  // SkipList skip_list(&allocator);

  // // Insert some key-value pairs
  // auto start = std::chrono::high_resolution_clock::now();
  // for (int i = 1; i <= 1000000; ++i) {
  //   KVTS kvts = {i, i * 10, 0};  // Key=i, Value=i*10, ts=0
  //   skip_list.insert(kvts);
  // }

  // // std::sort(&allocator[0], &allocator[0] + 1000000);
  // // std::sort(allocator[0])
  // auto end = std::chrono::high_resolution_clock::now();
  // auto duration =
  //     std::chrono::duration_cast<std::chrono::microseconds>(end - start)
  //         .count();
  // std::cout << "Insertion time: " << duration << " us" << std::endl;
  // // calculate throughput
  // std::cout << "Throughput: " << ((double)1000000 / duration) * 1000 * 1000
  //           << " KVTS/s" << std::endl;

  // // Try to find some keys
  // for (int i = 1; i <= 10; ++i) {
  //   Value value_out;
  //   bool found = skip_list.find(i, value_out);
  //   if (found) {
  //     std::cout << "Found key " << i << " with value " << value_out
  //               << std::endl;
  //   } else {
  //     std::cout << "Key " << i << " not found." << std::endl;
  //   }
  // }

  // // Try to find a key that does not exist
  // Value value_out;
  // bool found = skip_list.find(100, value_out);
  // if (found) {
  //   std::cout << "Found key 100 with value " << value_out << std::endl;
  // } else {
  //   std::cout << "Key 100 not found." << std::endl;
  // }

  return 0;
}