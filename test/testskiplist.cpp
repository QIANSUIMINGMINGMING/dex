#include "XMD/ChronoSkipList.h"

int main() {
  using namespace XMD;

  // Create an allocator
  size_t buffer_size = 2000000;  // Adjust as needed
  MonotonicBufferRing<SkipListNode> allocator(buffer_size);

  // Create a SkipList
  ;
  SkipList skip_list(&allocator);

  // Insert some key-value pairs
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 1; i <= 1000000; ++i) {
    KVTS kvts = {i, i * 10, 0};  // Key=i, Value=i*10, ts=0
    skip_list.insert(kvts);
  }

  std::sort(&allocator[0], &allocator[0] + 1000000);
  // std::sort(allocator[0])
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  std::cout << "Insertion time: " << duration << " us" << std::endl;
  // calculate throughput
  std::cout << "Throughput: " << ((double)1000000 / duration) * 1000 * 1000
            << " KVTS/s" << std::endl;

  // Try to find some keys
  for (int i = 1; i <= 10; ++i) {
    Value value_out;
    bool found = skip_list.find(i, value_out);
    if (found) {
      std::cout << "Found key " << i << " with value " << value_out
                << std::endl;
    } else {
      std::cout << "Key " << i << " not found." << std::endl;
    }
  }

  // Try to find a key that does not exist
  Value value_out;
  bool found = skip_list.find(100, value_out);
  if (found) {
    std::cout << "Found key 100 with value " << value_out << std::endl;
  } else {
    std::cout << "Key 100 not found." << std::endl;
  }

  return 0;
}