#include "XMD/ChronoSkipList.h"

int main() {
  using namespace XMD;

  // Create an allocator
  size_t buffer_size = 1024;  // Adjust as needed
  MonotonicBufferRing<SkipListNode> allocator(buffer_size);

  // Create a SkipList
;  SkipList skip_list(&allocator);

  // Insert some key-value pairs
  for (int i = 1; i <= 10; ++i) {
    KVTS kvts = {i, i * 10, 0};  // Key=i, Value=i*10, ts=0
    skip_list.insert(kvts);
  }

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