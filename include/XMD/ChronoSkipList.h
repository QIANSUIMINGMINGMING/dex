#include "ChronoBuffer.h"
namespace XMD
{
class SkipList {
 public:
  SkipList(MonotonicBufferRing<SkipListNode>* allocator,
           uint64_t first_offset) {
    // Initialize random seed
    std::srand(static_cast<unsigned>(std::time(nullptr)));
    SkipListNode* head_node = allocator->alloc(head_offset);
    head_node->level = kMaxLevel;
    std::memset(head_node->next_ptrs, 0, sizeof(head_node->next_ptrs));
  }

  void insert(const KVTS &kvts) {
    uint16_t update[kMaxLevel]{0};
    uint16_t current_offset = 0;

    // Find positions to update
    for (int level = kMaxLevel - 1; level >= 0; --level) {
      while (true) {
        SkipListNode& current_node = allocator->operator[](current_offset + head_offset);
        uint16_t relative_next = current_node.next_ptrs[level];
        if (relative_next == 0) break;

        SkipListNode& next_node = allocator->operator[](relative_next + head_offset);

        if (next_node.kvts.k < kvts.k) {
          current_offset = relative_next;
        } else {
          break;
        }
      }
      update[level] = current_offset;
    }

    // Generate random level for new node
    int node_level = randomLevel();

    // Allocate new node
    size_t new_offset_size_t;
    SkipListNode* new_node = allocator->alloc(new_offset_size_t);
    uint16_t new_offset = static_cast<uint16_t>(new_offset_size_t - head_offset);

    new_node->kvts = kvts;
    new_node->level = node_level;
    std::memset(new_node->next_ptrs, 0, sizeof(new_node->next_ptrs));

    // Update pointers
    for (int i = 0; i < node_level; ++i) {
      uint16_t prev_offset = update[i];
      SkipListNode& prev_node = allocator->operator[](head_offset + prev_offset);

      uint16_t relative_next = prev_node.next_ptrs[i];

      new_node->next_ptrs[i] = relative_next;
      prev_node.next_ptrs[i] = new_offset;
    }
  }

  bool find(const Key& key, Value& value_out) {
    uint16_t current_offset = head_offset;

    for (int level = kMaxLevel - 1; level >= 0; --level) {
      while (true) {
        SkipListNode<Key, Value>& current_node = allocator[current_offset];
        uint16_t relative_next = current_node.next_ptrs[level];

        if (relative_next == 0) break;

        uint16_t next_offset = current_offset + relative_next;
        SkipListNode<Key, Value>& next_node = allocator[next_offset];

        if (next_node.kvts.key == key) {
          value_out = next_node.kvts.value;
          return true;
        } else if (next_node.kvts.key < key) {
          current_offset = next_offset;
        } else {
          break;
        }
      }
    }
    return false;
  }

 private:
  uint64_t head_offset;
  MonotonicBufferRing<SkipListNode> * allocator;

  int randomLevel() {
    int level = 1;
    while ((std::rand() % 2) && level < kMaxLevel) {
      ++level;
    }
    return level;
  }
};
} // namespace XMD

