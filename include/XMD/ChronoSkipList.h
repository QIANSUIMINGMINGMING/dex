#include "ChronoBuffer.h"
namespace XMD {
class SkipList {
 public:
  SkipList(MonotonicBufferRing<SkipListNode>* allocator)
      : allocator_(allocator) {}

  void init_skiplist() {
    // Initialize random seed
    std::srand(static_cast<unsigned>(std::time(nullptr)));
    SkipListNode* head_node = allocator_->alloc(head_offset);
    head_node->level = kMaxLevel;
    std::memset(head_node->next_ptrs, 0, sizeof(head_node->next_ptrs));
  }

  bool not_full() {
    return cur_num < kMaxSkipListData;
  }

  void insert(const KVTS& kvts) {
    cur_num +=1;
    uint16_t update[kMaxLevel]{0};
    uint16_t current_offset = 0;
    // Find positions to update
    for (int level = kMaxLevel - 1; level >= 0; --level) {
      while (true) {
        SkipListNode& current_node =
            allocator_->operator[](current_offset + head_offset);
        uint16_t relative_next = current_node.next_ptrs[level];
        if (relative_next == 0) break;

        SkipListNode& next_node =
            allocator_->operator[](relative_next + head_offset);

        if (next_node.kvts < kvts) {
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
    SkipListNode* new_node = allocator_->alloc(new_offset_size_t);
    uint64_t new_offset;
    allocator_->check_distance(head_offset, new_offset);
    uint16_t bit16relative = static_cast<uint16_t>(new_offset);

    new_node->kvts = kvts;
    new_node->level = node_level;
    std::memset(new_node->next_ptrs, 0, sizeof(new_node->next_ptrs));

    // Update pointers
    for (int i = 0; i < node_level; ++i) {
      uint16_t prev_offset = update[i];
      SkipListNode& prev_node =
          allocator_->operator[](head_offset + prev_offset);

      uint16_t relative_next = prev_node.next_ptrs[i];

      new_node->next_ptrs[i] = relative_next;
      prev_node.next_ptrs[i] = new_offset;
    }
  }

  bool find(const Key& key, Value& value_out) {
    uint16_t current_offset = 0;
    value_out = kValueNull;
    for (int level = kMaxLevel - 1; level >= 0; --level) {
      while (true) {
        SkipListNode& current_node =
            allocator_->operator[](head_offset + current_offset);
        uint16_t relative_next = current_node.next_ptrs[level];

        if (relative_next == 0) break;
        SkipListNode& next_node =
            allocator_->operator[](head_offset + relative_next);

        if (next_node.kvts.k < key) {
          current_offset = relative_next;
        } else if (next_node.kvts.k == key) {
          value_out = next_node.kvts.v;
          current_offset = relative_next;
        } else {
          break;
        }
      }
    }
    if (value_out != kValueNull) {
      return true;
    }
    return false;
  }

  void reset_skiplist() {
    head_offset = 0;
    cur_num = 0;
  }

  SkipListNode *getNext(SkipListNode * last) {
    uint16_t next_relative = last->next_ptrs[0];
    return &allocator_->operator[](head_offset + next_relative);
  }

  SkipListNode *getHead() {
    return &allocator_->operator[](head_offset);
  }

 private:
  uint64_t head_offset;
  int cur_num = 0;
  MonotonicBufferRing<SkipListNode>* allocator_;

  int randomLevel() {
    int level = 1;
    while ((std::rand() % 2) && level < kMaxLevel) {
      ++level;
    }
    return level;
  }
};
}  // namespace XMD

