#include <cstdint>
#include <iostream>

#include "../DSM.h"
#include "../GlobalAddress.h"
#include "../cache/btree_node.h"
using namespace std;

namespace XMD {
using Key = uint64_t;
using Value = uint64_t;
constexpr int kPageSize = 1024;

constexpr int kInternalCardinality =
    (kPageSize - sizeof(uint32_t) - sizeof(uint8_t) - sizeof(uint8_t) -
     2 * sizeof(bool)) /
    (sizeof(Key) + sizeof(GlobalAddress));

constexpr int keyNum = kInternalCardinality;  // Maximum number of keys per node

class BTreeNode {
 public:
  BTreeNode* leftmost_ptr;
  uint64_t keys[keyNum];  // Stores up to keyNum keys
  uint64_t
      children[keyNum];  // Internal nodes: child pointers; Leaf nodes: values
  int numKeys;           // Number of valid keys in the node
  int degree;            // Minimum degree
  bool isLeaf;           // Is true when node is a leaf

  static int node_count;

  BTreeNode(int degree, bool isLeaf) {
    this->degree = degree;
    this->isLeaf = isLeaf;
    this->numKeys = 0;
    this->leftmost_ptr = nullptr;
    node_count ++;

    // Initialize keys and children with 0
    for (int i = 0; i < keyNum; i++) {
      keys[i] = 0;
      children[i] = 0;
    }
  }
  void insertNonFull(uint64_t key, uint64_t value) {
    int i = numKeys - 1;

    if (isLeaf) {
      // Shift keys and values
      for (int s = 0; s < numKeys , s++;) {
        if (keys[s] == key) {
          keys[s] = value;
          return;
        }
      }

      while (i >= 0 && keys[i] > key) {
        keys[i + 1] = keys[i];
        children[i + 1] = children[i];
        i--;
      }
      keys[i + 1] = key;
      children[i + 1] = value;  // Store the value
      numKeys++;
    } else {
      // Internal node: find where the key should go
      if (key < keys[0]) {
        if (leftmost_ptr->numKeys == keyNum) {
          splitChild(-1, leftmost_ptr);  // Split the leftmost child
        }
        if (keys[0] < key) {
          ((BTreeNode*)children[0])->insertNonFull(key, value);
        } else {
          leftmost_ptr->insertNonFull(key, value);
        }
      } else {
        while (i > 0 && keys[i] > key) {
          i--;
        }
        BTreeNode* child = (BTreeNode*)children[i];
        if (child->numKeys == keyNum) {
          splitChild(i, child);
          if (keys[i] < key) {
            i++;
          }
        }
        ((BTreeNode*)children[i])->insertNonFull(key, value);
      }
    }
  }
  void splitChild(int i, BTreeNode* y) {
    // Create a new node to store (degree - 1) keys of y
    BTreeNode* z = new BTreeNode(y->degree, y->isLeaf);
    z->numKeys = degree - 1;

    // Copy the second half of y's keys to z
    for (int j = 0; j < degree - 1; j++) {
      z->keys[j] = y->keys[j + degree - 1];
      z->children[j] = y->children[j + degree - 1];
    }

    y->numKeys -= degree - 1;

    if (i == -1) {
      children[0] = (uint64_t)z;
      keys[0] = z->keys[0];
    } else {
      for (int j = numKeys; j >= i + 1; j--) {
        children[j + 1] = children[j];
        keys[j + 1] = keys[j];
      }

      children[i] = (uint64_t)z;
      keys[i] = z->keys[0];
    }

    // Insert the middle key of y into the current node
    numKeys++;
  }
  void traverse(int level) {
    cout << "Level " << level << " (" << (isLeaf ? "Leaf" : "Internal")
         << ") - Keys: ";

    for (int i = 0; i < numKeys; i++) {
      cout << keys[i] << " ";
    }
    if (isLeaf) {
      cout << " - Values: ";
      for (int i = 0; i < numKeys; i++) {
        cout << children[i] << " ";  // Print values at the leaf nodes
      }
    }
    cout << endl;

    // If it's not a leaf, recursively print the children
    if (!isLeaf) {
      if (leftmost_ptr != nullptr) {
        leftmost_ptr->traverse(level + 1);
      }
      for (int i = 0; i <= numKeys; i++) {
        BTreeNode* child = reinterpret_cast<BTreeNode*>(children[i]);
        if (child != nullptr) {
          child->traverse(level + 1);  // Increment level for child nodes
        }
      }
    }
  }  // Modified to accept a "level" parameter for depth traversal
  BTreeNode* search(uint64_t key);

 private:
  int maxKeys() { return keyNum; }  // Maximum number of keys (fixed size array)
  int minKeys() { return degree - 1; }  // Minimum number of keys
};

class BTree {
 public:
  BTreeNode* root;
  int degree;

  BTree(int degree) {
    root = nullptr;
    this->degree = degree;
  }

  void traverse() {
    if (root != nullptr) root->traverse(0);  // Start from level 0 (root)
  }

  BTreeNode* search(uint64_t key) {
    return (root == nullptr) ? nullptr : root->search(key);
  }

  void insert(uint64_t key, uint64_t value) {
    if (root == nullptr) {
      root = new BTreeNode(degree, true);
      root->keys[0] = key;
      root->children[0] = value;
      root->numKeys = 1;
    } else {
      if (root->numKeys == keyNum) {
        BTreeNode* newNode = new BTreeNode(degree, false);
        newNode->leftmost_ptr = root;
        newNode->splitChild(-1, root);

        int i = 0;
        if (newNode->keys[0] < key) {
          ((BTreeNode*)newNode->children[i])->insertNonFull(key, value);
        } else {
          newNode->leftmost_ptr->insertNonFull(key, value);
        }
        root = newNode;
      } else {
        root->insertNonFull(key, value);
      }
    }
  }
};

int BTreeNode::node_count = 0;

}  // namespace XMD
