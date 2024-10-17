#include <cstdint>
#include <iostream>
#include "XMD/InMemoryBtree.h"
using namespace std;

// constexpr int keyNum = 6;  // Maximum number of keys per node

// uint64_t node_num = 0;

// class BTreeNode {
//  public:
//   uint64_t keys[keyNum];  // Stores up to keyNum keys
//   uint64_t children[keyNum +
//                     1];  // Internal nodes: child pointers; Leaf nodes: values
//   int numKeys;           // Number of valid keys in the node
//   int degree;            // Minimum degree
//   bool isLeaf;           // Is true when node is a leaf

//   BTreeNode(int degree, bool isLeaf);
//   void insertNonFull(uint64_t key, uint64_t value);
//   void splitChild(int i, BTreeNode* y);
//   void traverse(
//       int level);  // Modified to accept a "level" parameter for depth traversal
//   BTreeNode* search(uint64_t key);

//  private:
//   int maxKeys() { return keyNum; }  // Maximum number of keys (fixed size array)
//   int minKeys() { return degree - 1; }  // Minimum number of keys
// };

// class BTree {
//  public:
//   BTreeNode* root;
//   int degree;

//   BTree(int degree) {
//     root = nullptr;
//     this->degree = degree;
//   }

//   void traverse() {
//     if (root != nullptr) root->traverse(0);  // Start from level 0 (root)
//   }

//   BTreeNode* search(uint64_t key) {
//     return (root == nullptr) ? nullptr : root->search(key);
//   }

//   void insert(uint64_t key, uint64_t value);
// };

// BTreeNode::BTreeNode(int degree, bool isLeaf) {
//   this->degree = degree;
//   this->isLeaf = isLeaf;
//   this->numKeys = 0;
//   node_num += 1;
//   for (int i = 0; i < keyNum; i++) keys[i] = 0;
//   for (int i = 0; i < keyNum + 1; i++) children[i] = 0;
// }

// // Traverse the B-tree and print detailed information
// void BTreeNode::traverse(int level) {
//   cout << "Level " << level << " (" << (isLeaf ? "Leaf" : "Internal")
//        << ") - Keys: ";

//   for (int i = 0; i < numKeys; i++) {
//     cout << keys[i] << " ";
//   }
//   if (isLeaf) {
//     cout << " - Values: ";
//     for (int i = 0; i < numKeys; i++) {
//       cout << children[i] << " ";  // Print values at the leaf nodes
//     }
//   }
//   cout << endl;

//   // If it's not a leaf, recursively print the children
//   if (!isLeaf) {
//     for (int i = 0; i <= numKeys; i++) {
//       BTreeNode* child = reinterpret_cast<BTreeNode*>(children[i]);
//       if (child != nullptr) {
//         child->traverse(level + 1);  // Increment level for child nodes
//       }
//     }
//   }
// }

// BTreeNode* BTreeNode::search(uint64_t key) {
//   int i = 0;
//   while (i < numKeys && key > keys[i]) i++;

//   if (i < numKeys && keys[i] == key) return this;

//   if (isLeaf) return nullptr;

//   return reinterpret_cast<BTreeNode*>(children[i])->search(key);
// }

// void BTree::insert(uint64_t key, uint64_t value) {
//   if (root == nullptr) {
//     root = new BTreeNode(degree, true);
//     root->keys[0] = key;
//     root->children[0] = value;  // Store value at the leaf
//     root->numKeys = 1;
//   } else {
//     if (root->numKeys == keyNum) {
//       BTreeNode* s = new BTreeNode(degree, false);
//       s->children[0] = reinterpret_cast<uint64_t>(root);
//       s->splitChild(0, root);

//       int i = 0;
//       if (s->keys[0] < key) i++;
//       reinterpret_cast<BTreeNode*>(s->children[i])->insertNonFull(key, value);

//       root = s;
//     } else {
//       root->insertNonFull(key, value);
//     }
//   }
// }

// void BTreeNode::insertNonFull(uint64_t key, uint64_t value) {
//   int i = numKeys - 1;

//   if (isLeaf) {
//     while (i >= 0 && keys[i] > key) {
//       keys[i + 1] = keys[i];
//       children[i + 1] = children[i];  // Shift values as well
//       i--;
//     }
//     keys[i + 1] = key;
//     children[i + 1] = value;  // Insert value at the correct position
//     numKeys++;
//   } else {
//     while (i >= 0 && keys[i] > key) i--;

//     BTreeNode* child = reinterpret_cast<BTreeNode*>(children[i + 1]);
//     if (child->numKeys == keyNum) {
//       splitChild(i + 1, child);
//       if (keys[i + 1] < key) i++;
//     }
//     child = reinterpret_cast<BTreeNode*>(children[i + 1]);
//     child->insertNonFull(key, value);
//   }
// }

// void BTreeNode::splitChild(int i, BTreeNode* y) {
//   BTreeNode* z = new BTreeNode(y->degree, y->isLeaf);
//   z->numKeys = degree - 1;  // z will have (degree-1) keys after split

//   // Move the last (degree-1) keys of y to z
//   for (int j = 0; j < degree - 1; j++) z->keys[j] = y->keys[j + degree];

//   if (y->isLeaf) {
//     // Move the last (degree-1) values of y to z (since it's a leaf)
//     for (int j = 0; j < degree - 1; j++)
//       z->children[j] = y->children[j + degree];
//   } else {
//     // Move the last degree children of y to z (if it's an internal node)
//     for (int j = 0; j < degree; j++) z->children[j] = y->children[j + degree];
//   }

//   y->numKeys = degree - 1;  // y will now have (degree-1) keys

//   // Shift children of this node to make space for the new child
//   for (int j = numKeys; j >= i + 1; j--) children[j + 1] = children[j];

//   children[i + 1] = reinterpret_cast<uint64_t>(z);

//   // Move the middle key of y to this node
//   for (int j = numKeys - 1; j >= i; j--) keys[j + 1] = keys[j];

//   keys[i] = y->keys[degree - 1];
//   numKeys++;
// }

int main() {
  XMD::BTree t(XMD::keyNum/2 + 1);  // A B-tree with minimum degree 2
  t.insert(10, 100);
  t.insert(20, 200);
  t.insert(5, 50);
  t.insert(6, 60);
  t.insert(12, 120);
  t.insert(30, 300);
  t.insert(7, 70);
  t.insert(17, 170);

  cout << "Detailed Traversal of the constructed B-tree with values:" << endl;
  t.traverse();
  cout << endl;
  // cout << "node nums " << node_num;

  return 0;
}