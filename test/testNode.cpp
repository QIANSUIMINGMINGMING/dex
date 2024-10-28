#include "XMD/InMemoryBtree.h"

using namespace XMD;

int main() {
  // Create an instance of YourClass
  NodePage rootNode(0, GlobalAddress::Null());

  // Initialize rootNode with some keys and values
  rootNode.header.count = 3;
  rootNode.keys[0] = 10;
  rootNode.values[0] = 101;
  rootNode.keys[1] = 20;
  rootNode.values[1] = 201;
  rootNode.keys[2] = 30;
  rootNode.values[2] = 300;

  // Print initial state of rootNode
  std::cout << "Initial Root Node:\n";

  printNodePage(rootNode);
//   rootNode.printNode();


  // Create a vector of KVTS entries to update the node
  std::vector<KVTS> kvtss1 = {
      KVTS(15, 151, 1100), KVTS(25, 251, 1002),
      KVTS(20, 221, 1003),  // Update existing key with new value
      KVTS(35, 351, 1000), KVTS(40, 401, 1004),
      KVTS(45, 451, 1005), KVTS(50, 501, 1006)};

  std::vector<KVTS *> kvtss;
  for (size_t i = 0; i < kvtss1.size(); ++i) {
    kvtss.push_back(&kvtss1[i]);
  }

  std::vector<NodePage *> newNodes1;

  int updated = rootNode.updateNode(kvtss, newNodes1);
  std::cout << "Number of keys updated: " << updated << std::endl;

  // Output the keys and values in the root node
  printNodePage(rootNode);
  for (size_t idx = 0; idx < newNodes1.size(); ++idx) {
    std::cout << "\nNew Node " << idx + 1 << ":\n";
    printNodePage(*newNodes1[idx]);
  }

  // Initialize rootNode.keys, rootNode.values, and rootNode.numKeys...
  // For example, let's assume rootNode is empty for simplicity

  // Create a vector of KVTS entries to update the node
  std::vector<KVTS *> kvtss2;
  // Populate kvtss with enough entries to cause a split
  KVTS *new_kvts = new KVTS();
  for (uint64_t k = 1; k <= 100; ++k) {
    new_kvts->k = k;
    new_kvts->v = k * 10;
    new_kvts->ts = 1000 + k;
    kvtss2.push_back(new_kvts);
    new_kvts = new KVTS();
  }

  // Vector to hold new nodes created due to splitting
  std::vector<NodePage *> newNodes;

  // Update the node
  updated = rootNode.updateNode(kvtss2, newNodes);

  // Output the number of keys updated
  std::cout << "Number of keys updated: " << updated << std::endl;

//   // Output the keys and values in the root node
//   rootNode.printNode();
  printNodePage(rootNode);

//   // Output the keys and values in the new nodes
  for (size_t idx = 0; idx < newNodes.size(); ++idx) {
    std::cout << "\nNew Node " << idx + 1 << ":\n";
    printNodePage(*newNodes[idx]);
  }

  return 0;
}