#pragma once

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <getopt.h>
#include <infiniband/ib.h>
#include <infiniband/verbs.h>
#include <libmemcached/memcached.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <fstream>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../Common.h"
#include "../DSM.h"
#include "../third_party/concurrentqueue.h"
#include "memManager.h"

namespace XMD {
namespace multicast {
constexpr int kMcCardinality =
    (kMcPageSize - sizeof(u64) - sizeof(u64)) / sizeof(KVTS);

struct TransferObj {
  KVTS elements[kMcCardinality];
  u64 psn{std::numeric_limits<u64>::max()};
  u64 node_id{std::numeric_limits<u64>::max()};
} __attribute__((packed));

class TransferObjBuffer {
 public:
  TransferObjBuffer(size_t buffer_size)
      : buffer_size_(buffer_size), write_index_(0), full_flag_(false) {
    // Initialize buffer to hold TransferObjs
    buffer_.reserve(buffer_size_);
  }

  // Insert a KVTS (k, ts, v) into the current TransferObj in the buffer
  bool insert(uint64_t k, uint64_t ts, uint64_t v) {
    if (full_flag_.load()) {
      return false;  // Buffer is full
    }

    size_t index = write_index_.fetch_add(1, std::memory_order_relaxed);

    if (index < buffer_size_) {
      // Get the current TransferObj to insert data into
      TransferObj &current_obj = buffer_[index / kMcCardinality];
      size_t element_index = index % kMcCardinality;
      current_obj.elements[element_index] = {k, ts, v};

      if (element_index == kMcCardinality - 1) {
        // When the current TransferObj is full, move to the next
        full_flag_.store(true, std::memory_order_release);
      }
      return true;
    } else {
      return false;
    }
  }

  // Send the TransferObj to remote when full
  void send_to_remote() {
    if (full_flag_.load()) {
      // Simulate sending to remote
      for (const auto &transfer_obj : buffer_) {
        send(transfer_obj);
      }
      reset();
    }
  }

 private:
  void send(const TransferObj &obj) {
    std::cout << "Sending TransferObj to remote (psn: " << obj.psn
              << ", node_id: " << obj.node_id << ")...\n";
  }

  void reset() {
    // Reset the buffer and write index
    write_index_.store(0, std::memory_order_relaxed);
    full_flag_.store(false, std::memory_order_release);
  }

  size_t buffer_size_;
  std::vector<TransferObj> buffer_;  // Buffer of TransferObjs
  std::atomic<size_t> write_index_;  // Current write index
  std::atomic<bool> full_flag_;      // Indicates if the buffer is full
};

struct multicast_node {
  int id;
  struct rdma_event_channel *channel;
  struct rdma_cm_id *cma_id;
  int connected;

  struct ibv_cq *send_cq;
  struct ibv_cq *recv_cq;
  struct ibv_ah *ah;

  struct sockaddr_storage dst_in;
  struct sockaddr *dst_addr;

  uint32_t remote_qpn;
  uint32_t remote_qkey;
  struct ibv_recv_wr recv_wr[kMcMaxRecvPostList];
  struct ibv_sge recv_sgl[kMcMaxRecvPostList];
  struct ibv_send_wr send_wr[kMcMaxPostList];
  struct ibv_sge send_sgl[kMcMaxPostList];

  // send message control
  int send_pos{0};

  uint8_t *send_messages;

  // recv message control
  uint8_t *recv_messages;
};

struct rdma_event_channel *create_first_event_channel();
int get_addr(std::string dst, struct sockaddr *addr);
int verify_port(struct multicast_node *node);

enum SR { SEND, RECV };

// package loss
typedef std::pair<uint64_t, uint64_t> Gpsn;  // <nodeid, psn>
uint64_t check_package_loss(DSM *dsm, uint64_t psn_num);

class multicastCM {
 public:
  multicastCM(DSM *dsm, u64 buffer_size = 1, std::string mc_ip = "226.0.0.1");
  ~multicastCM();

  int test_node();
  int send_message(int pos);
  int get_pos(TransferObj *&message_address);
  uint64_t get_cnode_id() { return cnode_id; }
  multicast_node *getNode() { return node; }

  void fetch_message(TransferObj *&message);
  void print_self() {
    printf("transferobj size %lu\n", sizeof(TransferObj));
    printf("node: %d\n", node->id);
    // ud related
    printf("remote qpn: %d\n", node->remote_qpn);
    printf("remote qkey: %d\n", node->remote_qkey);
    // ah address
    printf("ah address: %p\n", node->ah);
    printf("send pos: %d\n", node->send_pos);
  }

 private:
  int init_node(struct multicast_node *node);
  int create_message(struct multicast_node *node);
  void destroy_node(struct multicast_node *node);
  int alloc_nodes(int connections = 1);

  int poll_scqs(int connections, int message_count);
  int poll_rcqs(int connections, int message_count);
  int poll_cqs(int connections, int message_count, enum SR sr);
  int post_recvs(struct multicast_node *node);
  int post_sends(struct multicast_node *node, int signal_flag);

  // void send_message(multicast_node *node, uint8_t *message);

  void handle_recv(struct multicast_node *node, int id);
  void handle_recv_bulk(struct multicast_node *node, int start_id, int num);

  int cma_handler(struct rdma_cm_id *cma_id, struct rdma_cm_event *event);
  int addr_handler(struct multicast_node *node);
  int join_handler(struct multicast_node *node, struct rdma_ud_param *param);

  int init_recvs(struct multicast_node *node);

  int resolve_nodes();

  static void *cma_thread_worker(void *arg);
  static void *mc_maintainer(DSM *dsm, multicastCM *me);

 private:
  // mc info
  uint64_t cnode_id;
  pthread_t cmathread;

  // recv handling components
  std::thread maintainer;
  moodycamel::ConcurrentQueue<uint8_t *> recv_message_addrs;

  // rdma info for multicast
  struct multicast_node *node;  // default one group
  std::string mcIp;
  struct ibv_mr *mr;
  struct ibv_pd *pd;
  utils::SynchronizedMonotonicBufferRessource mbr;
  DSM *dsm_;

  // memcached info
  memcached_st *memc;
  std::string SERVER_NUM_KEY;
  std::string NODE_PSN_KEY;
};


// class TransferObjBuffer {
//  public:
//   TransferObjBuffer(multicastCM *agent) {
//     struct multicast_node *mc_node;
//     mc_node = agent_->getNode();
//     buffer = (TransferObj *)(mc_node->send_messages);
//   }

//   void insert(Key k, TS ts, Value v, int pos) {
//     buffer->elements[pos] = {k, v, ts};
//   }

//   void emit() {
//     buffer->psn = cur_psn;
//     // if (cur_psn == FLAGS_psn_numbers - 1) {
//     //   is_end = true;
//     // }
//     buffer->node_id = agent_->get_cnode_id();
//     cur_psn++;
//     int cur_pos = agent_->get_pos(buffer);
//     agent_->send_message(cur_pos);
//   }

//   void get_psn(uint64_t &psn) { psn = cur_psn; }

//  private:
//   TransferObj *buffer;
//   uint64_t cur_psn{0};
//   multicastCM *agent_;
// };

};  // namespace multicast
};  // namespace XMD