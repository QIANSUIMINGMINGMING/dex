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

#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
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

void printTransferObj(const TransferObj &obj, int numElementsToPrint = 5);

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
  int received_ = 0;
  int received_from_remote_ = 0;

  // send message control
  int send_pos{0};

  uint8_t *send_messages;

  // recv message control
  uint8_t *recv_messages;

  void print_self() {
    printf("node: %d\n", id);
    // ud related
    printf("remote qpn: %d\n", remote_qpn);
    printf("remote qkey: %d\n", remote_qkey);
    // ah address
    printf("ah address: %p\n", ah);
    printf("send pos: %d\n", send_pos);
  }
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
  multicastCM(DSM *dsm, int group_size, u64 buffer_size = 1,
              std::string mc_ip = "226.0.0.1");
  ~multicastCM();

  int test_node();
  int send_message(int node_id, int pos);
  int get_pos(int node_id, TransferObj *&message_address);
  uint64_t get_cnode_id() { return cnode_id; }
  multicast_node *getNode(int i) { return nodes[i]; }

  void fetch_message(int node_id, TransferObj *message);
  void print_self() {
    printf("transferobj size %lu\n", sizeof(TransferObj));
    for (size_t i = 0; i < mcGroups; i++) {
      nodes[i]->print_self();
    }
  }

  int getGroupSize() { return mcGroups; }

 private:
  int init_node(struct multicast_node *node);
  int create_message(struct multicast_node *node);
  void destroy_node(struct multicast_node *node);
  int alloc_nodes();

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
  static void *cma_thread_manager(void *arg);
  static void *mc_maintainer(DSM *dsm, int node_id, multicastCM *me);

 private:
  // mc info
  uint64_t cnode_id;
  pthread_t cmathread;
  int mcGroups;

  // recv handling components
  constexpr static int kMaxNodes = 10;
  std::thread maintainers[kMaxNodes];
  moodycamel::ConcurrentQueue<TransferObj *> recv_obj_ptrs[kMaxNodes];

  // rdma info for multicast
  struct multicast_node *nodes[kMaxNodes];
  std::string mcIp;
  struct ibv_mr *mr;
  struct ibv_pd *pd;
  utils::SynchronizedMonotonicBufferRessource mbr;
  DSM *dsm_;
};

constexpr int BUFFER_SIZE = kMcMaxPostList / 2;
// BufferPool class to manage two buffers and threading

static inline std::atomic<uint64_t> global_psn{0};

// class TransferObjBuffer_v2 {
//  public:
//   multicastCM *my_cm_;
//   multicast_node *cm_node_;
//   int cur_pos{0};

//   std::atomic<uint64_t> cur_element_num_in_concurrency{0};
//   TransferObj *buffer_;
//   TransferObj *recv_buffer_;
//   TransferObj recv_buffer_for_test[1000];
//   // std::thread fetch_thread;

//   std::atomic<uint64_t> ready_to_send{0};
//   inline static thread_local bool pause_flag = false;

//   int recv_messages_num{0};
//   TransferObjBuffer_v2(multicastCM *cm, int thread_group_id) : my_cm_(cm) {
//     cm_node_ = my_cm_->getNode(thread_group_id);
//     cur_pos = my_cm_->get_pos(thread_group_id, buffer_);
//     buffer_->node_id = my_cm_->get_cnode_id();
//     buffer_->psn = global_psn.fetch_add(1);
//     recv_buffer_ = new TransferObj();
//   }

//   // test
//   void print_recv() {
//     int print_num = std::min(recv_messages_num, 20);
//     for (int i = 0; i < print_num; i++) {
//       printTransferObj(recv_buffer_[i]);
//     }
//   }

//   ~TransferObjBuffer_v2() { delete recv_buffer_; }

//   static void fetch_thread_run(TransferObjBuffer *tob) {
//     int multicast_node_id = tob->cm_node_->id;
//     while (true) {
//       // tob->my_cm_->fetch_message(multicast_node_id, tob->recv_buffer_);
//       tob->my_cm_->fetch_message(
//           multicast_node_id,
//           &(tob->recv_buffer_for_test[tob->recv_messages_num]));
//       tob->recv_messages_num++;
//       printf("one receive\n");
//     }
//     // tob->my_cm_->fetch_message(m, tob->buffer_);
//   }

//   void packKVTS(const KVTS &kvts, int thread_id) {
//     while (pause_flag && ready_to_send.load() != 0) {
//       yield(1);
//     }
//     if (pause_flag) {
//       pause_flag = false;
//       thread_num_cur[thread_id] = 0;
//     }
//     uint64_t &my_thread_cur = thread_num_cur[thread_id];
//     if (my_thread_cur < per_thread_occupy) {
//       uint64_t my_pos = my_thread_cur + thread_id * per_thread_occupy;
//       buffer_->elements[my_pos].k = kvts.k;
//       buffer_->elements[my_pos].ts = kvts.ts;
//       buffer_->elements[my_pos].v = kvts.v;
//       my_thread_cur++;
//       if (ready_to_send.load() != 0) {
//         ready_to_send.fetch_add(1);
//         pause_flag = true;
//       }
//     } else {
//       // put into concurrency
//       uint64_t my_pos = cur_element_num_in_concurrency.fetch_add(1);
//       if (my_pos < concurrency_num) {
//         buffer_->elements[my_pos].k = kvts.k;
//         buffer_->elements[my_pos].ts = kvts.ts;
//         buffer_->elements[my_pos].v = kvts.v;
//         if (my_pos == concurrency_num - 1) {
//           ready_to_send.fetch_add(1);
//           while (ready_to_send.load() != default_thread_num) {
//             yield(1);
//           }
//           // send
//           my_cm_->send_message(cm_node_->id, cur_pos);
//           cur_pos = my_cm_->get_pos(cm_node_->id, buffer_);
//           buffer_->node_id = my_cm_->get_cnode_id();
//           buffer_->psn = global_psn.fetch_add(1);
//           cur_element_num_in_concurrency.store(0);
//           ready_to_send.store(0);
//         }
//       } else {
//         ready_to_send.fetch_add(1);
//         pause_flag = true;
//       }
//     }
//   }
// };

class TransferObjBuffer {
 public:
  multicastCM *my_cm_;
  multicast_node *cm_node_;
  int cur_pos{0};

  // uint64_t packing_thread_num;
  // uint64_t per_thread_occupy;
  // //     kMcCardinality / (packing_thread_num + 1);
  // inline static thread_local uint64_t thread_num_cur{0};
  // uint64_t concurrency_num;

  std::atomic<uint64_t> cur_element_num_in_concurrency{0};
  TransferObj *buffer_;
  TransferObj *recv_buffer_;
  TransferObj recv_buffer_for_test[1000];
  // std::thread fetch_thread;

  std::atomic<uint64_t> ready_to_send{0};
  inline static thread_local bool pause_flag = false;
  int recv_messages_num{0};
  TransferObjBuffer(multicastCM *cm, int thread_group_id) : my_cm_(cm) {
    cm_node_ = my_cm_->getNode(thread_group_id);
    cur_pos = my_cm_->get_pos(thread_group_id, buffer_);
    buffer_->node_id = my_cm_->get_cnode_id();
    buffer_->psn = global_psn.fetch_add(1);
    recv_buffer_ = new TransferObj();
  }

  // test
  void print_recv() {
    int print_num = std::min(recv_messages_num, 20);
    for (int i = 0; i < print_num; i++) {
      printTransferObj(recv_buffer_[i]);
    }
  }

  ~TransferObjBuffer() { delete recv_buffer_; }

  static void fetch_thread_run(TransferObjBuffer *tob) {
    int multicast_node_id = tob->cm_node_->id;
    while (true) {
      tob->my_cm_->fetch_message(multicast_node_id, tob->recv_buffer_);
      // tob->my_cm_->fetch_message(
      //     multicast_node_id,
      //     &(tob->recv_buffer_for_test[tob->recv_messages_num]));
      // tob->recv_messages_num++;
      // printf("one receive\n");
    }
    // tob->my_cm_->fetch_message(m, tob->buffer_);
  }

  void packKVTS(const KVTS &kvts, int thread_id) {
  restart:
    // while (pause_flag && ready_to_send.load() != 0) {
    //   yield(1);
    // }
    // if (pause_flag) {
    //   pause_flag = false;
    //   thread_num_cur[thread_id] = 0;
    // }
    // uint64_t &my_thread_cur = thread_num_cur[thread_id];
    // if (my_thread_cur < per_thread_occupy) {
    //   uint64_t my_pos = my_thread_cur + thread_id * per_thread_occupy;
    //   buffer_->elements[my_pos].k = kvts.k;
    //   buffer_->elements[my_pos].ts = kvts.ts;
    //   buffer_->elements[my_pos].v = kvts.v;
    //   my_thread_cur++;
    //   if (ready_to_send.load() != 0) {
    //     ready_to_send.fetch_add(1);
    //     pause_flag = true;
    //   }
    // } else {
    // put into concurrency
    uint64_t my_pos = cur_element_num_in_concurrency.fetch_add(1);
    if (my_pos < kMcCardinality) {
      buffer_->elements[my_pos].k = kvts.k;
      buffer_->elements[my_pos].ts = kvts.ts;
      buffer_->elements[my_pos].v = kvts.v;
      if (my_pos == kMcCardinality - 1) {
        my_cm_->send_message(cm_node_->id, cur_pos);
        cur_pos = my_cm_->get_pos(cm_node_->id, buffer_);
        buffer_->node_id = my_cm_->get_cnode_id();
        buffer_->psn = global_psn.fetch_add(1);
        cur_element_num_in_concurrency.store(0);
      }
    } else {
      int yield_time = 0;
      while (cur_element_num_in_concurrency.load() >= kMcCardinality) {
        yield_time++;
        yield(yield_time);
      }
      goto restart;
    }
    // if (my_pos < ) {
    //   buffer_->elements[my_pos].k = kvts.k;
    //   buffer_->elements[my_pos].ts = kvts.ts;
    //   buffer_->elements[my_pos].v = kvts.v;
    //   if (my_pos == concurrency_num - 1) {
    //     ready_to_send.fetch_add(1);
    //     while (ready_to_send.load() != default_thread_num) {
    //       yield(1);
    //     }
    //     // send
    //     my_cm_->send_message(cm_node_->id, cur_pos);
    //     cur_pos = my_cm_->get_pos(cm_node_->id, buffer_);
    //     buffer_->node_id = my_cm_->get_cnode_id();
    //     buffer_->psn = global_psn.fetch_add(1);
    //     cur_element_num_in_concurrency.store(0);
    //     ready_to_send.store(0);
    //   }
    // } else {
    //   ready_to_send.fetch_add(1);
    //   pause_flag = true;
    // }
  }
};

};  // namespace multicast
};  // namespace XMD