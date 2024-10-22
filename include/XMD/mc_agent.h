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

  void fetch_message(TransferObj *message);
  void print_self() {
    printf("transferobj size %lu\n", sizeof(TransferObj));
    printf("node: %d\n", node->id);
    // ud related
    printf("remote qpn: %d\n", node->remote_qpn);
    printf("remote qkey: %d\n", node->remote_qkey);
    // ah address
    printf("ah address: %p\n", node->ah);
    printf("ah GID: ");
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
  moodycamel::ConcurrentQueue<TransferObj *> recv_obj_ptrs;

  // rdma info for multicast
  struct multicast_node *node;  // default one group
  std::string mcIp;
  struct ibv_mr *mr;
  struct ibv_pd *pd;
  utils::SynchronizedMonotonicBufferRessource mbr;
  DSM *dsm_;
};

constexpr int BUFFER_SIZE = kMcMaxPostList / 2;
// BufferPool class to manage two buffers and threading
class TransferObjBuffer {
 public:
  multicastCM *my_cm_;
  multicast_node *cm_node_;
  int cur_psn{0};

  TransferObj *buffers_[BUFFER_SIZE];
  int active_buffer_{0};
  uint64_t maxSeqN{0};
  uint64_t buffer_start_seqn[BUFFER_SIZE]{0};
  std::atomic<uint64_t> buffer_element_count[BUFFER_SIZE]{0};
  std::atomic<cutil::ull_t> buffer_mutex{0};
  std::atomic<uint64_t> cur_global_se_max{0};

  std::atomic<int> buffer_full_cv[BUFFER_SIZE]{0};

  TransferObjBuffer(multicastCM *cm) : my_cm_(cm) {
    cm_node_ = my_cm_->getNode();
    for (int i = 0; i < BUFFER_SIZE; i++) {
      buffer_start_seqn[i] = (static_cast<uint64_t>(i * kMcCardinality));
      buffers_[i] = (TransferObj *)(cm_node_->send_messages + kMcPageSize * i);
    }
    maxSeqN = BUFFER_SIZE * kMcCardinality;
  }

  ~TransferObjBuffer() {}

  void packKVTS(const KVTS &kvts) {
    int my_pos = cur_global_se_max.fetch_add(1);
    while (!can_go(my_pos));
    for (int i = 0; i < BUFFER_SIZE; i++) {
      if (my_pos >= buffer_start_seqn[i] &&
          my_pos < (buffer_start_seqn[i] + kMcCardinality)) {
        TransferObj *obj_place = buffers_[i];
        KVTS *set_place = (obj_place->elements) + (my_pos % kMcCardinality);
        set_place->k = kvts.k;
        set_place->ts = kvts.ts;
        set_place->v = kvts.v;
        if (buffer_element_count[i].fetch_add(1) == kMcCardinality - 1) {
          // set conditional variable for sending
          buffer_full_cv[i].store(1);
        }
        break;
      }
    }
  }

  bool can_go(uint64_t pos) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool need_restart = false;
    cutil::ull_t cur_v =
        cutil::read_lock_or_restart(buffer_mutex, need_restart);
    if (need_restart) {
      goto restart;
    }
    bool can_go = pos < maxSeqN;
    cutil::read_unlock_or_restart(buffer_mutex, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    return can_go;
  }

  void sendBuffers() {
    while (true) {
      int buf_idx = active_buffer_;
      // wait conditional variable and send buffer_[active_buffer_], you do not
      // need to care rdma_Send
      while (buffer_full_cv[buf_idx].load() == 0) {
        yield(1);
      }

      TransferObj *next_buffer_field;
      int node_send_pos = my_cm_->get_pos(next_buffer_field);
      next_buffer_field->node_id = my_cm_->get_cnode_id();
      next_buffer_field->psn = cur_psn;
      cur_psn++;
      my_cm_->send_message(node_send_pos);

      // update buffer fields
      bool need_restart = false;
      cutil::ull_t cur_v =
          cutil::read_lock_or_restart(buffer_mutex, need_restart);
      if (need_restart) {
        assert(false);
      }
      cutil::upgrade_to_write_lock_or_restart(buffer_mutex, cur_v,
                                              need_restart);
      if (need_restart) {
        assert(false);
      }

      buffers_[active_buffer_] = next_buffer_field;
      buffer_element_count[active_buffer_].store(0);
      buffer_start_seqn[active_buffer_] = maxSeqN;
      maxSeqN += kMcCardinality;

      active_buffer_ = RING_ADD(active_buffer_, 1, BUFFER_SIZE);

      cutil::write_unlock(buffer_mutex);
      // then change active_buffer_ to next
      // Then modify the maxSeqN and other corresponding fields
    }
  }
};

};  // namespace multicast
};  // namespace XMD