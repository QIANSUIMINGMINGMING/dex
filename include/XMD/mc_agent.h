// #pragma once

// #include <arpa/inet.h>
// #include <assert.h>
// #include <errno.h>
// #include <getopt.h>
// #include <infiniband/ib.h>
// #include <infiniband/verbs.h>
// #include <libmemcached/memcached.h>
// #include <netdb.h>
// #include <rdma/rdma_cma.h>
// #include <stdint.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <sys/socket.h>
// #include <sys/types.h>
// #include <time.h>
// #include <unistd.h>

// #include <fstream>
// #include <functional>
// #include <string>
// #include <thread>
// #include <unordered_map>
// #include <vector>

// #include "Common.h"
// #include "flags.h"
// #include "memManager.h"
// #include "third_party/concurrentqueue.h"

// namespace XMD {
// namespace multicast {
// constexpr int kMcCardinality =
//     (kMcPageSize - sizeof(u64) - sizeof(u64)) / sizeof(KVTS);

// struct TransferObj {
//   KVTS elements[kMcCardinality];
//   u64 psn{std::numeric_limits<u64>::max()};
//   u64 node_id{std::numeric_limits<u64>::max()};
// } __attribute__((packed));

// struct multicast_node {
//   int id;
//   struct rdma_event_channel *channel;
//   struct rdma_cm_id *cma_id;
//   int connected;

//   struct ibv_cq *send_cq;
//   struct ibv_cq *recv_cq;
//   struct ibv_ah *ah;

//   struct sockaddr_storage dst_in;
//   struct sockaddr *dst_addr;

//   uint32_t remote_qpn;
//   uint32_t remote_qkey;
//   struct ibv_recv_wr recv_wr[kMcMaxRecvPostList];
//   struct ibv_sge recv_sgl[kMcMaxRecvPostList];
//   struct ibv_send_wr send_wr[kMcMaxPostList];
//   struct ibv_sge send_sgl[kMcMaxPostList];

//   // send message control
//   int send_pos{0};

//   uint8_t *send_messages;

//   // recv message control
//   uint8_t *recv_messages;
// };

// struct rdma_event_channel *create_first_event_channel();
// int get_addr(std::string dst, struct sockaddr *addr);
// int verify_port(struct multicast_node *node);

// enum SR { SEND, RECV };

// // package loss
// typedef std::pair<uint64_t, uint64_t> Gpsn;  // <nodeid, psn>
// uint64_t check_package_loss(uint64_t psn_num);

// class multicastCM {
//  public:
//   multicastCM();
//   ~multicastCM();

//   int test_node();
//   void send_message(int pos);
//   int get_pos(TransferObj *&message_address);
//   uint64_t get_cnode_id() { return cnode_id; }
//   multicast_node *getNode() { return node; }

//   void fetch_message(TransferObj *&message);
//   void print_self() {
//     printf("transferobg size %lu\n", sizeof(TransferObj));
//     printf("node: %d\n", node->id);
//     // ud related
//     printf("remote qpn: %d\n", node->remote_qpn);
//     printf("remote qkey: %d\n", node->remote_qkey);
//     // ah address
//     printf("ah address: %p\n", node->ah);
//     printf("send pos: %d\n", node->send_pos);
//   }

//  private:
//   void connect_memcached() {
//     memcached_util::memcached_Connect(memc);
//     if (FLAGS_cnodeId == 0) {
//       memcached_util::memcachedSet(memc, SERVER_NUM_KEY.c_str(),
//                                    SERVER_NUM_KEY.size(), "0", 1);
//     }
//   }
//   void init_message() {
//     memcached_util::memcachedFetchAndAdd(memc, SERVER_NUM_KEY.c_str(),
//                                          SERVER_NUM_KEY.size());
//   }
//   void check_message() {
//     while (true) {
//       uint64_t v = std::stoull(memcached_util::memcachedGet(
//           memc, SERVER_NUM_KEY.c_str(), SERVER_NUM_KEY.size()));
//       if (v == FLAGS_computeNodes) {
//         return;
//       }
//     }
//   }

//   int init_node(struct multicast_node *node);
//   int create_message(struct multicast_node *node);
//   void destroy_node(struct multicast_node *node);
//   int alloc_nodes(int connections = 1);

//   int poll_scqs(int connections, int message_count);
//   int poll_rcqs(int connections, int message_count);
//   int poll_cqs(int connections, int message_count, enum SR sr);
//   int post_recvs(struct multicast_node *node);
//   int post_sends(struct multicast_node *node, int signal_flag);

//   // void send_message(multicast_node *node, uint8_t *message);

//   void handle_recv(struct multicast_node *node, int id);
//   void handle_recv_bulk(struct multicast_node *node, int start_id, int num);

//   int cma_handler(struct rdma_cm_id *cma_id, struct rdma_cm_event *event);
//   int addr_handler(struct multicast_node *node);
//   int join_handler(struct multicast_node *node, struct rdma_ud_param *param);

//   int init_recvs(struct multicast_node *node);

//   int resolve_nodes();

//   static void *cma_thread_worker(void *arg);
//   static void *mc_maintainer(multicastCM *me);

//  private:
//   // mc info
//   uint64_t cnode_id;
//   pthread_t cmathread;

//   // recv handling components
//   std::thread maintainer;
//   moodycamel::ConcurrentQueue<uint8_t *> recv_message_addrs;

//   // rdma info for multicast
//   struct multicast_node *node;  // default one group
//   std::string mcIp;
//   struct ibv_mr *mr;
//   struct ibv_pd *pd;
//   utils::SynchronizedMonotonicBufferRessource mbr;

//   // memcached info
//   memcached_st *memc;
//   std::string SERVER_NUM_KEY;
//   std::string NODE_PSN_KEY;
// };

// class TransferObjBuffer {
//  public:
//   TransferObjBuffer() {
//     struct multicast_node *mc_node;
//     mc_node = mcm->getNode();
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
//     buffer->node_id = mcm->get_cnode_id();
//     cur_psn++;
//     int cur_pos = mcm->get_pos(buffer);
//     mcm->send_message(cur_pos);
//   }

//   void get_psn(uint64_t &psn) { psn = cur_psn; }

//  private:
//   TransferObj *buffer;
//   uint64_t cur_psn{0};
//   multicastCM *mcm;
// };

// };  // namespace multicast
// };  // namespace XMD