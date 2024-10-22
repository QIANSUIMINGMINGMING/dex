#include "XMD/mc_agent.h"

// DEFINE_double(
//     rdmaMemoryFactor, 1.1,
//     "Factor to be multiplied by dramGB");  // factor to be multiplied by
//     dramGB
// DEFINE_uint32(port, 20886, "port");
// DEFINE_uint32(mcport, 20887, "mcport");

// DEFINE_string(ownIp, "172.18.94.80", "own IP server");
// DEFINE_string(mcIp, "226.0.0.1", "multicast group IP");
// DEFINE_uint32(mcGroups, 10, "multicast group number");

// DEFINE_bool(storageNode, false, "storage node");
// DEFINE_uint64(storageNodes, 1, "Number storage nodes participating");

// DEFINE_double(dramGB, 1, "DRAM buffer pool size");

// DEFINE_uint64(computeNodes, 2, "Number compute nodes participating");

// DEFINE_uint64(worker, 1, "Number worker threads");

// DEFINE_string(memcachedIp, "10.16.70.16", "memcached server Ip");
// DEFINE_uint32(memcachedPort, 2378, "memcached server port");

// DEFINE_bool(testmachineOn, false, "leafcache or not on test machine");

// DEFINE_uint32(nodeId, 0, "");
// DEFINE_uint64(cnodeId, 0, "node id for compute nodes");
// DEFINE_uint64(all_worker, 1,
//               "number of all worker threads in the cluster for barrier");
// DEFINE_bool(mcIsSender, false, "cmIsSender");
// DEFINE_int32(internalPageSize, 1024, "internal page size");
// DEFINE_int32(leafPageSize, 1024, "leaf page size");
// DEFINE_int32(KVCacheSize, 1024 * 1024 * 256, "KV Cache Size B");
// DEFINE_bool(usingStringKey, false, "using string keys");

namespace XMD {
namespace multicast {

std::set<Gpsn> packages;
uint64_t check_package_loss(DSM *dsm, uint64_t psn_num) {
  uint64_t loss_packages = 0;
  for (uint64_t i = 0; i < dsm->getComputeNum(); i++) {
    for (uint64_t j = 0; j < psn_num; j++) {
      if (packages.find(std::make_pair(i, j)) == packages.end()) {
        loss_packages++;
      }
    }
  }
  return loss_packages;
}

struct rdma_event_channel *create_first_event_channel() {
  struct rdma_event_channel *channel;
  channel = rdma_create_event_channel();
  if (!channel) {
    if (errno == ENODEV)
      fprintf(stderr, "No RDMA devices were detected\n");
    else
      perror("failed to create RDMA CM event channel");
  }
  return channel;
}

int get_addr(std::string dst, struct sockaddr *addr) {
  struct addrinfo *res;
  int ret;

  ret = getaddrinfo(dst.c_str(), NULL, NULL, &res);
  if (ret) {
    printf("getaddrinfo failed (%s) - invalid hostname or IP address\n",
           gai_strerror(ret));
    return ret;
  }

  memcpy(addr, res->ai_addr, res->ai_addrlen);
  freeaddrinfo(res);
  return ret;
}

int verify_port(struct multicast_node *node) {
  struct ibv_port_attr port_attr;
  int ret;

  ret = ibv_query_port(node->cma_id->verbs, node->cma_id->port_num, &port_attr);
  if (ret) {
    perror("failed to query port");
    return ret;
  }

  if (port_attr.state != IBV_PORT_ACTIVE) {
    fprintf(stderr, "port %d is not active\n", node->cma_id->port_num);
    return -1;
  }

  return 0;
}

multicastCM::multicastCM(DSM *dsm, u64 buffer_size, std::string mc_ip)
    : dsm_(dsm),
      cnode_id(dsm->getMyNodeID()),
      mcIp(mc_ip),
      mbr(buffer_size * 1024 * 1024 * 1024) {
  if (alloc_nodes()) exit(1);

  pthread_create(&cmathread, NULL, cma_thread_worker, node);
  /*
   * Pause to give SM chance to configure switches.  We don't want to
   * handle reliability issue in this simple test program.
   */
  sleep(5);
  // recv maintainer
  maintainer = std::thread(mc_maintainer, dsm_, this);
  sleep(5);

  dsm->barrier("create_multicast_agent", dsm->getComputeNum());
};

multicastCM::~multicastCM() { destroy_node(node); }

int multicastCM::alloc_nodes(int connections) {
  int ret;
  node = (multicast_node *)mbr.allocate(sizeof(struct multicast_node) *
                                        connections);
  if (!node) {
    fprintf(stderr, "failed to allocate memory for test nodes\n");
    return -ENOMEM;
  }

  node->id = 0;
  node->dst_addr = (struct sockaddr *)&node->dst_in;
  ret = get_addr(mcIp, (struct sockaddr *)&node->dst_in);
  if (ret) {
    fprintf(stderr, "failed to get destination address\n");
    return ret;
  }
  node->channel = create_first_event_channel();
  if (!node->channel) {
    fprintf(stderr, "failed to create RDMA CM event channel\n");
    return -1;
  }
  ret = rdma_create_id(node->channel, &node->cma_id, node, RDMA_PS_UDP);
  if (ret) {
    fprintf(stderr, "failed to create RDMA CM ID\n");
    return ret;
  }
  ret = rdma_resolve_addr(node->cma_id, NULL, node->dst_addr, 2000);
  if (ret) {
    perror("mckey: resolve addr failure");
    return ret;
  }
  struct rdma_cm_event *event;

  while (!node->connected && !ret) {
    ret = rdma_get_cm_event(node->channel, &event);
    if (!ret) {
      ret = cma_handler(event->id, event);
      rdma_ack_cm_event(event);
    }
  }

  return ret;
}

int multicastCM::init_node(struct multicast_node *node) {
  struct ibv_qp_init_attr_ex init_qp_attr_ex;
  int cqe, ret = 0;

  if (pd == NULL) {
    pd = ibv_alloc_pd(node->cma_id->verbs);
    if (!pd) {
      fprintf(stderr, "failed to allocate PD\n");
      return -1;
    }

    mr = ibv_reg_mr(pd, mbr.getUnderlyingBuffer(), mbr.getBufferSize(),
                    IBV_ACCESS_LOCAL_WRITE);
    if (!mr) {
      fprintf(stderr, "failed to register MR\n");
      return -1;
    }
  }

  // cqe = message_count ? message_count * 2 : 2;
  cqe = kMcMaxPostList;
  int recv_cqe = kMcMaxRecvPostList;
  node->send_cq = ibv_create_cq(node->cma_id->verbs, cqe, NULL, NULL, 0);
  node->recv_cq = ibv_create_cq(node->cma_id->verbs, recv_cqe, NULL, NULL, 0);

  if (!node->send_cq || !node->recv_cq) {
    ret = -ENOMEM;
    printf("mckey: unable to create CQ\n");
    return ret;
  }

  memset(&init_qp_attr_ex, 0, sizeof(init_qp_attr_ex));
  init_qp_attr_ex.cap.max_send_wr = kMcMaxPostList;
  init_qp_attr_ex.cap.max_recv_wr = kMcMaxRecvPostList;
  init_qp_attr_ex.cap.max_send_sge = 1;
  init_qp_attr_ex.cap.max_recv_sge = 1;
  init_qp_attr_ex.qp_context = node;
  init_qp_attr_ex.sq_sig_all = 0;
  init_qp_attr_ex.qp_type = IBV_QPT_UD;
  init_qp_attr_ex.send_cq = node->send_cq;
  init_qp_attr_ex.recv_cq = node->recv_cq;

  init_qp_attr_ex.comp_mask =
      IBV_QP_INIT_ATTR_CREATE_FLAGS | IBV_QP_INIT_ATTR_PD;
  init_qp_attr_ex.pd = pd;
  init_qp_attr_ex.create_flags =
      IBV_QP_CREATE_BLOCK_SELF_MCAST_LB;  // not work attr

  ret = rdma_create_qp_ex(node->cma_id, &init_qp_attr_ex);

  if (ret) {
    perror("mckey: unable to create QP");
    return ret;
  }

  ret = create_message(node);
  if (ret) {
    printf("mckey: failed to create messages: %d\n", ret);
    return ret;
  }
  return ret;
}

void multicastCM::destroy_node(struct multicast_node *node) {
  if (!node->cma_id) return;

  if (node->ah) ibv_destroy_ah(node->ah);

  if (node->cma_id->qp) rdma_destroy_qp(node->cma_id);

  if (node->send_cq) ibv_destroy_cq(node->send_cq);

  if (node->recv_cq) {
    ibv_destroy_cq(node->recv_cq);
  }

  /* Destroy the RDMA ID after all device resources */
  rdma_destroy_id(node->cma_id);
  rdma_destroy_event_channel(node->channel);
}

int multicastCM::create_message(struct multicast_node *node) {
  node->send_messages = (uint8_t *)mbr.allocate(kMcPageSize * kMcMaxPostList);
  node->recv_messages =
      (uint8_t *)mbr.allocate(kRecvMcPageSize * kMcMaxRecvPostList);
  // post initial recvs
  init_recvs(node);
  return 0;
}

void multicastCM::fetch_message(TransferObj *message) {
  // message = new TransferObj;
  TransferObj *message_place;
  bool got = recv_obj_ptrs.try_dequeue(message_place);
  while (!got) {
    got = recv_obj_ptrs.try_dequeue(message_place);
  }
  memcpy(message, message_place, kMcPageSize);
}

void multicastCM::handle_recv(struct multicast_node *node, int id) {
#if defined(SINGLE_KEY)
  assert(false);
#elif defined(KEY_PAGE)
  uint8_t *message = node->recv_messages + id * kRecvMcPageSize +
                     (kRecvMcPageSize - kMcPageSize);  // no ud padding
  TransferObj *recv_obj_ptr = reinterpret_cast<TransferObj *>(message);
  if (recv_obj_ptr->node_id != cnode_id) recv_obj_ptrs.enqueue(recv_obj_ptr);
    // recv_message_addrs.enqueue(message);
#elif defined(FILTER_PAGE)
#else
  assert(false);
#endif
}

// void multicastCM::handle_recv_bulk(struct multicast_node *node, int start_id,
// int num) {
//   uint8_t *message_1 = node->recv_messages + start_id * kRecvMcPageSize;
//   uint8_t *message_2 = node->recv_messages;
//   int num_1 = num, num_2 = 0;
//   if (start_id + num > kMcMaxRecvPostList) {
//     num_1 = kMcMaxRecvPostList - start_id;
//     num_2 = num - num_1;
//   }
//   recv_message_addrs.enqueue_bulk(message_1, num_1);
//   recv_message_addrs.enqueue_bulk(message_2, num_2);
// }

int multicastCM::cma_handler(struct rdma_cm_id *cma_id,
                             struct rdma_cm_event *event) {
  int ret = 0;
  multicast_node *m_node = static_cast<multicast_node *>(cma_id->context);
  switch (event->event) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
      ret = addr_handler(m_node);
      break;
    case RDMA_CM_EVENT_MULTICAST_JOIN:
      ret = join_handler(m_node, &event->param.ud);
      break;
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_MULTICAST_ERROR:
      printf("mckey: event: %s, error: %d\n", rdma_event_str(event->event),
             event->status);
      // connect_error();
      ret = event->status;
      break;
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
      /* Cleanup will occur after test completes. */
      break;
    default:
      break;
  }
  return ret;
}

int multicastCM::addr_handler(struct multicast_node *node) {
  int ret;
  struct rdma_cm_join_mc_attr_ex mc_attr;

  ret = init_node(node);
  if (ret) return ret;

  mc_attr.comp_mask =
      RDMA_CM_JOIN_MC_ATTR_ADDRESS | RDMA_CM_JOIN_MC_ATTR_JOIN_FLAGS;
  mc_attr.addr = node->dst_addr;
  mc_attr.join_flags = RDMA_MC_JOIN_FLAG_FULLMEMBER;

  ret = rdma_join_multicast_ex(node->cma_id, &mc_attr, node);

  if (ret) {
    perror("mckey: failure joining");
    return ret;
  }
  return 0;
}

int multicastCM::join_handler(struct multicast_node *node,
                              struct rdma_ud_param *param) {
  char buf[40];
  inet_ntop(AF_INET6, param->ah_attr.grh.dgid.raw, buf, 40);
  printf("mckey: joined dgid: %s mlid 0x%x sl %d\n", buf, param->ah_attr.dlid,
         param->ah_attr.sl);

  node->remote_qpn = param->qp_num;
  node->remote_qkey = param->qkey;
  node->ah = ibv_create_ah(pd, &param->ah_attr);
  if (!node->ah) {
    printf("mckey: failure creating address handle\n");
    return -1;
  }

  // init send wrs
  for (int i = 0; i < kMcMaxPostList; i++) {
    node->send_wr[i].next = nullptr;
    node->send_wr[i].sg_list = &node->send_sgl[i];
    node->send_wr[i].num_sge = 1;
    node->send_wr[i].opcode = IBV_WR_SEND;
    if (i % (kMcMaxPostList / 2) == 0) {
      node->send_wr[i].send_flags = IBV_SEND_SIGNALED;
    }
    node->send_wr[i].wr_id = (uint64_t)node;
    node->send_wr[i].wr.ud.ah = node->ah;
    node->send_wr[i].wr.ud.remote_qkey = node->remote_qkey;
    node->send_wr[i].wr.ud.remote_qpn = node->remote_qpn;

    node->send_sgl[i].length = kMcPageSize;
    node->send_sgl[i].lkey = mr->lkey;
    node->send_sgl[i].addr = (uintptr_t)node->send_messages + i * kMcPageSize;
  }

  // init send_wr
  node->connected = 1;
  return 0;
}

int multicastCM::init_recvs(struct multicast_node *node) {
  int ret;
  for (int i = 0; i < kMcMaxRecvPostList; i++) {
    memset(&node->recv_wr[i], 0, sizeof(node->recv_wr[i]));
    node->recv_wr[i].next =
        i == kMcMaxRecvPostList - 1 ? nullptr : &node->recv_wr[i + 1];
    node->recv_wr[i].sg_list = &node->recv_sgl[i];
    node->recv_wr[i].num_sge = 1;
    node->recv_wr[i].wr_id = (uintptr_t)node;

    memset(&node->recv_sgl[i], 0, sizeof(node->recv_sgl[i]));
    node->recv_sgl[i].length = kRecvMcPageSize;
    node->recv_sgl[i].lkey = mr->lkey;
    node->recv_sgl[i].addr =
        (uintptr_t)node->recv_messages + i * kRecvMcPageSize;
  }

  struct ibv_recv_wr *bad_recv_wr;
  ret = ibv_post_recv(node->cma_id->qp, &node->recv_wr[0], &bad_recv_wr);
  if (ret) {
    printf("failed to post receives: %d\n", ret);
  }
  return ret;
}

int multicastCM::get_pos(TransferObj *&next_message_address) {
  int pos = node->send_pos;
  node->send_pos = RING_ADD(node->send_pos, 1, kMcMaxPostList);
  next_message_address =
      (TransferObj *)(node->send_messages + node->send_pos * kMcPageSize);
  return pos;
}

// return the sent position
int multicastCM::send_message(int pos) {
  struct ibv_send_wr *bad_send_wr;
  ibv_send_wr *send_wr = &node->send_wr[pos];
  ibv_sge *sge = &(node->send_sgl[pos]);
  if (pos % (kMcMaxPostList / 2) == 1) {
    struct ibv_wc wc;
    pollWithCQ(node->send_cq, kMcMaxPostList / 2, &wc);
  }
  sge->addr = (uint64_t)node->send_messages + pos * kMcPageSize;
  int ret = ibv_post_send(node->cma_id->qp, send_wr, &bad_send_wr);
  if (ret) {
    printf("failed to post sends: %d\n", ret);
  }

  return ret;
}

void *multicastCM::cma_thread_worker(void *arg) {
  bindCore(mcCmaCore);
  struct rdma_cm_event *event;
  int ret;
  struct multicast_node *node = static_cast<multicast_node *>(arg);
  printf("mckey: worker %d, bind core is %d\n ", node->id, mcCmaCore);
  while (1) {
    ret = rdma_get_cm_event(node->channel, &event);
    if (ret) {
      perror("rdma_get_cm_event");
      break;
    }

    switch (event->event) {
      case RDMA_CM_EVENT_MULTICAST_ERROR:
      case RDMA_CM_EVENT_ADDR_CHANGE:
        printf("mckey: event: %s, status: %d\n", rdma_event_str(event->event),
               event->status);
        break;
      default:
        break;
    }

    rdma_ack_cm_event(event);
  }

  return NULL;
}

void *multicastCM::mc_maintainer(DSM *dsm, multicastCM *me) {
  // int id = (*(static_cast<int16_t *>(args[0])));
  bindCore(rpcCore);
  printf("mckey: maintainer, using core %d\n", rpcCore);

  // multicastCM *me = static_cast<multicastCM *>(args[1]);
  multicast_node *node = me->node;
  struct ibv_wc wc_buffer[kMcMaxRecvPostList + kpostlist];
  struct ibv_recv_wr *bad_recv_wr;

  int empty_start_pos = 0;
  int recv_handle_pos = 0;
  int empty_recv_num = 0;

  dsm->barrier("create_multicast_agent", dsm->getComputeNum());

  while (true) {
    int num_comps =
        ibv_poll_cq(node->recv_cq, kpostlist, wc_buffer + recv_handle_pos);
    assert(num_comps >= 0);
    if (num_comps == 0) continue;

    empty_recv_num += num_comps;

    // multi-thread handling
    for (int i = 0; i < num_comps; i++) {
      int pos = recv_handle_pos + i;
      struct ibv_wc *wc = &wc_buffer[pos];
      assert(wc->status == IBV_WC_SUCCESS && wc->opcode == IBV_WC_RECV);
      // memcpy
      me->handle_recv(node, pos % kMcMaxRecvPostList);
    }
    // me->handle_recv_bulk(node, recv_handle_pos, num_comps);

    recv_handle_pos = RING_ADD(recv_handle_pos, num_comps, kMcMaxRecvPostList);

    // batch post recvs
    if (empty_recv_num >= kpostlist) {
      for (int w_i = 0; w_i < empty_recv_num; w_i++) {
        int pos = RING_ADD(empty_start_pos, w_i, kMcMaxRecvPostList);
        node->recv_wr[pos].next =
            w_i == empty_recv_num - 1
                ? nullptr
                : &node->recv_wr[RING_ADD(pos, 1, kMcMaxPostList)];
      }
      int ret = ibv_post_recv(node->cma_id->qp, &node->recv_wr[empty_start_pos],
                              &bad_recv_wr);
      assert(ret == 0);
      empty_start_pos =
          RING_ADD(empty_start_pos, empty_recv_num, kMcMaxPostList);
      empty_recv_num = 0;
    }
  }
}

}  // namespace multicast
}  // namespace XMD