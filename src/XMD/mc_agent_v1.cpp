#include "XMD/mc_agent_v1.h"
#include <unicode/unistr.h>

namespace rdmacm {
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

void printTransferObj(const TransferObj &obj, int numElementsToPrint) {
  printf("TransferObj {\n");
  printf("  node_id: %lu,\n", obj.node_id);
  printf("  psn: %lu,\n", obj.psn);
  printf("  elements:\n");

  // Ensure we don't exceed the array bounds
  size_t elementsToPrint = std::min(numElementsToPrint, kMcCardinality);
  for (size_t i = 0; i < elementsToPrint; ++i) {
    printf("    elements[%lu]: ", i);
    obj.elements[i].self_print();
  }
  if (elementsToPrint < kMcCardinality) {
    printf("    ...\n");
  }
  printf("}\n");
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

multicastCM::multicastCM(DSM *dsm, uint64_t buffer_size, std::string mc_ip)
    : cnode_id(dsm->getMyNodeID()),
      mcIp(mc_ip),
      mcGroups(10),
      mbr(buffer_size * 1024 * 1024 * 1024) {
  connects_left = mcGroups;
  if (alloc_nodes(mcGroups)) exit(1);

  pthread_create(&cmathread, NULL, cma_thread_manager, this);
  // maintainer = std::thread(mc_maintainer, this);
  /*
   * Pause to give SM chance to configure switches.  We don't want to
   * handle reliability issue in this simple test program.
   */
  sleep(5);
  // for (int i = 0; i < kMaxRpcCoreNum; i++)
  // {
  //     pageQueues[i] = new
  //     moodycamel::BlockingReaderWriterCircularBuffer<TransferObj
  //     *>(kMcMaxPostList);
  // }
  maintainer_start_block.store(1);

  for (int i = 0; i < mcGroups; i++) {
    maintainers[i] =
        std::thread(std::bind(&multicastCM::mc_maintainer, i, this));
  }
  sleep(5);

  dsm->barrier("create_multicast_agent", dsm->getComputeNum());
  maintainer_start_block.store(0);
};

multicastCM::~multicastCM() {
  for (int i = 0; i < mcGroups; i++) {
    destroy_node(&nodes[i]);
  }
}

int multicastCM::alloc_nodes(int connections) {
  int ret, i;
  nodes = (multicast_node *)mbr.allocate(sizeof(struct multicast_node) *
                                         connections);
  if (!nodes) {
    fprintf(stderr, "failed to allocate memory for test nodes\n");
    return -ENOMEM;
  }

  // memset(nodes, 0, sizeof(struct multicast_node) * connections);
  for (i = 0; i < connections; i++) {
    nodes[i].id = i;
    nodes[i].dst_addr = (struct sockaddr *)&nodes[i].dst_in;

    std::string nodeMcIp = mcIp;
    std::string::size_type pos = nodeMcIp.find_last_of(".");
    nodeMcIp = nodeMcIp.substr(0, pos + 1);
    nodeMcIp += std::to_string(i + 1);
    ret = get_addr(nodeMcIp, (struct sockaddr *)&nodes[i].dst_in);
    if (ret) {
      fprintf(stderr, "failed to get destination address\n");
      return ret;
    }

    nodes[i].channel = create_first_event_channel();
    if (!nodes[i].channel) {
      fprintf(stderr, "failed to create RDMA CM event channel\n");
      return -1;
    }
    ret = rdma_create_id(nodes[i].channel, &nodes[i].cma_id, &nodes[i],
                         RDMA_PS_UDP);
    if (ret) {
      fprintf(stderr, "failed to create RDMA CM ID\n");
      return ret;
    }
    ret = rdma_resolve_addr(nodes[i].cma_id, NULL, nodes[i].dst_addr, 2000);
    if (ret) {
      perror("mckey: resolve addr failure");
      return ret;
    }

    struct rdma_cm_event *event;

    while (!nodes[i].connected && !ret) {
      ret = rdma_get_cm_event(nodes[i].channel, &event);
      if (!ret) {
        ret = cma_handler(event->id, event);
        rdma_ack_cm_event(event);
      }
    }
  }
  return 0;
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
  cqe = XMD::kMcMaxPostList;
  int recv_cqe = XMD::kMcMaxRecvPostList;
  node->send_cq = ibv_create_cq(node->cma_id->verbs, cqe, NULL, NULL, 0);
  node->recv_cq = ibv_create_cq(node->cma_id->verbs, recv_cqe, NULL, NULL, 0);

  if (!node->send_cq || !node->recv_cq) {
    ret = -ENOMEM;
    printf("mckey: unable to create CQ\n");
    return ret;
  }

  memset(&init_qp_attr_ex, 0, sizeof(init_qp_attr_ex));
  init_qp_attr_ex.cap.max_send_wr = XMD::kMcMaxPostList;
  init_qp_attr_ex.cap.max_recv_wr = XMD::kMcMaxRecvPostList;
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
  init_qp_attr_ex.create_flags = IBV_QP_CREATE_BLOCK_SELF_MCAST_LB;

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
  return 0;
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
  node->send_messages = (uint8_t *)mbr.allocate(XMD::kMcPageSize * XMD::kMcMaxPostList);
  node->recv_messages =
      (uint8_t *)mbr.allocate(XMD::kRecvMcPageSize * XMD::kMcMaxRecvPostList);
  // post initial recvs
  init_recvs(node);
  return 0;
}

// test functions
int multicastCM::poll_scqs(int connections, int message_count) {
  return poll_cqs(connections, message_count, SEND);
}

int multicastCM::poll_rcqs(int connections, int message_count) {
  return poll_cqs(connections, message_count, RECV);
}

int multicastCM::poll_cqs(int connections, int message_count, enum SR sr) {
  struct ibv_wc wc[8];
  int done, i, ret;

  for (i = 0; i < connections; i++) {
    if (!nodes[i].connected) continue;

    struct ibv_cq *cq = sr == SEND ? nodes[i].send_cq : nodes[i].recv_cq;

    for (done = 0; done < message_count; done += ret) {
      ret = ibv_poll_cq(cq, 8, wc);
      if (ret < 0) {
        printf("mckey: failed polling CQ: %d\n", ret);
        return ret;
      }
    }
  }
  return 0;
}

void multicastCM::handle_recv(struct multicast_node *node, int id) {
#if defined(SINGLE_KEY)
#elif defined(KEY_PAGE)
  uint8_t *message =
      node->recv_messages + id * XMD::kRecvMcPageSize + 40;  // ud padding
  TransferObj *page = (TransferObj *)message;
  // record psn
  printf("emplace psn %d\n", page->psn);
  packages.emplace(std::make_pair(page->node_id, page->psn));
  // assert(pageQueues[node->id]->try_enqueue(page));
#elif defined(FILTER_PAGE)
#else
  assert(false);
#endif
}

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
  for (int i = 0; i < XMD::kMcMaxPostList; i++) {
    node->send_wr[i].next = nullptr;
    node->send_wr[i].sg_list = &node->send_sgl[i];
    node->send_wr[i].num_sge = 1;
    node->send_wr[i].opcode = IBV_WR_SEND;
    if (i % (XMD::kMcMaxPostList / 2) == 0) {
      node->send_wr[i].send_flags = IBV_SEND_SIGNALED;
    }
    node->send_wr[i].wr_id = (uint64_t)node;
    node->send_wr[i].wr.ud.ah = node->ah;
    node->send_wr[i].wr.ud.remote_qkey = node->remote_qkey;
    node->send_wr[i].wr.ud.remote_qpn = node->remote_qpn;

    node->send_sgl[i].length = XMD::kMcPageSize;
    node->send_sgl[i].lkey = mr->lkey;
    node->send_sgl[i].addr = (uintptr_t)node->send_messages + i * XMD::kMcPageSize;
  }

  node->connected = 1;
  connects_left--;
  return 0;
}

int multicastCM::init_recvs(struct multicast_node *node) {
  int ret;
  for (int i = 0; i < XMD::kMcMaxRecvPostList; i++) {
    memset(&node->recv_wr[i], 0, sizeof(node->recv_wr[i]));
    node->recv_wr[i].next =
        i == XMD::kMcMaxRecvPostList - 1 ? nullptr : &node->recv_wr[i + 1];
    node->recv_wr[i].sg_list = &node->recv_sgl[i];
    node->recv_wr[i].num_sge = 1;
    node->recv_wr[i].wr_id = (uintptr_t)node;

    memset(&node->recv_sgl[i], 0, sizeof(node->recv_sgl[i]));
    node->recv_sgl[i].length = XMD::kRecvMcPageSize;
    node->recv_sgl[i].lkey = mr->lkey;
    node->recv_sgl[i].addr =
        (uintptr_t)node->recv_messages + i * XMD::kRecvMcPageSize;
  }

  struct ibv_recv_wr *bad_recv_wr;
  ret = ibv_post_recv(node->cma_id->qp, &node->recv_wr[0], &bad_recv_wr);
  if (ret) {
    printf("failed to post receives: %d\n", ret);
  }
  return ret;
}

// test functions
int multicastCM::post_recvs(struct multicast_node *node) {
  struct ibv_recv_wr recv_wr, *recv_failure;
  struct ibv_sge sge;
  int i, ret = 0;

  int message_count = XMD::kMcMaxPostList;

  if (!message_count) return 0;

  recv_wr.next = NULL;
  recv_wr.sg_list = &sge;
  recv_wr.num_sge = 1;
  recv_wr.wr_id = (uintptr_t)node;

  sge.length = XMD::kRecvMcPageSize;
  sge.lkey = mr->lkey;
  sge.addr = (uintptr_t)node->recv_messages;

  for (i = 0; i < message_count && !ret; i++) {
    ret = ibv_post_recv(node->cma_id->qp, &recv_wr, &recv_failure);
    if (ret) {
      printf("failed to post receives: %d\n", ret);
      break;
    }
  }
  return ret;
}

int multicastCM::post_sends(struct multicast_node *node, int signal_flag) {
  struct ibv_send_wr send_wr, *bad_send_wr;
  struct ibv_sge sge;
  int i, ret = 0;

  send_wr.next = NULL;
  send_wr.sg_list = &sge;
  send_wr.num_sge = 1;
  send_wr.opcode = IBV_WR_SEND_WITH_IMM;
  send_wr.send_flags = signal_flag;
  send_wr.wr_id = (unsigned long)node;
  send_wr.imm_data = htobe32(node->cma_id->qp->qp_num);

  send_wr.wr.ud.ah = node->ah;
  send_wr.wr.ud.remote_qpn = node->remote_qpn;
  send_wr.wr.ud.remote_qkey = node->remote_qkey;

  int message_count = XMD::kMcMaxPostList;

  sge.length = XMD::kMcPageSize;
  sge.lkey = mr->lkey;
  sge.addr = (uintptr_t)node->send_messages;

  for (i = 0; i < message_count && !ret; i++) {
    ret = ibv_post_send(node->cma_id->qp, &send_wr, &bad_send_wr);
    if (ret) printf("failed to post sends: %d\n", ret);
  }
  return ret;
}

int multicastCM::get_pos(int tid, TransferObj *&next_message_address) {
  struct multicast_node *node = &nodes[tid];
  int pos = node->send_pos;
  node->send_pos = RING_ADD(node->send_pos, 1, XMD::kMcMaxPostList);
  next_message_address =
      (TransferObj *)(node->send_messages + node->send_pos * XMD::kMcPageSize);
  return pos;
}

void multicastCM::send_message(int tid, int pos) {
  struct multicast_node *node = &nodes[tid];
  struct ibv_send_wr *bad_send_wr;
  ibv_send_wr *send_wr = &node->send_wr[pos];
  ibv_sge *sge = &node->send_sgl[pos];
  if (pos % (XMD::kMcMaxPostList / 2) == 1) {
    struct ibv_wc wc;
    pollWithCQ(node->send_cq, 1, &wc);
  }
  sge->addr = (uint64_t)node->send_messages + pos * XMD::kMcPageSize;
  int ret = ibv_post_send(node->cma_id->qp, send_wr, &bad_send_wr);
  if (ret) {
    printf("failed to post sends: %d\n", ret);
  }
}

int multicastCM::resolve_nodes() {
  int i, ret;

  for (i = 0; i < mcGroups; i++) {
    ret = rdma_resolve_addr(nodes[i].cma_id, NULL, nodes[i].dst_addr, 2000);
    if (ret) {
      perror("mckey: resolve addr failure");
      return ret;
    }
  }
  return 0;
}

void *multicastCM::cma_thread_worker(void *arg) {
  bindCore(XMD::mcCmaCore);
  struct rdma_cm_event *event;
  int ret;

  struct multicast_node *node = static_cast<multicast_node *>(arg);
  printf("mckey: worker %d, bind core is %d\n ", node->id, XMD::mcCmaCore);
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

void *multicastCM::mc_maintainer(uint16_t id, multicastCM *me) {
  // int id = (*(static_cast<int16_t *>(args[0])));
  bindCore(XMD::rpcCore - id);
  printf("mckey: maintainer %d, using core %d\n", id, XMD::rpcCore - id);

  // multicastCM *me = static_cast<multicastCM *>(args[1]);
  multicast_node *node = &me->nodes[id];
  struct ibv_wc wc_buffer[XMD::kMcMaxRecvPostList + XMD::kpostlist];
  struct ibv_recv_wr *bad_recv_wr;

  int empty_start_pos = 0;
  int recv_handle_pos = 0;
  int empty_recv_num = 0;

  while (me->maintainer_start_block.load() == 1) {}

  while (true) {
    int num_comps =
        ibv_poll_cq(node->recv_cq, XMD::kpostlist, wc_buffer + recv_handle_pos);
    assert(num_comps >= 0);
    if (num_comps == 0) continue;

    empty_recv_num += num_comps;

    // multi-thread handling
    for (int i = 0; i < num_comps; i++) {
      int pos = recv_handle_pos + i;
      struct ibv_wc *wc = &wc_buffer[pos];
      assert(wc->status == IBV_WC_SUCCESS && wc->opcode == IBV_WC_RECV);
      // memcpy
      me->handle_recv(node, pos % XMD::kMcMaxPostList);
    }

    recv_handle_pos = RING_ADD(recv_handle_pos, num_comps, XMD::kMcMaxPostList);

    // batch post recvs
    if (empty_recv_num >= XMD::kpostlist) {
      for (int w_i = 0; w_i < empty_recv_num; w_i++) {
        int pos = RING_ADD(empty_start_pos, w_i, XMD::kMcMaxPostList);
        node->recv_wr[pos].next =
            w_i == empty_recv_num - 1
                ? nullptr
                : &node->recv_wr[RING_ADD(pos, 1, XMD::kMcMaxPostList)];
      }
      int ret = ibv_post_recv(node->cma_id->qp, &node->recv_wr[empty_start_pos],
                              &bad_recv_wr);
      assert(ret == 0);
      empty_start_pos =
          RING_ADD(empty_start_pos, empty_recv_num, XMD::kMcMaxPostList);
      empty_recv_num = 0;
    }
  }
}

void *multicastCM::cma_thread_manager(void *arg) {
  bindCore(XMD::mcCmaCore);
  multicastCM *mckey = static_cast<multicastCM *>(arg);
  pthread_t workers[mckey->getGroupSize()];
  printf("mckey: manager, using core %d, there are %d workers \n", XMD::mcCmaCore,
         mckey->getGroupSize());

  for (int i = 0; i < mckey->getGroupSize(); i++) {
    // create worker threads for each group
    int ret;
    ret =
        pthread_create(&workers[i], NULL, cma_thread_worker, mckey->getNode(i));
    if (ret) {
      perror("mckey: failed to create worker thread");
      return NULL;
    }
  }

  for (int i = 0; i < mckey->getGroupSize(); i++) {
    // join worker threads for each group
    int ret;
    ret = pthread_join(workers[i], NULL);
    if (ret) {
      perror("mckey: failed to join worker thread");
      return NULL;
    }
  }

  return NULL;
}

int multicastCM::test_node() {
  int i, ret;
  int message_count = 10;

  printf("mckey: starting %s\n");

  printf("mckey: joining\n");
  // ret = resolve_nodes();
  // if (ret) {
  //     perror("mckey: resolve nodes failure");
  //     return ret;
  // }

  // ret = connect_events(&nodes[0]);
  // if (ret) {
  //     perror("mckey: connect events failure");
  //     return ret;
  // }

  // pthread_create(&cmathread, NULL, cma_thread_manager, this);

  // /*
  // * Pause to give SM chance to configure switches.  We don't want to
  // * handle reliability issue in this simple test program.
  // */
  // sleep(5);

  if (message_count) {
    if (true) {
      printf("initiating data transfers\n");
      for (i = 0; i < mcGroups; i++) {
        ret = post_sends(&nodes[i], 0);
        if (ret) {
          perror("mckey: failure sending");
          return ret;
        }
      }
    } else {
      printf("receiving data transfers\n");
      ret = poll_rcqs(mcGroups, message_count);
      if (ret) {
        perror("mckey: failure receiving");
        return ret;
      }
    }
    printf("data transfers complete\n");
  }

  return 0;
}

}  // namespace multicast
}  // namespace rdmacm
