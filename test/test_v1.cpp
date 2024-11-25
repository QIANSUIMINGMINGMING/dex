#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <infiniband/ib.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

struct rdma_event_channel *create_first_event_channel(void) {
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

struct test_addr {
  struct sockaddr_storage dst_in;
  struct sockaddr *dst_addr;
  struct sockaddr_storage src_in;
  struct sockaddr *src_addr;
};

struct cmatest_node {
  int id;
  struct rdma_cm_id *cma_id;
  int connected;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_mr *mr;
  struct ibv_ah *ah;
  uint32_t remote_qpn;
  uint32_t remote_qkey;
  struct test_addr *myaddr_;
  void *mem;
};

struct cmatest {
  struct rdma_event_channel *channel;
  pthread_t cmathread;
  struct cmatest_node *nodes;
  int conn_index;
  int connects_left;

  struct test_addr *addrs;
};

static struct cmatest test;
static int connections = 2;
static int message_size = 100;
static int message_count = 10;
static int is_sender;
static int send_only;
static int loopback = 1;
static int unmapped_addr;
static char *dst_addr;
static char *src_addr;
static enum rdma_port_space port_space = RDMA_PS_UDP;

static int create_message(struct cmatest_node *node) {
  if (!message_size) message_count = 0;

  if (!message_count) return 0;

  node->mem = malloc(message_size + sizeof(struct ibv_grh));
  if (!node->mem) {
    printf("failed message allocation\n");
    return -1;
  }
  node->mr =
      ibv_reg_mr(node->pd, node->mem, message_size + sizeof(struct ibv_grh),
                 IBV_ACCESS_LOCAL_WRITE);
  if (!node->mr) {
    printf("failed to reg MR\n");
    goto err;
  }
  return 0;
err:
  free(node->mem);
  return -1;
}

static int verify_test_params(struct cmatest_node *node) {
  struct ibv_port_attr port_attr;
  int ret;

  ret = ibv_query_port(node->cma_id->verbs, node->cma_id->port_num, &port_attr);
  if (ret) return ret;

  if (message_count && message_size > (1 << (port_attr.active_mtu + 7))) {
    printf("mckey: message_size %d is larger than active mtu %d\n",
           message_size, 1 << (port_attr.active_mtu + 7));
    return -EINVAL;
  }

  return 0;
}

static int init_node(struct cmatest_node *node) {
  struct ibv_qp_init_attr_ex init_qp_attr_ex;
  struct ibv_qp_init_attr init_qp_attr;
  int cqe, ret = 0;

  node->pd = ibv_alloc_pd(node->cma_id->verbs);
  if (!node->pd) {
    ret = -ENOMEM;
    printf("mckey: unable to allocate PD\n");
    goto out;
  }

  cqe = message_count ? message_count * 2 : 2;
  node->cq = ibv_create_cq(node->cma_id->verbs, cqe, node, NULL, 0);
  if (!node->cq) {
    ret = -ENOMEM;
    printf("mckey: unable to create CQ\n");
    goto out;
  }

  memset(&init_qp_attr, 0, sizeof init_qp_attr);
  init_qp_attr.cap.max_send_wr = message_count ? message_count : 1;
  init_qp_attr.cap.max_recv_wr = message_count ? message_count : 1;
  init_qp_attr.cap.max_send_sge = 1;
  init_qp_attr.cap.max_recv_sge = 1;
  init_qp_attr.qp_context = node;
  init_qp_attr.sq_sig_all = 0;
  init_qp_attr.qp_type = IBV_QPT_UD;
  init_qp_attr.send_cq = node->cq;
  init_qp_attr.recv_cq = node->cq;

  if (!loopback) {
    memset(&init_qp_attr_ex, 0, sizeof(init_qp_attr_ex));
    init_qp_attr_ex.cap.max_send_wr = message_count ? message_count : 1;
    init_qp_attr_ex.cap.max_recv_wr = message_count ? message_count : 1;
    init_qp_attr_ex.cap.max_send_sge = 1;
    init_qp_attr_ex.cap.max_recv_sge = 1;
    init_qp_attr_ex.qp_context = node;
    init_qp_attr_ex.sq_sig_all = 0;
    init_qp_attr_ex.qp_type = IBV_QPT_UD;
    init_qp_attr_ex.send_cq = node->cq;
    init_qp_attr_ex.recv_cq = node->cq;

    init_qp_attr_ex.comp_mask =
        IBV_QP_INIT_ATTR_CREATE_FLAGS | IBV_QP_INIT_ATTR_PD;
    init_qp_attr_ex.pd = node->pd;
    init_qp_attr_ex.create_flags = IBV_QP_CREATE_BLOCK_SELF_MCAST_LB;

    ret = rdma_create_qp_ex(node->cma_id, &init_qp_attr_ex);
  } else {
    ret = rdma_create_qp(node->cma_id, node->pd, &init_qp_attr);
  }

  if (ret) {
    perror("mckey: unable to create QP");
    goto out;
  }

  ret = create_message(node);
  if (ret) {
    printf("mckey: failed to create messages: %d\n", ret);
    goto out;
  }
out:
  return ret;
}

static int post_recvs(struct cmatest_node *node) {
  struct ibv_recv_wr recv_wr, *recv_failure;
  struct ibv_sge sge;
  int i, ret = 0;

  if (!message_count) return 0;

  recv_wr.next = NULL;
  recv_wr.sg_list = &sge;
  recv_wr.num_sge = 1;
  recv_wr.wr_id = (uintptr_t)node;

  sge.length = message_size + sizeof(struct ibv_grh);
  sge.lkey = node->mr->lkey;
  sge.addr = (uintptr_t)node->mem;

  for (i = 0; i < message_count && !ret; i++) {
    ret = ibv_post_recv(node->cma_id->qp, &recv_wr, &recv_failure);
    if (ret) {
      printf("failed to post receives: %d\n", ret);
      break;
    }
  }
  return ret;
}

static int post_sends(struct cmatest_node *node, int signal_flag) {
  struct ibv_send_wr send_wr, *bad_send_wr;
  struct ibv_sge sge;
  int i, ret = 0;

  if (!node->connected || !message_count) return 0;

  send_wr.next = NULL;
  send_wr.sg_list = &sge;
  send_wr.num_sge = 1;
  send_wr.opcode = IBV_WR_SEND_WITH_IMM;
  send_wr.send_flags = signal_flag;
  send_wr.wr_id = (unsigned long)node;
  send_wr.imm_data = htobe32(node->id);

  send_wr.wr.ud.ah = node->ah;
  send_wr.wr.ud.remote_qpn = node->remote_qpn;
  send_wr.wr.ud.remote_qkey = node->remote_qkey;

  sge.length = message_size;
  sge.lkey = node->mr->lkey;
  sge.addr = (uintptr_t)node->mem;

  for (i = 0; i < message_count && !ret; i++) {
    ret = ibv_post_send(node->cma_id->qp, &send_wr, &bad_send_wr);
    if (ret) printf("failed to post sends: %d\n", ret);
  }
  return ret;
}

static void connect_error(void) { test.connects_left--; }

static int addr_handler(struct cmatest_node *node) {
  int ret;
  struct rdma_cm_join_mc_attr_ex mc_attr;

  ret = verify_test_params(node);
  if (ret) goto err;

  ret = init_node(node);
  if (ret) goto err;

  if (!is_sender) {
    ret = post_recvs(node);
    if (ret) goto err;
  }

  mc_attr.comp_mask =
      RDMA_CM_JOIN_MC_ATTR_ADDRESS | RDMA_CM_JOIN_MC_ATTR_JOIN_FLAGS;
  // mc_attr.addr = test.dst_addr;
  mc_attr.addr = node->myaddr_->dst_addr;
  mc_attr.join_flags = send_only ? RDMA_MC_JOIN_FLAG_SENDONLY_FULLMEMBER
                                 : RDMA_MC_JOIN_FLAG_FULLMEMBER;

  ret = rdma_join_multicast_ex(node->cma_id, &mc_attr, node);

  if (ret) {
    perror("mckey: failure joining");
    goto err;
  }
  return 0;
err:
  connect_error();
  return ret;
}

static int join_handler(struct cmatest_node *node,
                        struct rdma_ud_param *param) {
  char buf[40];

  inet_ntop(AF_INET6, param->ah_attr.grh.dgid.raw, buf, 40);
  printf("mckey: joined dgid: %s mlid 0x%x sl %d\n", buf, param->ah_attr.dlid,
         param->ah_attr.sl);

  node->remote_qpn = param->qp_num;
  node->remote_qkey = param->qkey;
  node->ah = ibv_create_ah(node->pd, &param->ah_attr);
  if (!node->ah) {
    printf("mckey: failure creating address handle\n");
    goto err;
  }

  node->connected = 1;
  test.connects_left--;
  return 0;
err:
  connect_error();
  return -1;
}

static int cma_handler(struct rdma_cm_id *cma_id, struct rdma_cm_event *event) {
  int ret = 0;

  cmatest_node *c_node = static_cast<cmatest_node *>(cma_id->context);
  switch (event->event) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
      ret = addr_handler(c_node);
      break;
    case RDMA_CM_EVENT_MULTICAST_JOIN:
      ret = join_handler(c_node, &event->param.ud);
      break;
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_MULTICAST_ERROR:
      printf("mckey: event: %s, error: %d\n", rdma_event_str(event->event),
             event->status);
      connect_error();
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

static void *cma_thread(void *arg) {
  struct rdma_cm_event *event;
  int ret;

  while (1) {
    ret = rdma_get_cm_event(test.channel, &event);
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

static void destroy_node(struct cmatest_node *node) {
  if (!node->cma_id) return;

  if (node->ah) ibv_destroy_ah(node->ah);

  if (node->cma_id->qp) rdma_destroy_qp(node->cma_id);

  if (node->cq) ibv_destroy_cq(node->cq);

  if (node->mem) {
    ibv_dereg_mr(node->mr);
    free(node->mem);
  }

  if (node->pd) ibv_dealloc_pd(node->pd);

  /* Destroy the RDMA ID after all device resources */
  rdma_destroy_id(node->cma_id);
}

static int alloc_nodes(void) {
  int ret, i;

  test.nodes = (cmatest_node *)malloc(sizeof *test.nodes * connections);
  if (!test.nodes) {
    printf("mckey: unable to allocate memory for test nodes\n");
    return -ENOMEM;
  }
  memset(test.nodes, 0, sizeof *test.nodes * connections);

  for (i = 0; i < connections; i++) {
    test.nodes[i].id = i;
    test.nodes[i].myaddr_ = &test.addrs[i];
    ret = rdma_create_id(test.channel, &test.nodes[i].cma_id, &test.nodes[i],
                         port_space);
    if (ret) goto err;
  }
  return 0;
err:
  while (--i >= 0) rdma_destroy_id(test.nodes[i].cma_id);
  free(test.nodes);
  return ret;
}

static void destroy_nodes(void) {
  int i;

  for (i = 0; i < connections; i++) destroy_node(&test.nodes[i]);
  free(test.nodes);
}

static int poll_cqs(void) {
  struct ibv_wc wc[8];
  int done, i, ret;

  for (i = 0; i < connections; i++) {
    if (!test.nodes[i].connected) continue;

    for (done = 0; done < message_count; done += ret) {
      ret = ibv_poll_cq(test.nodes[i].cq, 8, wc);
      if (ret < 0) {
        printf("mckey: failed polling CQ: %d\n", ret);
        return ret;
      }
    }

    printf("connection: %d received\n");
  }
  return 0;
}

static int connect_events(void) {
  struct rdma_cm_event *event;
  int ret = 0;

  while (test.connects_left && !ret) {
    ret = rdma_get_cm_event(test.channel, &event);
    if (!ret) {
      ret = cma_handler(event->id, event);
      rdma_ack_cm_event(event);
    }
  }
  return ret;
}

static int get_addr(char *dst, struct sockaddr *addr) {
  struct addrinfo *res;
  int ret;

  ret = getaddrinfo(dst, NULL, NULL, &res);
  if (ret) {
    printf("getaddrinfo failed (%s) - invalid hostname or IP address\n",
           gai_strerror(ret));
    return ret;
  }

  memcpy(addr, res->ai_addr, res->ai_addrlen);

  freeaddrinfo(res);
  return ret;
}

static int get_dst_addr(char *dst, struct sockaddr *addr) {
  struct sockaddr_ib *sib;

  if (!unmapped_addr) return get_addr(dst, addr);

  sib = (struct sockaddr_ib *)addr;
  memset(sib, 0, sizeof *sib);
  sib->sib_family = AF_IB;
  inet_pton(AF_INET6, dst, &sib->sib_addr);
  return 0;
}

static int run(void) {
  int i, ret;

  printf("mckey: starting %s\n", is_sender ? "client" : "server");
  int octet1, octet2, octet3, octet4;
  if (src_addr) {
    // ret = get_addr(src_addr, (struct sockaddr *)&test.src_in);
    if (sscanf(src_addr, "%d.%d.%d.%d", &octet1, &octet2, &octet3, &octet4) !=
        4) {
      fprintf(stderr, "Invalid src IP address format.\n");
      exit(EXIT_FAILURE);
    }

    for (int i = octet4; i < connections + octet4; i++) {
      char *new_ip = (char *)malloc(16 * sizeof(char));
      sprintf(new_ip, "%d.%d.%d.%d", octet1, octet2, octet3, i);
      printf("New IP: %s\n", new_ip);
      ret = get_addr(new_ip, (struct sockaddr *)&test.addrs[i - octet4].src_in);
    }

    if (ret) return ret;
  }

  if (sscanf(dst_addr, "%d.%d.%d.%d", &octet1, &octet2, &octet3, &octet4) !=
      4) {
    fprintf(stderr, "Invalid dst IP address format.\n");
    exit(EXIT_FAILURE);
  }
  //ret = get_dst_addr(dst_addr, (struct sockaddr *)&test.dst_in);
  for (int i = octet4; i < connections + octet4; i++) {
    char *new_ip = (char *)malloc(16 * sizeof(char));
    sprintf(new_ip, "%d.%d.%d.%d", octet1, octet2, octet3, i);
    printf("New IP: %s\n", new_ip);
    ret = get_addr(new_ip, (struct sockaddr *)&test.addrs[i - octet4].dst_in);
  }

  if (ret) return ret;

  printf("mckey: joining\n");
  for (i = 0; i < connections; i++) {
    if (src_addr) {
      ret = rdma_bind_addr(test.nodes[i].cma_id, test.addrs[i].src_addr);
      if (ret) {
        perror("mckey: addr bind failure");
        connect_error();
        return ret;
      }
    }

    if (unmapped_addr)
      ret = addr_handler(&test.nodes[i]);
    else
      ret = rdma_resolve_addr(test.nodes[i].cma_id, test.addrs[i].src_addr,
                              test.addrs[i].dst_addr, 2000);
    if (ret) {
      perror("mckey: resolve addr failure");
      connect_error();
      return ret;
    }
  }

  ret = connect_events();
  if (ret) goto out;

  pthread_create(&test.cmathread, NULL, cma_thread, NULL);

  /*
   * Pause to give SM chance to configure switches.  We don't want to
   * handle reliability issue in this simple test program.
   */
  sleep(3);

  if (message_count) {
    if (is_sender) {
      printf("initiating data transfers\n");
      for (i = 0; i < connections; i++) {
        ret = post_sends(&test.nodes[i], 0);
        if (ret) goto out;
      }
    } else {
      printf("receiving data transfers\n");
      ret = poll_cqs();
      if (ret) goto out;
    }
    printf("data transfers complete\n");
  }
out:
  for (i = 0; i < connections; i++) {
    ret = rdma_leave_multicast(test.nodes[i].cma_id, test.addrs[i].dst_addr);
    if (ret) perror("mckey: failure leaving");
  }
  return ret;
}

int main(int argc, char **argv) {
  int op, ret;

  while ((op = getopt(argc, argv, "m:M:sb:c:C:S:p:ol")) != -1) {
    switch (op) {
      case 'm':
        dst_addr = optarg;
        break;
      case 'M':
        unmapped_addr = 1;
        dst_addr = optarg;
        break;
      case 's':
        is_sender = 1;
        break;
      case 'b':
        src_addr = optarg;
        // test.src_addr = (struct sockaddr *)&test.src_in;
        break;
      case 'c':
        connections = atoi(optarg);
        break;
      case 'C':
        message_count = atoi(optarg);
        break;
      case 'S':
        message_size = atoi(optarg);
        break;
      case 'p':
        port_space = static_cast<rdma_port_space>(strtol(optarg, NULL, 0));
        break;
      case 'o':
        send_only = 1;
        break;
      case 'l':
        loopback = 0;
        break;

      default:
        printf("usage: %s\n", argv[0]);
        printf("\t-m multicast_address\n");
        printf(
            "\t[-M unmapped_multicast_address]\n"
            "\t replaces -m and requires -b\n");
        printf("\t[-s(ender)]\n");
        printf("\t[-b bind_address]\n");
        printf("\t[-c connections]\n");
        printf("\t[-C message_count]\n");
        printf("\t[-S message_size]\n");
        printf(
            "\t[-p port_space - %#x for UDP (default), "
            "%#x for IPOIB]\n",
            RDMA_PS_UDP, RDMA_PS_IPOIB);
        printf("\t[-o join as a send-only full-member]\n");
        printf("\t[-l join without multicast loopback]\n");
        exit(1);
    }
  }

  if (unmapped_addr && !src_addr) {
    printf(
        "unmapped multicast address requires binding "
        "to source address\n");
    exit(1);
  }

  test.addrs = (test_addr *)malloc(sizeof(struct test_addr) * connections);

  for (int i = 0; i < connections; i++) {
    test.addrs[i].dst_addr = (struct sockaddr *)&test.addrs[i].dst_in;
  }

  for (int i = 0; i < connections; i++) {
    test.addrs[i].src_addr = (struct sockaddr *)&test.addrs[i].src_in;
  }

  // test.dst_addr = (struct sockaddr *)&test.dst_in;
  test.connects_left = connections;

  test.channel = create_first_event_channel();
  if (!test.channel) {
    exit(1);
  }

  if (alloc_nodes()) exit(1);

  ret = run();

  printf("test complete\n");
  destroy_nodes();
  rdma_destroy_event_channel(test.channel);

  printf("return status %d\n", ret);
  return ret;
}