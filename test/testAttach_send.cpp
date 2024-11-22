#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MC_QKEY 0x11111111
#define MC_LID 0xc001   // Multicast LID (example)
#define MC_GID_INDEX 0  // GID index (usually 0)

int main() {
  struct ibv_device **dev_list;
  struct ibv_device *ib_dev;
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_qp *qp;
  struct ibv_ah *ah;
  struct ibv_port_attr port_attr;
  struct ibv_qp_init_attr qp_init_attr = {0};
  struct ibv_qp_attr qp_attr;
  struct ibv_ah_attr ah_attr;
  struct ibv_sge sge;
  struct ibv_send_wr wr = {0}, *bad_wr = NULL;
  struct ibv_mr *mr;
  char *buf;
  int ret;

  // Get device list
  dev_list = ibv_get_device_list(NULL);
  if (!dev_list) {
    perror("Failed to get IB devices list");
    return -1;
  }

  ib_dev = dev_list[0];
  if (!ib_dev) {
    fprintf(stderr, "No IB devices found\n");
    return -1;
  }

  // Open device context
  ctx = ibv_open_device(ib_dev);
  if (!ctx) {
    perror("Couldn't open device");
    return -1;
  }

  // Allocate Protection Domain
  pd = ibv_alloc_pd(ctx);
  if (!pd) {
    perror("Couldn't allocate PD");
    return -1;
  }

  // Create Completion Queue
  cq = ibv_create_cq(ctx, 10, NULL, NULL, 0);
  if (!cq) {
    perror("Couldn't create CQ");
    return -1;
  }

  // Create Queue Pair
  qp_init_attr.qp_type = IBV_QPT_UD;
  qp_init_attr.send_cq = cq;
  qp_init_attr.recv_cq = cq;
  qp_init_attr.cap.max_send_wr = 10;
  qp_init_attr.cap.max_recv_wr = 10;
  qp_init_attr.cap.max_send_sge = 1;
  qp_init_attr.cap.max_recv_sge = 1;

  qp = ibv_create_qp(pd, &qp_init_attr);
  if (!qp) {
    perror("Couldn't create QP");
    return -1;
  }

  // Initialize QP
  ret = ibv_query_port(ctx, 1, &port_attr);
  if (ret) {
    perror("Couldn't query port");
    return -1;
  }

  qp_attr.qp_state = IBV_QPS_INIT;
  qp_attr.pkey_index = 0;
  qp_attr.port_num = 1;
  qp_attr.qkey = MC_QKEY;

  ret = ibv_modify_qp(
      qp, &qp_attr,
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY);
  if (ret) {
    perror("Failed to modify QP to INIT");
    return -1;
  }

  // Transition QP to RTR
  qp_attr.qp_state = IBV_QPS_RTR;

  ret = ibv_modify_qp(qp, &qp_attr, IBV_QP_STATE);
  if (ret) {
    perror("Failed to modify QP to RTR");
    return -1;
  }

  // Transition QP to RTS
  qp_attr.qp_state = IBV_QPS_RTS;
  qp_attr.sq_psn = 0;

  ret = ibv_modify_qp(qp, &qp_attr, IBV_QP_STATE | IBV_QP_SQ_PSN);
  if (ret) {
    perror("Failed to modify QP to RTS");
    return -1;
  }

  // Prepare multicast address handle
  ah_attr.is_global = 0;
  ah_attr.dlid = MC_LID;
  ah_attr.sl = 0;
  ah_attr.src_path_bits = 0;
  ah_attr.port_num = 1;

  ah = ibv_create_ah(pd, &ah_attr);
  if (!ah) {
    perror("Couldn't create AH");
    return -1;
  }

  // Allocate and register memory buffer
  buf = (char *)malloc(1024);
  strcpy(buf, "Hello RDMA Multicast!");
  mr = ibv_reg_mr(pd, buf, 1024, IBV_ACCESS_LOCAL_WRITE);
  if (!mr) {
    perror("Couldn't register MR");
    return -1;
  }

  // Prepare scatter/gather entry
  sge.addr = (uintptr_t)buf;
  sge.length = strlen(buf) + 1;
  sge.lkey = mr->lkey;

  // Prepare send work request
  wr.wr_id = 1;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.ud.ah = ah;
  wr.wr.ud.remote_qpn = 0xFFFFFF;  // Multicast QPN
  wr.wr.ud.remote_qkey = MC_QKEY;

  // Post send work request
  ret = ibv_post_send(qp, &wr, &bad_wr);
  if (ret) {
    perror("Failed to post send");
    return -1;
  }

  // Poll for completion
  struct ibv_wc wc;
  do {
    ret = ibv_poll_cq(cq, 1, &wc);
  } while (ret == 0);

  if (wc.status != IBV_WC_SUCCESS) {
    fprintf(stderr, "Failed status %s (%d)\n", ibv_wc_status_str(wc.status),
            wc.status);
    return -1;
  }

  printf("Message sent successfully\n");

  // Cleanup
  ibv_destroy_ah(ah);
  ibv_dereg_mr(mr);
  ibv_destroy_qp(qp);
  ibv_destroy_cq(cq);
  ibv_dealloc_pd(pd);
  ibv_close_device(ctx);
  ibv_free_device_list(dev_list);
  free(buf);

  return 0;
}