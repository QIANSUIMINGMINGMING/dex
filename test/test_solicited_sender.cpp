// sender.cpp

#include <infiniband/verbs.h>

#include <cstring>
#include <iostream>
#include <vector>

#define BUFFER_SIZE 1024

int main() {
  struct ibv_context *ctx = nullptr;
  struct ibv_pd *pd = nullptr;
  struct ibv_cq *cq = nullptr;
  struct ibv_qp *qp = nullptr;
  struct ibv_mr *mr = nullptr;
  struct ibv_port_attr port_attr;
  struct ibv_device **dev_list = nullptr;
  struct ibv_device *dev = nullptr;
  int ret = 0;

  // Get the list of devices
  dev_list = ibv_get_device_list(nullptr);
  if (!dev_list) {
    perror("Failed to get IB devices list");
    return 1;
  }
  dev = dev_list[0];
  if (!dev) {
    std::cerr << "No IB devices found\n";
    return 1;
  }

  // Open device context
  ctx = ibv_open_device(dev);
  if (!ctx) {
    perror("Failed to open device context");
    return 1;
  }

  // Allocate Protection Domain (PD)
  pd = ibv_alloc_pd(ctx);
  if (!pd) {
    perror("Failed to allocate PD");
    return 1;
  }

  // Create Completion Queue (CQ)
  cq = ibv_create_cq(ctx, 10, nullptr, nullptr, 0);
  if (!cq) {
    perror("Failed to create CQ");
    return 1;
  }

  // Allocate memory buffer using std::vector
  std::vector<char> buffer(BUFFER_SIZE);
  std::string message = "Hello, RDMA with solicited event!";
  std::copy(message.begin(), message.end(), buffer.begin());
  buffer[message.size()] = '\0';  // Ensure null-termination

  // Register memory region
  mr = ibv_reg_mr(pd, buffer.data(), BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);
  if (!mr) {
    perror("Failed to register MR");
    return 1;
  }

  // Create Queue Pair (QP)
  struct ibv_qp_init_attr qp_init_attr = {};
  qp_init_attr.send_cq = cq;
  qp_init_attr.recv_cq = cq;
  qp_init_attr.cap.max_send_wr = 10;
  qp_init_attr.cap.max_recv_wr = 10;
  qp_init_attr.cap.max_send_sge = 1;
  qp_init_attr.cap.max_recv_sge = 1;
  qp_init_attr.qp_type = IBV_QPT_RC;

  qp = ibv_create_qp(pd, &qp_init_attr);
  if (!qp) {
    perror("Failed to create QP");
    return 1;
  }

  // Transition QP to INIT, RTR, RTS (omitted for brevity)
  // Exchange QP info with receiver (omitted for brevity)

  // Prepare the Send Work Request
  struct ibv_sge sge = {};
  sge.addr = reinterpret_cast<uintptr_t>(buffer.data());
  sge.length = message.size() + 1;
  sge.lkey = mr->lkey;

  struct ibv_send_wr send_wr = {};
  send_wr.wr_id = 0;
  send_wr.sg_list = &sge;
  send_wr.num_sge = 1;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_SOLICITED;

  struct ibv_send_wr *bad_send_wr = nullptr;
  ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
  if (ret) {
    perror("Failed to post send WR");
    return 1;
  }

  // Poll the CQ for completion
  struct ibv_wc wc = {};
  int num_completions = 0;

  do {
    num_completions = ibv_poll_cq(cq, 1, &wc);
    if (num_completions < 0) {
      perror("Failed to poll CQ");
      return 1;
    }
  } while (num_completions == 0);

  if (wc.status != IBV_WC_SUCCESS) {
    std::cerr << "Send completion error: " << ibv_wc_status_str(wc.status)
              << "\n";
    return 1;
  }

  std::cout << "Message sent successfully\n";

  // Cleanup (omitted for brevity)

  return 0;
}