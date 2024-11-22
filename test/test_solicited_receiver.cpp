// receiver.cpp

#include <infiniband/verbs.h>

#include <cstring>
#include <iostream>
#include <vector>

#define BUFFER_SIZE 1024

int main() {
  struct ibv_context *ctx = nullptr;
  struct ibv_pd *pd = nullptr;
  struct ibv_cq *cq = nullptr;
  struct ibv_comp_channel *comp_channel = nullptr;
  struct ibv_qp *qp = nullptr;
  struct ibv_mr *mr = nullptr;
  struct ibv_port_attr port_attr;
  struct ibv_device **dev_list = nullptr;
  struct ibv_device *dev = nullptr;
  int ret = 0;

  // Get the list of devices
  int devicesNum;
  dev_list = ibv_get_device_list(&devicesNum);
  if (!dev_list) {
    perror("Failed to get IB devices list");
    return 1;
  }
  int devIndex = -1;
  for (int i = 0; i < devicesNum; ++i) {
    // printf("Device %d: %s\n", i, ibv_get_device_name(deviceList[i]));
    if (ibv_get_device_name(dev_list[i])[5] == '2') {
      devIndex = i;
      break;
    }
  }
  dev = dev_list[devIndex];
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

  // Create Completion Channel
  comp_channel = ibv_create_comp_channel(ctx);
  if (!comp_channel) {
    perror("Failed to create completion channel");
    return 1;
  }

  // Create Completion Queue (CQ)
  cq = ibv_create_cq(ctx, 10, nullptr, comp_channel, 0);
  if (!cq) {
    perror("Failed to create CQ");
    return 1;
  }

  // Request CQ notification
  ret = ibv_req_notify_cq(cq, 0);
  if (ret) {
    perror("Failed to request CQ notification");
    return 1;
  }

  // Allocate memory buffer using std::vector
  std::vector<char> buffer(BUFFER_SIZE, 0);

  // Register memory region
  mr = ibv_reg_mr(pd, buffer.data(), BUFFER_SIZE,
                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
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
  // Exchange QP info with sender (omitted for brevity)

  // Post a Receive Work Request
  struct ibv_sge sge = {};
  sge.addr = reinterpret_cast<uintptr_t>(buffer.data());
  sge.length = BUFFER_SIZE;
  sge.lkey = mr->lkey;

  struct ibv_recv_wr recv_wr = {};
  recv_wr.wr_id = 0;
  recv_wr.sg_list = &sge;
  recv_wr.num_sge = 1;

  struct ibv_recv_wr *bad_recv_wr = nullptr;
  ret = ibv_post_recv(qp, &recv_wr, &bad_recv_wr);
  if (ret) {
    perror("Failed to post receive WR");
    return 1;
  }

  // Event loop to handle completions
  while (true) {
    struct ibv_cq *ev_cq = nullptr;
    void *ev_ctx = nullptr;

    // Wait for CQ event
    ret = ibv_get_cq_event(comp_channel, &ev_cq, &ev_ctx);
    if (ret) {
      perror("Failed to get CQ event");
      return 1;
    }

    // Acknowledge the event
    ibv_ack_cq_events(ev_cq, 1);

    // Request notification for the next event
    ret = ibv_req_notify_cq(cq, 0);
    if (ret) {
      perror("Failed to request CQ notification");
      return 1;
    }

    // Poll the CQ
    struct ibv_wc wc = {};
    int num_completions = 0;

    do {
      num_completions = ibv_poll_cq(cq, 1, &wc);
      if (num_completions < 0) {
        perror("Failed to poll CQ");
        return 1;
      } else if (num_completions > 0) {
        if (wc.status == IBV_WC_SUCCESS) {
          if (wc.opcode == IBV_WC_RECV) {
            std::cout << "Received message: " << buffer.data() << "\n";
          } else {
            std::cout << "Received unexpected opcode: " << wc.opcode << "\n";
          }
        } else {
          std::cerr << "Completion with error: " << ibv_wc_status_str(wc.status)
                    << "\n";
        }
      }
    } while (num_completions);

    // Re-post the receive WR if necessary (omitted for brevity)
  }

  // Cleanup (omitted for brevity)

  return 0;
}