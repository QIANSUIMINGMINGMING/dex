#include "Common.h"

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static int sched_setattr(pid_t pid, const struct sched_attr *attr,
                         unsigned int flags) {
  return syscall(SYS_sched_setattr, pid, attr, flags);
}

// 实现sched_getattr函数（可选，用于调试）
static int sched_getattr(pid_t pid, struct sched_attr *attr, unsigned int size,
                         unsigned int flags) {
  return syscall(SYS_sched_getattr, pid, attr, size, flags);
}

int set_sched_deadline(pid_t pid, uint64_t runtime_ns, uint64_t deadline_ns,
                       uint64_t period_ns) {
  struct sched_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.size = sizeof(attr);
  attr.sched_policy = SCHED_DEADLINE;
  attr.sched_flags = 0;
  attr.sched_runtime = runtime_ns;
  attr.sched_deadline = deadline_ns;
  attr.sched_period = period_ns;

  return sched_setattr(pid, &attr, 0);
}

void bindCore(uint16_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    Debug::notifyError("can't bind core!");
  }
}

char *getIP() {
  struct ifreq ifr;
  int fd = socket(AF_INET, SOCK_DGRAM, 0);

  ifr.ifr_addr.sa_family = AF_INET;
  strncpy(ifr.ifr_name, "ib0", IFNAMSIZ - 1);

  ioctl(fd, SIOCGIFADDR, &ifr);
  close(fd);

  return inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr);
}

char *getMac() {
  static struct ifreq ifr;
  int fd = socket(AF_INET, SOCK_DGRAM, 0);

  ifr.ifr_addr.sa_family = AF_INET;
  strncpy(ifr.ifr_name, "ens2", IFNAMSIZ - 1);

  ioctl(fd, SIOCGIFHWADDR, &ifr);
  close(fd);

  return (char *)ifr.ifr_hwaddr.sa_data;
}

namespace XMD {
void yield(int count) {
  if (count > 3)
    sched_yield();
  else
    _mm_pause();
}
}