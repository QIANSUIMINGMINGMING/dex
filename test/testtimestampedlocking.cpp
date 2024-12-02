#include <atomic>
#include <random>

#include "Common.h"

constexpr int Locking_Size = 8192;
uint64_t locking_area[Locking_Size]{0};
std::atomic<uint64_t> timestamp_buffer[Locking_Size]{0};
std::atomic_flag spin_lock_buffer[Locking_Size]{0};
std::atomic<uint64_t> optimistic_buffer[Locking_Size]{0};

uint64_t total_tp[MAX_APP_THREAD]{0};
std::atomic<int> one_finish{0};

inline bool is_locked(uint64_t version) { return ((version & 1) == 1); }
inline bool is_lost(uint64_t my_version, uint64_t stored_version) {
  return my_version <= stored_version;
}

// the following API may be reimplemented in node.cuh
inline uint64_t read_lock_or_restart(
    const std::atomic<uint64_t> &version_lock_obsolete, bool &need_restart) {
  uint64_t version = version_lock_obsolete.load();
  if (is_locked(version)) {
    need_restart = true;
  }
  return version;
}

inline void read_unlock_or_restart(
    const std::atomic<uint64_t> &version_lock_obsolete, uint64_t start_read,
    bool &need_restart) {
  // TODO: should we use spinlock to await?
  need_restart = (start_read != version_lock_obsolete.load());
}

inline void upgrade_to_write_lock_or_restart(
    std::atomic<uint64_t> &version_lock_obsolete, uint64_t &version,
    bool &need_restart) {
  // if (version == atomicCAS(&version_lock_obsolete, version, version + 0b10))
  // {
  bool success =
      version_lock_obsolete.compare_exchange_strong(version, version + 1);
  if (success) {
    version = version + 1;
  } else {
    need_restart = true;
  }
}

inline void op_write_unlock(std::atomic<uint64_t> &version_lock_obsolete) {
  version_lock_obsolete.fetch_add(1);
}

inline bool write_lock_or_restart(std::atomic<uint64_t> &version_lock_obsolete,
                                  const uint64_t &my_version,
                                  bool &need_restart) {
  // if (version == atomicCAS(&version_lock_obsolete, version, version + 0b10))
  // {
  uint64_t version_old = version_lock_obsolete.load();
  if (is_lost(my_version, version_old)) {
    return false;
  }
  if (is_locked(version_old)) {
    need_restart = true;
    return false;
  }
  bool success = version_lock_obsolete.compare_exchange_strong(version_old,
                                                               my_version - 1);
  if (success) {
    return true;
  } else {
    need_restart = true;
    return false;
  }
}

inline void write_unlock(std::atomic<uint64_t> &version_lock_obsolete) {
  version_lock_obsolete.fetch_add(1);
}

void spinlock(std::atomic_flag &lock) noexcept {
  while (lock.test_and_set(std::memory_order_acq_rel));
}
void unspinlock(std::atomic_flag &lock) noexcept {
  lock.clear(std::memory_order_release);
}

void timstamped_read(uint64_t idx, uint64_t &value) {
  bool needRestart = false;
restart:
  uint64_t v = read_lock_or_restart(timestamp_buffer[idx], needRestart);
  if (needRestart) {
    needRestart = false;
    goto restart;
  }
  value = locking_area[idx];
  read_unlock_or_restart(timestamp_buffer[idx], v, needRestart);
  if (needRestart) {
    needRestart = false;
    goto restart;
  }
}

void optimistic_read(uint64_t idx, uint64_t &value) {
  uint64_t ts = XMD::myClock::get_ts();
  bool needRestart = false;
restart:
  uint64_t v = read_lock_or_restart(timestamp_buffer[idx], needRestart);
  if (needRestart) {
    needRestart = false;
    goto restart;
  }
  value = locking_area[idx];
  read_unlock_or_restart(timestamp_buffer[idx], v, needRestart);
  if (needRestart) {
    needRestart = false;
    goto restart;
  }
}

void spin_read(uint64_t idx, uint64_t &value) {
//   spinlock(spin_lock_buffer[idx]);
  value = locking_area[idx];
//   unspinlock(spin_lock_buffer[idx]);
}

void timestamped_write(uint64_t idx, uint64_t value) {
  uint64_t ts = XMD::myClock::get_ts();
  bool needRestart = false;
restart:
  bool lock_success =
      write_lock_or_restart(timestamp_buffer[idx], 2*ts, needRestart);
  if (needRestart) {
    assert(!lock_success);
    needRestart = false;
    goto restart;
  }
  if (!lock_success) {
    return;
  }
  locking_area[idx] = value;
  write_unlock(timestamp_buffer[idx]);
}

void optimistic_write(uint64_t idx, uint64_t value) {
  bool needRestart = false;
restart:
  uint64_t v = read_lock_or_restart(timestamp_buffer[idx], needRestart);
  if (needRestart) {
    needRestart = false;
    goto restart;
  }
  upgrade_to_write_lock_or_restart(timestamp_buffer[idx], v, needRestart);
  if (needRestart) {
    needRestart = false;
    goto restart;
  }
  value = locking_area[idx];
  op_write_unlock(timestamp_buffer[idx]);
}

void spin_write(uint64_t idx, uint64_t value) {
//   spinlock(spin_lock_buffer[idx]);
  locking_area[idx] = value;
//   unspinlock(spin_lock_buffer[idx]);
}

int test_num;
enum lock_type { SPIN, OP, TIME, LAST };

void thread_run(int id, lock_type lt) {
  bindCore(id);
  int i = 0;
  auto start = std::chrono::high_resolution_clock::now();
  while (i < test_num && one_finish.load() == 0) {
    bool is_write = rand() % 2 == 0;
    uint64_t idx = rand() % Locking_Size;
    switch (lt) {
      case lock_type::OP: {
        if (is_write) {
          optimistic_write(idx, idx + 1);
        } else {
          uint64_t value;
          optimistic_read(idx, value);
        }
      } break;
      case lock_type::SPIN: {
        if (is_write) {
          spin_write(idx, idx + 1);
        } else {
          uint64_t value;
          spin_read(idx, value);
        }
      } break;
      case lock_type::TIME: {
        if (is_write) {
          timestamped_write(idx, idx + 1);
        } else {
          uint64_t value;
          timstamped_read(idx, value);
        }
      } break;
      default: {
        assert(false);
      }
    }
    i++;
  }
  if (one_finish.load() == 0) {
    one_finish.store(1);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  uint64_t throughput = i / (static_cast<double>(duration) / std::pow(10, 6));
  total_tp[id] = throughput;  // (ops/s)
}

int main() {
    test_num = 100000;

    for (int lt = lock_type::SPIN; lt!= lock_type::LAST; lt++) {
      lock_type foo = static_cast<lock_type>(lt);
      std::thread th[MAX_APP_THREAD];

      for (int i=0; i< MAX_APP_THREAD;i++) {
        th[i] = std::thread(thread_run, i, foo);
      }

      for (int i=0; i< MAX_APP_THREAD;i++) {
        th[i].join();
      }

      uint64_t total_tp_sum = 0;
      for (size_t i = 0; i < MAX_APP_THREAD; i++) {
        total_tp_sum += total_tp[i];
      }
      std::cout << "total " << foo << " throughput: " << total_tp_sum << std::endl;
      one_finish.store(0);
      memset(total_tp, 0, sizeof(uint64_t) * MAX_APP_THREAD);
      memset(locking_area, 0, sizeof(uint64_t) * Locking_Size);
      for (uint64_t i =0; i< Locking_Size;i++) {
        timestamp_buffer[i].store(0);
        spin_lock_buffer[i].clear();
        optimistic_buffer[i].store(0);
      }
    }

    return 0;
}