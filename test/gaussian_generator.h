#include <algorithm>  // for std::clamp
#include <iostream>
#include <random>

class gaussian_key_generator_t {
 public:
  // base model suppose [-1, 1], mean = 0
  // parameter mean = space / 2 stddev = mean*ratio
  gaussian_key_generator_t(double mean, double stddev)
      : dist_(0, stddev / mean), mean_(mean) {
    generator_.seed((uint32_t)0xc70f6907);
  }

  uint64_t next_id() {
    double result = dist_(generator_);
    // generate [-1, 1]
    double clamped_result =
        std::clamp(result, static_cast<double>(-1), static_cast<double>(1));
    // std::cout << clamped_result << "key" <<std::endl;
    double restored_result = mean_ * clamped_result + mean_;
    // std::cout << static_cast<uint64_t> (restored_result) << "res_key" <<
    // std::endl;
    return static_cast<uint64_t>(restored_result);
  }

 private:
  std::normal_distribution<double> dist_;
  std::default_random_engine generator_;
  double mean_;
};