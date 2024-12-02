#include <cstdint>
#include <iostream>
#include <string>

uint64_t convertToUint64(const std::string& input) {
  uint64_t result = 0;

  // Split the string into the numeric and character part
  size_t spacePos = input.find(' ');
  if (spacePos == std::string::npos) {
    throw std::invalid_argument("Invalid input format");
  }

  // Parse the number
  uint64_t number = std::stoull(input.substr(0, spacePos));

  // Parse the character (skip the space and quote)
  char character =
      input[spacePos + 2];  // Assuming fixed format "number 'char'"

  // Combine the number and character into uint64_t
  result = (number << 8) | static_cast<uint8_t>(character);

  return result;
}

int main() {
  std::string input = "103 'g'";
  uint64_t result = convertToUint64(input);
  std::cout << "Result: " << result << std::endl;
  return 0;
}