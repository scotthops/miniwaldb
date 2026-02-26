#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace miniwaldb::storage {

// Tiny helper for reading/writing whole files (MVP)
std::vector<std::uint8_t> read_file(const std::string& path);
void write_file_append(const std::string& path, const std::vector<std::uint8_t>& bytes);

} // namespace miniwaldb::storage
