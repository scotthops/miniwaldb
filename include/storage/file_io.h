#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace miniwaldb::storage {

// Tiny helper for reading/writing whole files (MVP)
std::vector<std::uint8_t> read_file(const std::string& path);
void write_file_append(const std::string& path, const std::vector<std::uint8_t>& bytes);
void write_file(const std::string& path, const std::vector<std::uint8_t>& bytes);

using KvSnapshot = std::unordered_map<std::int64_t, std::string>;

void save_snapshot(const std::string& path, const KvSnapshot& kv);
KvSnapshot load_snapshot(const std::string& path);

} // namespace miniwaldb::storage
