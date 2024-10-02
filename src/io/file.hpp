#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <cstdarg>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "log.h"
#include "serialize.hpp"


template <typename T>
bool save_raw(const std::string& filename, const std::vector<T>& data) {
  // log_info("saving file '%s'", filename.c_str());
  std::size_t delpos = filename.find_last_of("/");
  if (delpos != std::string::npos) {
    std::filesystem::create_directory(filename.substr(0, delpos));
  }

  std::ofstream file(filename, std::ios::binary);
  if (file.eof() || file.fail()) {
    log_error("failed to save file '%s'", filename.c_str());
    return false;
  }

  file.write(reinterpret_cast<const char*>(&data[0]), data.size() * sizeof(T));
  file.close();
  // log_info("file '%s' saved", filename.c_str());
  return true;
}

// save a serialized file
template <class T>
bool save_file(const std::string& filename, const T& data) {
  log_info("saving file '%s'", filename.c_str());
  std::size_t delpos = filename.find_last_of("/");
  if (delpos != std::string::npos) {
    std::filesystem::create_directory(filename.substr(0, delpos));
  }

  std::ofstream file(filename, std::ios::binary);
  if (file.eof() || file.fail()) {
    log_error("failed to save file '%s'", filename.c_str());
    return false;
  }

  __serialize_detail::stream res;
  serialize(data, res);
  file.write(reinterpret_cast<char*>(&res[0]), res.size());
  file.close();
  res.clear();
  log_info("file '%s' saved", filename.c_str());
  return true;
}

template <typename T>
std::vector<T> load_raw(const std::string& filename) {
  // log_info("loading file '%s'", filename.c_str());

  std::ifstream file(filename, std::ios::binary);
  if (file.eof() || file.fail()) {
    log_fatal("cannot open file '%s'", filename.c_str());
    exit(1);
  }

  file.seekg(0, std::ios_base::end);
  const std::streampos size = file.tellg();
  file.seekg(0, std::ios_base::beg);
  std::vector<T> res((size_t(size) + sizeof(T) - 1) / sizeof(T));
  file.read(reinterpret_cast<char*>(&res[0]), size);
  file.close();

  return res;
}

// load a serialized file
template <class T>
T load_file(const std::string& filename) {
  auto res = load_raw<uint8_t>(filename);
  T ret = deserialize<T>(res);
  res.clear();
  return ret;
}
