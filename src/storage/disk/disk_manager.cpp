//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager.cpp
//
// Identification: src/storage/disk/disk_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>  // NOLINT

#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

static char *buffer_used;

/**
 * Constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
DiskManager::DiskManager(const std::string &db_file)
    : file_name_(db_file),
      next_page_id_(0),
      num_flushes_(0),
      num_writes_(0),
      flush_log_(false),
      flush_log_f_(nullptr) {
  std::string::size_type n = file_name_.rfind('.');
  if (n == std::string::npos) {
    LOG_DEBUG("wrong file format");
    return;
  }

  log_name_ = file_name_.substr(0, n) + ".log";

  if (!OpenOrCreateFile(log_name_, log_io_)) {
    throw Exception("can't open dblog file");
  }

  if (!OpenOrCreateFile(db_file, db_io_)) {
    throw Exception("can't open db file");
  }

  buffer_used = nullptr;
}

bool DiskManager::OpenOrCreateFile(const std::string &filename,
                                   std::fstream &fs) {
  fs.open(filename,
          std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  // directory or file does not exist
  if (!fs.is_open()) {
    fs.clear();
    // create a new file
    fs.open(filename,
            std::ios::binary | std::ios::trunc | std::ios::app | std::ios::out);
    fs.close();
    // reopen with original mode
    fs.open(filename,
            std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  }
  return fs.is_open();
}

/**
 * Close all file streams
 */
void DiskManager::ShutDown() {
  db_io_.close();
  log_io_.close();
}

/**
 * Write the contents of the specified page into disk file
 */
void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
  // set write cursor to offset
  num_writes_ += 1;
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG_DEBUG("I/O error while writing");
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}

/**
 * Read the contents of the specified page into the given memory area
 */
void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
  int offset = page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset > GetFileSize(file_name_)) {
    LOG_DEBUG("I/O error reading past end of file");
    // std::cerr << "I/O error while reading" << std::endl;
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    if (db_io_.bad()) {
      LOG_DEBUG("I/O error while reading");
      return;
    }
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
      LOG_DEBUG("Read less than a page");
      db_io_.clear();
      // std::cerr << "Read less than a page" << std::endl;
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

/**
 * Write the contents of the log into disk file
 * Only return when sync is done, and only perform sequence write
 */
void DiskManager::WriteLog(char *log_data, int size) {
  // enforce swap log buffer
  assert(log_data != buffer_used);
  buffer_used = log_data;

  if (size == 0) {  // no effect on num_flushes_ if log buffer is empty
    return;
  }

  flush_log_ = true;

  if (flush_log_f_ != nullptr) {
    // used for checking non-blocking flushing
    assert(flush_log_f_->wait_for(std::chrono::seconds(10)) ==
           std::future_status::ready);
  }

  num_flushes_ += 1;
  // sequence write
  log_io_.write(log_data, size);

  // check for I/O error
  if (log_io_.bad()) {
    LOG_DEBUG("I/O error while writing log");
    return;
  }
  // needs to flush to keep disk file in sync
  log_io_.flush();
  flush_log_ = false;
}

/**
 * Read the contents of the log into the given memory area
 * Always read from the beginning and perform sequence read
 * @return: false means already reach the end
 */
bool DiskManager::ReadLog(char *log_data, int size, int offset) {
  if (offset >= GetFileSize(log_name_)) {
    // LOG_DEBUG("end of log file");
    // LOG_DEBUG("file size is %d", GetFileSize(log_name_));
    return false;
  }
  log_io_.seekp(offset);
  log_io_.read(log_data, size);

  if (log_io_.bad()) {
    LOG_DEBUG("I/O error while reading log");
    return false;
  }
  // if log file ends before reading "size"
  int read_count = log_io_.gcount();
  if (read_count < size) {
    log_io_.clear();
    memset(log_data + read_count, 0, size - read_count);
  }

  return true;
}

/**
 * Allocate new page (operations like create index/table)
 * For now just keep an increasing counter
 */
page_id_t DiskManager::AllocatePage() { return next_page_id_++; }

/**
 * Deallocate page (operations like drop index/table)
 * Need bitmap in header page for tracking pages
 * This does not actually need to do anything for now.
 */
void DiskManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {}

/**
 * Returns number of flushes made so far
 */
int DiskManager::GetNumFlushes() const { return num_flushes_; }

/**
 * Returns number of Writes made so far
 */
int DiskManager::GetNumWrites() const { return num_writes_; }

/**
 * Returns true if the log is currently being flushed
 */
bool DiskManager::GetFlushState() const { return flush_log_; }

/**
 * Private helper function to get disk file size
 */
int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
}

}  // namespace bustub
