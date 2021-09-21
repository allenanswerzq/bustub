//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// logger.h
//
// Identification: src/include/common/logger.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   This software may be modified and distributed under the terms         *
 *   of the MIT license.  See the LICENSE file for details.                *
 *                                                                         *
 ***************************************************************************/

#ifndef HSTOREDEBUGLOG_H
#define HSTOREDEBUGLOG_H

/**
 * Debug logging functions for EE. Unlike the performance counters,
 * these are just fprintf() turned on/off by LOG_LEVEL compile option.
 * The main concern here is not to add any overhead on runtime performance
 * when the logging is turned off. Use LOG_XXX_ENABLED macros defined here to
 * eliminate all instructions in the final binary.
 * @author Hideaki
 */

#include <cxxabi.h>
#include <execinfo.h>
#include <sys/time.h>
#include <cstring>
#include <ctime>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

namespace bustub {

// https://blog.galowicz.de/2016/02/20/short_file_macro/
using cstr = const char *;

static constexpr cstr PastLastSlash(cstr a, cstr b) {
  return *a == '\0' ? b
                    : *b == '/' ? PastLastSlash(a + 1, a + 1)
                                : PastLastSlash(a + 1, b);
}

static constexpr cstr PastLastSlash(cstr a) { return PastLastSlash(a, a); }

#define __SHORT_FILE__                            \
  ({                                              \
    constexpr cstr sf__{PastLastSlash(__FILE__)}; \
    sf__;                                         \
  })

// Log levels.
static constexpr int LOG_LEVEL_OFF = 1000;
static constexpr int LOG_LEVEL_ERROR = 500;
static constexpr int LOG_LEVEL_WARN = 400;
static constexpr int LOG_LEVEL_INFO = 300;
static constexpr int LOG_LEVEL_DEBUG = 200;
static constexpr int LOG_LEVEL_TRACE = 100;
static constexpr int LOG_LEVEL_ALL = 0;

#define LOG_LOG_TIME_FORMAT "%Y-%m-%d %H:%M:%S"
#define LOG_OUTPUT_STREAM stdout

// Compile Option
#ifndef LOG_LEVEL
// TODO(TAs) : any way to use pragma message in GCC?
// #pragma message("Warning: LOG_LEVEL compile option was not explicitly
// given.")
#ifndef NDEBUG
// #pragma message("LOG_LEVEL_DEBUG is used instead as DEBUG option is on.")
static constexpr int LOG_LEVEL = LOG_LEVEL_DEBUG;
#else
// #pragma message("LOG_LEVEL_WARN is used instead as DEBUG option is off.")
static constexpr int LOG_LEVEL = LOG_LEVEL_INFO;
#endif
// #pragma message("Give LOG_LEVEL compile option to overwrite the default
// level.")
#endif

// For compilers which do not support __FUNCTION__
#if !defined(__FUNCTION__) && !defined(__GNUC__)
#define __FUNCTION__ ""
#endif

void OutputLogHeader(const char *file, int line, const char *func, int level);

// Two convenient macros for debugging
// 1. Logging macros.
// 2. LOG_XXX_ENABLED macros. Use these to "eliminate" all the debug blocks from
// release binary.
#ifdef LOG_ERROR_ENABLED
#undef LOG_ERROR_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ERROR_ENABLED
// #pragma message("LOG_ERROR was enabled.")
#define LOG_ERROR(...)                                                      \
  OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_ERROR); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                                \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                         \
  ::fflush(stdout)
#else
#define LOG_ERROR(...) ((void)0)
#endif

#ifdef LOG_WARN_ENABLED
#undef LOG_WARN_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_WARN
#define LOG_WARN_ENABLED
// #pragma message("LOG_WARN was enabled.")
#define LOG_WARN(...)                                                      \
  OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_WARN); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                               \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                        \
  ::fflush(stdout)
#else
#define LOG_WARN(...) ((void)0)
#endif

#ifdef LOG_INFO_ENABLED
#undef LOG_INFO_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_INFO_ENABLED
// #pragma message("LOG_INFO was enabled.")
#define LOG_INFO(...)                                                      \
  OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_INFO); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                               \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                        \
  ::fflush(stdout)
#else
#define LOG_INFO(...) ((void)0)
#endif

#ifdef LOG_DEBUG_ENABLED
#undef LOG_DEBUG_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_DEBUG_ENABLED
// #pragma message("LOG_DEBUG was enabled.")
#define LOG_DEBUG(...)                                                      \
  OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_DEBUG); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                                \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                         \
  ::fflush(stdout)
#else
#define LOG_DEBUG(...) ((void)0)
#endif

#ifdef LOG_TRACE_ENABLED
#undef LOG_TRACE_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_TRACE
#define LOG_TRACE_ENABLED
// #pragma message("LOG_TRACE was enabled.")
#define LOG_TRACE(...)                                                      \
  OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_TRACE); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                                \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                         \
  ::fflush(stdout)
#else
#define LOG_TRACE(...) ((void)0)
#endif

// Output log message header in this format: [type] [file:line:function] time -
// ex: [ERROR] [somefile.cpp:123:doSome()] 2008/07/06 10:00:00 -
inline void OutputLogHeader(const char *file, int line, const char *func,
                            int level) {
  time_t t = ::time(nullptr);
  tm *curTime = localtime(&t);  // NOLINT
  char time_str[32];            // FIXME
  ::strftime(time_str, 32, LOG_LOG_TIME_FORMAT, curTime);
  const char *type;
  switch (level) {
    case LOG_LEVEL_ERROR:
      type = "ERROR";
      break;
    case LOG_LEVEL_WARN:
      type = "WARN ";
      break;
    case LOG_LEVEL_INFO:
      type = "INFO ";
      break;
    case LOG_LEVEL_DEBUG:
      type = "DEBUG";
      break;
    case LOG_LEVEL_TRACE:
      type = "TRACE";
      break;
    default:
      type = "UNKWN";
  }
  // PAVLO: DO NOT CHANGE THIS
  ::fprintf(LOG_OUTPUT_STREAM, "%s [%s:%d:%s] %s - ", time_str, file, line,
            func, type);
}

//------------------------------------------------------------------------------
// Get debug option from env variable.
inline bool DebugLoggingEnabled() {
  static int state = 0;
  if (state == 0) {
    if (auto var = std::getenv("BUSTUB_LOG_DEBUG")) {
      if (std::string(var) == "1") {
        state = 1;
      }
      else {
        state = -1;
      }
    }
    else {
      // by default hide debug logging.
      state = -1;
    }
  }
  return state == 1;
}

class AtomicStream {
 public:
  template <typename T>
  AtomicStream &operator<<(T const &t) {
    oss << t;
    return *this;
  }

  std::string FinalString() { return oss.str(); }

 private:
  std::ostringstream oss;
};

class LogMessage {
 public:
  LogMessage(const char *file, int line, const std::string &prefix) {
    uint64_t thread = std::hash<std::thread::id>{}(std::this_thread::get_id());
    log_stream_ << "[" << GetDateTime() << "] "
                << "{" << thread << "} " << file << ":" << line << ":" << prefix
                << ": ";
  }

  void Flush() {
    log_stream_ << "\n";
    std::cerr << log_stream_.FinalString() << std::flush;
  }

  void Stacktrace() {
    const int stack_depth = 12;
    void *array[stack_depth];
    int size = backtrace(array, stack_depth);
    char **msg = backtrace_symbols(array, size);
    log_stream_ << "\n";
    for (int i = 1; i < size && msg[i]; i++) {
      log_stream_ << Demangle(msg[i]) << "\n";
    }
    free(msg);
  }

  std::string Demangle(char *msg) {
    // SO: how-to-automatically-generate-a-stacktrace-when-my-program-crashes
    char *mangled_name = 0;
    char *offset_begin = 0;
    char *offset_end = 0;
    // find parantheses and +address offset surrounding mangled name
    for (char *p = msg; *p; ++p) {
      if (*p == '(') {
        mangled_name = p;
      }
      else if (*p == '+') {
        offset_begin = p;
      }
      else if (*p == ')') {
        offset_end = p;
        break;
      }
    }
    if (mangled_name && offset_begin && offset_end &&
        mangled_name < offset_begin) {
      // if the line could be processed, attempt to demangle the symbol
      *mangled_name++ = '\0';
      *offset_begin++ = '\0';
      *offset_end++ = '\0';

      int status;
      char *real_name = abi::__cxa_demangle(mangled_name, 0, 0, &status);
      std::string real(real_name);
      free(real_name);

      if (status == 0) {
        // if demangling is successful, output the demangled function name
        return real;
      }
      else {
        return std::string(mangled_name);
        // otherwise, output the mangled function name
      }
    }
    else {
      // otherwise, print the whole line
      return msg;
    }
  }

  ~LogMessage() { Flush(); }

  AtomicStream &stream() { return log_stream_; }

 protected:
  AtomicStream log_stream_;

  std::string GetDateTime() {
    struct tm tm_time;
    struct timeval tval;
    memset(&tval, 0, sizeof(tval));
    gettimeofday(&tval, NULL);
    localtime_r(&tval.tv_sec, &tm_time);

    std::string buffer;
    buffer.resize(100);
    snprintf(&buffer[0], 100, "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
             tm_time.tm_year + 1900, 1 + tm_time.tm_mon, tm_time.tm_mday,
             tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec,
             static_cast<long>(tval.tv_usec));

    return buffer;
  }

 private:
  LogMessage(const LogMessage &);
  void operator=(const LogMessage &);
};

class LogMessageFatal : public LogMessage {
 public:
  LogMessageFatal(const char *file, int line, const std::string &prefix)
      : LogMessage(file, line, prefix) {}

  ~LogMessageFatal() {
    Stacktrace();
    Flush();
    abort();
  }

 private:
  LogMessageFatal(const LogMessageFatal &);
  void operator=(const LogMessageFatal &);
};

// This class is used to explicitly ignore values in the conditional
// logging macros.  This avoids compiler warnings like "value computed
// is not used" and "statement has no effect".
class LogMessageVoidify {
 public:
  LogMessageVoidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than "?:". See its usage.
#if !defined(_LIBCPP_SGX_NO_IOSTREAMS)
  void operator&(std::ostream &) {}
#endif
};

#define CHECK(x)                                        \
  if (!(x))                                             \
  LogMessageFatal(__FILE__, __LINE__, "FATAL").stream() \
      << "Check failed: " #x << ": "

#define LOG(severity) MY_LOG_##severity
#define LOG_IF(severity, condition) \
  !(condition) ? (void)0 : LogMessageVoidify() & LOG(severity)

#define MY_LOG_DEBUG \
  if (DebugLoggingEnabled()) LogMessage(__FILE__, __LINE__, "DEBUG").stream()
#define MY_LOG_INFO LogMessage(__FILE__, __LINE__, "INFO").stream()
#define MY_LOG_FATAL LogMessageFatal(__FILE__, __LINE__, "FATAL").stream()

}  // namespace bustub

#endif
