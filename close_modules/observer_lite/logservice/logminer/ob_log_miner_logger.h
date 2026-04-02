/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_LOG_MINER_LOGGER_H_
#define OCEANBASE_LOG_MINER_LOGGER_H_
#include <cstdint>

namespace oceanbase
{
namespace oblogminer
{
#define LOGMINER_STDOUT(...) \
::oceanbase::oblogminer::LogMinerLogger::get_logminer_logger_instance().log_stdout(__VA_ARGS__)
#define LOGMINER_STDOUT_V(...) \
::oceanbase::oblogminer::LogMinerLogger::get_logminer_logger_instance().log_stdout_v(__VA_ARGS__)
#define LOGMINER_LOGGER \
::oceanbase::oblogminer::LogMinerLogger::get_logminer_logger_instance()
class LogMinerLogger {
public:
  LogMinerLogger();
  static LogMinerLogger &get_logminer_logger_instance();
  void set_verbose(bool verbose) {
    verbose_ = verbose;
  }
  void log_stdout(const char *format, ...);
  void log_stdout_v(const char *format, ...);
  int log_progress(int64_t record_num, int64_t current_ts, int64_t begin_ts, int64_t end_ts);
private:
  int get_terminal_width();

private:
  static const int MIN_PB_WIDTH = 5;
  // 68: 20->datatime, 2->[], 7->percentage, 19->", written records: ", 20->the length of INT64_MAX
  static const int FIXED_TERMINAL_WIDTH = 68;
  static const int MAX_SCREEN_WIDTH = 4096;
  bool verbose_;
  char pb_str_[MAX_SCREEN_WIDTH];
  int64_t begin_ts_;
  int64_t last_ts_;
  int64_t last_record_num_;
};

}
}

#endif
