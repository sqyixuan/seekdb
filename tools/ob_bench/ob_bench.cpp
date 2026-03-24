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

#include <iostream>
#include <thread>
#define USING_LOG_PREFIX SERVER
#define private public
#define protected public
#include "deps/oblib/src/lib/list/ob_list.h"
#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"


using namespace oceanbase;
using namespace common;


bool GLOBAL_STOP = false;

int find_session(sqlclient::ObISQLConnection *conn, int64_t &session_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(conn->execute_read(OB_SYS_TENANT_ID, "select connection_id() as val", res))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int("val", session_id))) {
      }
    }
  }
  return ret;
}

struct RowKeyInfo
{
  std::vector<std::pair<int64_t , std::string>> pk_;
};

class ObBench
{
public:
  int init(const char *ip, int port, const char *tenant_name, const char *password, const char *db_name, const char *user_name) {
    int ret = OB_SUCCESS;
    ip_ = ip;
    port_= port;
    tenant_name_ = tenant_name;
    password_ = password;
    db_name_ = db_name;
    user_name_ = user_name;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_sql_proxy())) {
      LOG_WARN("init_sql_proxy fail", K(ret));
    }
    return ret;
  }
  common::ObMySQLProxy &get_proxy() {
    return sql_proxy_;
  }
  int64_t add_req() {
    return ATOMIC_AAF(&req_, 1);
  }
  int64_t add_succ() {
    return ATOMIC_AAF(&succ_, 1);
  }
  void add_time_cost(int64_t time_cost) {
    ATOMIC_AAF(&time_cost_, time_cost);
  }
  int64_t get_time_cost() {
    return ATOMIC_LOAD(&time_cost_);
  }
  uint64_t get_req() {
    return ATOMIC_LOAD(&req_);
  }
  uint64_t get_succ() {
    return ATOMIC_LOAD(&succ_);
  }
  void add_err() {
    ATOMIC_INC(&err_);
  }
  uint64_t get_err() {
    return ATOMIC_LOAD(&err_);
  }
  uint64_t fetch_pk() {
    return ATOMIC_AAF(&pk_, 1);
  }
  int init_sql_proxy();
  int do_prepare(const char* prepare_sql);
  int do_sample(const char* sample_sql);
  int do_query(const char *sql_str, int worker_idx);
private:
  int use_sample_format_sql_(std::string &sql_input, ObSqlString &sql, RowKeyInfo &info);
  int do_query_inner_(int64_t req_id, sqlclient::ObISQLConnection *conn, std::string &sql_input, ObSqlString &sql);
private:
  const char *ip_ = nullptr;
  int port_ = 0;
  const char *tenant_name_ = nullptr;
  const char *db_name_ = nullptr;
  const char *password_ = nullptr;
  const char *user_name_ = nullptr;

  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool_;
  common::ObMySQLProxy sql_proxy_;

  uint64_t req_ = 0;
  uint64_t succ_ = 0;
  uint64_t err_ = 0;
  uint64_t pk_ = 0;
  int64_t time_cost_ = 0;

public:
  std::vector<std::pair<std::string, int>> column_desc_; // column name and value type when sampling
  std::vector<RowKeyInfo> sample_info_;
  common::ObRandom rand_;
  int rand_mode_;
};

int ObBench::init_sql_proxy()
{
  int ret = OB_SUCCESS;
  sql_conn_pool_.set_db_param((std::string(user_name_) + "@" + std::string(tenant_name_)).c_str(), password_, db_name_);
  common::ObAddr db_addr;
  db_addr.set_ip_addr(ip_, port_);

  ObConnPoolConfigParam param;
  //param.sqlclient_wait_timeout_ = 10; // 10s
  // turn up it, make unittest pass
  param.sqlclient_wait_timeout_ = 100; // 100s
  param.long_query_timeout_ = 120*1000*1000; // 120s
  param.connection_refresh_interval_ = 200*1000; // 200ms
  param.connection_pool_warn_time_ = 1*1000*1000; // 1s
  param.sqlclient_per_observer_conn_limit_ = 100000;
  ret = sql_conn_pool_.init(db_addr, param);
  if (OB_SUCC(ret)) {
    sql_conn_pool_.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    ret = sql_proxy_.init(&sql_conn_pool_);
  }
  return ret;
}

int ObBench::do_prepare(const char* prepare_sql)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql_proxy_.write(prepare_sql, affected_rows))) {
    LOG_WARN("bench prepare fail", K(ret), K(prepare_sql));
  }
  return ret;
}

int ObBench::do_sample(const char* sample_sql)
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  sql.assign(sample_sql);
  int64_t start_time = ObTimeUtil::current_time();
  LOG_INFO("bench sample start", K(ret), K(sample_info_.size()), K(sql));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy_.read(res, sql.ptr()))) {
      LOG_WARN("bench sample failed", K(ret), K(sql));
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        sqlclient::ObMySQLResultImpl *result_impl = dynamic_cast<sqlclient::ObMySQLResultImpl*>(result);
        if (OB_ISNULL(result_impl)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          for (int idx = 0; idx < result_impl->result_column_count_ && OB_SUCC(ret); idx++) {
            std::string column_name(result_impl->fields_[idx].name, result_impl->fields_[idx].name_length);
            int column_type = result_impl->fields_[idx].type;
            column_desc_.push_back(std::pair<std::string, int>{column_name, column_type});
            LOG_INFO("bench sample column:", K(column_name.c_str()), K(column_type));
          }
        }
      }
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        RowKeyInfo info;
        for (int idx = 0; OB_SUCC(ret) && idx < column_desc_.size(); idx++) {
          ObString ob_string;
          int64_t val = 0;
          std::string str;
          switch (column_desc_.at(idx).second) {
           case MYSQL_TYPE_SHORT:
           case MYSQL_TYPE_LONG:
           case MYSQL_TYPE_LONGLONG:
             EXTRACT_INT_FIELD_MYSQL(*result, column_desc_.at(idx).first.c_str(), val, int64_t);
             info.pk_.push_back(std::pair<int64_t, std::string>{val, std::string()});
             break;
          case MYSQL_TYPE_VAR_STRING:
             EXTRACT_VARCHAR_FIELD_MYSQL(*result, column_desc_.at(idx).first.c_str(), ob_string);
             info.pk_.push_back(std::pair<int64_t, std::string>{0, std::string(ob_string.ptr(), ob_string.length())});
             break;
          default:
             ret = OB_NOT_SUPPORTED;
             LOG_WARN("not support" , K(ret), K(column_desc_.at(idx).second), K(column_desc_.at(idx).first.c_str()));
          }
        }
        if (OB_SUCC(ret)) {
          sample_info_.push_back(info);
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  int64_t prepare_cost = ObTimeUtil::current_time() - start_time;
  if (OB_SUCC(ret) && sample_info_.size() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  LOG_INFO("bench sample finish", K(ret), K(sample_info_.size()), K(sql), K(prepare_cost));
  if (OB_SUCC(ret)) {
    std::cout << "sample_size: " << sample_info_.size() << std::endl;
  }
  return ret;
}

int ObBench::use_sample_format_sql_(std::string &sql_input, ObSqlString &sql, RowKeyInfo &info)
{
  int ret = OB_SUCCESS;
   if (column_desc_.size() == 1 && column_desc_.at(0).second == MYSQL_TYPE_LONGLONG) {
     ret = sql.assign_fmt(sql_input.c_str(), info.pk_.at(0).first);
   } else if (column_desc_.size() == 1 && column_desc_.at(0).second == MYSQL_TYPE_LONG) {
     ret = sql.assign_fmt(sql_input.c_str(), int32_t(info.pk_.at(0).first));
   } else if (column_desc_.size() == 1 && column_desc_.at(0).second == MYSQL_TYPE_VAR_STRING) {
     ret = sql.assign_fmt(sql_input.c_str(), info.pk_.at(0).second.c_str());
   } else if (column_desc_.size() == 2 &&
              column_desc_.at(0).second == MYSQL_TYPE_LONGLONG &&
              column_desc_.at(1).second == MYSQL_TYPE_LONGLONG) {
     ret = sql.assign_fmt(sql_input.c_str(), info.pk_.at(0).first, info.pk_.at(1).first);
   } else if (column_desc_.size() == 3 &&
              column_desc_.at(0).second == MYSQL_TYPE_LONGLONG &&
              column_desc_.at(1).second == MYSQL_TYPE_VAR_STRING &&
              column_desc_.at(2).second == MYSQL_TYPE_LONGLONG) {
     ret = sql.assign_fmt(sql_input.c_str(), info.pk_.at(0).first, info.pk_.at(1).second.c_str(), info.pk_.at(2).first);
   } else {
     ret = OB_NOT_SUPPORTED;
   }
  return ret;
}

int ObBench::do_query_inner_(int64_t req_id, sqlclient::ObISQLConnection *conn, std::string &sql_input, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows;
  DEFER(if (OB_FAIL(ret)) LOG_WARN("do_query_inner", K(ret), K(sql_input.c_str()), K(sql)););
  int cmd_type = -1;
  if (sql_input.find("select") != sql_input.npos) {
    cmd_type = 1;
    if (sql_input.find('%') == sql_input.npos) {
      ret = sql.assign(sql_input.c_str());
    } else if (sample_info_.size() > 0) {
      int64_t rand_idx = 0;
      if (rand_mode_ == 0) {
        rand_idx = rand_.rand(0, sample_info_.size() - 1);
      } else if (rand_mode_ == 1) {
        rand_idx = req_id % sample_info_.size();
      } else if (rand_mode_ == 2) {
        if (req_id < sample_info_.size()) {
          rand_idx = req_id;
        } else {
          GLOBAL_STOP = true;
          cmd_type = 0;
        }
      } else {
        rand_idx = rand_.rand(0, sample_info_.size() - 1);
      }
      RowKeyInfo info = sample_info_.at(rand_idx);
      ret = use_sample_format_sql_(sql_input, sql, info);
    } else if (sql_input.find_first_of("%") == sql_input.find_last_of("%")) {
      ret = sql.assign_fmt(sql_input.c_str(), fetch_pk());
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  } else {
    cmd_type = 2;
    if (sql_input.find('%') == sql_input.npos) {
      ret = sql.assign(sql_input.c_str());
    } else if (sample_info_.size() > 0) {
      int64_t rand_idx = 0;
      if (rand_mode_ == 0) {
        rand_idx = rand_.rand(0, sample_info_.size() - 1);
      } else if (rand_mode_ == 1) {
        rand_idx = req_id % sample_info_.size();
      } else if (rand_mode_ == 2) {
        if (req_id < sample_info_.size()) {
          rand_idx = req_id;
        } else {
          GLOBAL_STOP = true;
          cmd_type = 0;
        }
      } else {
        rand_idx = rand_.rand(0, sample_info_.size() - 1);
      }
      RowKeyInfo info = sample_info_.at(rand_idx);
      ret = use_sample_format_sql_(sql_input, sql, info);
    } else if (sql_input.find_first_of("%") == sql_input.find_last_of("%")) {
      ret = sql.assign_fmt(sql_input.c_str(), fetch_pk());
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to prepare query", K(ret), K(sql), K(cmd_type), K(sql));
  } else if (cmd_type == -1) {
    std::cout << "unknow command " << sql_input << std::endl;
    ret = OB_ERR_UNEXPECTED;
  } else if (cmd_type == 0) {
    //do nothing
  } else if (cmd_type == 2) {
    if (OB_FAIL(conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    }
  } else if (cmd_type == 1) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(get_proxy().read(conn, res, OB_SYS_TENANT_ID, sql.ptr()))) {
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_SUCC(result->next())) {
        } else if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObBench::do_query(const char *sql_str, int worker_idx)
{
  int ret = OB_SUCCESS;
  sqlclient::ObISQLConnection *conn = NULL;
  int64_t session_id = 0;
  if (OB_FAIL(get_proxy().acquire(conn))) {
    LOG_WARN("get conn failed", K(ret), K(session_id));
    add_err();
  } else if (OB_FAIL(find_session(conn, session_id))) {
    LOG_WARN("find session failed", K(ret), K(session_id));
    add_err();
  }
  while (!GLOBAL_STOP && OB_SUCC(ret)) {
    int64_t start_time = ObTimeUtility::current_time();
    std::string sql_input(sql_str);
    ObSqlString query_sql;
    int64_t req_id = add_req();
    if (OB_FAIL(do_query_inner_(req_id, conn, sql_input, query_sql))) {
    }
    int64_t end_time = ObTimeUtility::current_time();
    int64_t cost = end_time - start_time;
    if (OB_FAIL(ret)) {
      LOG_WARN("query end>>> fail", K(ret), K(cost), K(session_id), K(query_sql));
    } else if (REACH_TIME_INTERVAL(1 * 1000L * 1000L)) {
      LOG_INFO("query end>>>", K(ret), K(cost), K(req_id), K(session_id), K(query_sql));
    }
    add_time_cost(end_time-start_time);
    if (OB_SUCC(ret)) {
      add_succ();
    } else {
      add_err();
    }
  }
  get_proxy().close(conn, ret);

  return ret;
}

void show_usage()
{
  printf("Usage:\n");
  printf("\t-h server host    [default 127.0.0.1]\n");
  printf("\t-p server port    [default 11002]\n");
  printf("\t-u user name      [default root]\n");
  printf("\t-P passworkd      [default null]\n");
  printf("\t-t tenant name    [default tt1]\n");
  printf("\t-d database       [default test]\n");
  printf("\t-c thread num     [default 1]\n");
  printf("\t-s time seconds   [default 300]\n");
  printf("\t-q sql cmd        [default select 1 from dual]\n");
  printf("\t-S sample rowkey for select\n");
  printf("\t-Q sql cmd before bench\n");
  printf("\t                  [PM_EASY] bench for easy io thread\n");
  printf("\t                  [PM_WORKER] bench for tenant worker thread\n");
  printf("\t                  [PM_CLOG] bench for clog submit\n");
  printf("\t                  select * from t1\n");
  printf("\t                  insert into t1 values(%%lu)\n");
  printf("\t                  select * from t1 where c1=%%d\n");
}

void signalHandler(int signum)
{
  GLOBAL_STOP = true;
}

int main(int argc, char *argv[])
{
  if (argc < 2) {
    show_usage();
    return 0;
  }
  int c = 0;
  const char *host = "127.0.0.1";
  int port = 11002;
  const char *tenant_name = "tt1";
  const char *user_name = "root";
  const char *db_name = "test";
  const char *password = "";
  int thread_count = 1;
  int time_sec = 300;
  const char *sql_str = "select 1 from dual";
  const char *sample_sql = NULL;
  const char *prepare_sql = NULL;
  int rand_mode = 0;
  while(EOF != (c = getopt(argc,argv,"h:p:t:d:c:s:S:q:Q:P:u:r:"))) {
    switch(c) {
    case 'h':
      host = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 't':
      tenant_name = optarg;
      break;
    case 'u':
      user_name = optarg;
      break;
    case 'P':
      password = optarg;
      break;
    case 'd':
      db_name = optarg;
      break;
    case 'c':
      thread_count = atoi(optarg);
      break;
    case 's':
      time_sec = atoi(optarg);
      break;
    case 'S':
      sample_sql = optarg;
      break;
    case 'q':
      sql_str = optarg;
      break;
    case 'Q':
      prepare_sql = optarg;
      break;
    case 'r':
      rand_mode = atoi(optarg);
      break;
    case '?':
      printf("unknow option:%c\n",optopt);
      break;
    default:
      break;
    }
  }
  if (host == nullptr || port == 0) {
    printf("need param %s %d\n", host, port);
    return -1;
  }
  system("rm -rf ob_bench.log*");

  OB_LOGGER.set_file_name("ob_bench.log", true);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_enable_async_log(true);

  ObBench bench;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObClockGenerator::get_instance().init())) {
  } else if (OB_FAIL(bench.init(host, port, tenant_name, password, db_name, user_name))) {
    LOG_WARN("bench init fail", K(ret));
  } else if (OB_NOT_NULL(prepare_sql) && OB_FAIL(bench.do_prepare(prepare_sql))) {
    LOG_WARN("bench prepare fail", K(ret), K(prepare_sql));
  } else if (OB_NOT_NULL(sample_sql) && OB_FAIL(bench.do_sample(sample_sql))) {
    LOG_WARN("bench sample fail", K(ret), K(sample_sql));
  } else {
    bench.rand_mode_ = rand_mode;
  }
  if (OB_FAIL(ret)) {
    std::cout << "bench prepare failed " << ret << std::endl;
    return ret;
  }
  signal(SIGINT, signalHandler);

  std::vector<std::thread> workers;
  printf("workers: %d\n", thread_count);
  printf("sql: %s\n", sql_str);
  int64_t start_time = ObTimeUtil::current_time();
  for (int i=0;i<thread_count;i++) {
    std::thread th(&ObBench::do_query, std::ref(bench), sql_str, i);
    workers.push_back(std::move(th));
  }

  uint64_t last_succ_sum = 0;
  uint64_t last_err_sum = 0;
  int64_t last_time_cost = 0;
  int64_t last_timestamp = start_time;
  int64_t current_time = 0;
  ::sleep(1);
  for (int i = 0; last_err_sum==0 && (current_time - start_time)/1e6 < time_sec && !GLOBAL_STOP; i++) {
    uint64_t succ_sum = bench.get_succ();
    uint64_t err_sum = bench.get_err();
    int64_t time_cost = bench.get_time_cost();
    current_time = ObTimeUtil::current_time();
    int64_t duration = (current_time - last_timestamp)/1000;
    double lat = 0;
    if (succ_sum - last_succ_sum > 0) {
      lat = double(time_cost - last_time_cost)/double(succ_sum - last_succ_sum)/1000;
    }
    char buffer [128];
    time_t rawtime;
    time (&rawtime);
    struct tm * timeinfo = localtime (&rawtime);
    strftime (buffer,sizeof(buffer),"%Y/%m/%d %H:%M:%S",timeinfo);
    printf("%s tps: %lu err: %lu avg_lat: %.1f ms\n", buffer,(succ_sum - last_succ_sum)*1000/duration, err_sum - last_err_sum, lat);
    std::cout.flush();
    last_succ_sum = succ_sum;
    last_err_sum = err_sum;
    last_time_cost = time_cost;
    last_timestamp = current_time;
    ::sleep(1);
  }
  GLOBAL_STOP = true;
  for (std::thread &it : workers) {
    it.join();
  }
  int64_t end_time = ObTimeUtil::current_time();
  printf("\n");
  int64_t tps_avg = 0;
  double lat_avg = 0;
  if (bench.get_succ() > 0) {
    tps_avg = bench.get_succ()/((end_time-start_time)/1000000);
    lat_avg = double(bench.get_time_cost())/double(bench.get_succ())/1000.0;
  }
  printf("cost: %ld s\n", (end_time - start_time)/1000000);
  printf("req:  %ld\n", bench.get_req());
  printf("succ: %ld\n", bench.get_succ());
  printf("err:  %ld\n", bench.get_err());
  printf("tps:  %ld\n", tps_avg);
  printf("lat:  %.1f ms\n", lat_avg);
  std::cout.flush();

  _Exit(0);
  return 0;
}
