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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_DISPATCHER_INTERFACE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_DISPATCHER_INTERFACE_H_

namespace oceanbase
{
namespace libobcdc
{
enum FetcherDispatcherType
{
  UNKNOWN,
  DATA_DICT_DIS_TYPE,
  CDC_DIS_TYPE
};

class PartTransTask;
class IObLogFetcherDispatcher
{
public:
  IObLogFetcherDispatcher(FetcherDispatcherType dispatch_type) : dispatch_type_(dispatch_type) {}
  virtual ~IObLogFetcherDispatcher() {}

  bool is_data_dict_dispatcher() const { return FetcherDispatcherType::DATA_DICT_DIS_TYPE == dispatch_type_; }
  bool is_cdc_dispatcher() const { return FetcherDispatcherType::CDC_DIS_TYPE == dispatch_type_; }

  // DDL/DML: Support for dispatch all kinds of partition transaction tasks
  virtual int dispatch(PartTransTask &task, volatile bool &stop_flag) = 0;

  FetcherDispatcherType dispatch_type_;
};

}
}

#endif
