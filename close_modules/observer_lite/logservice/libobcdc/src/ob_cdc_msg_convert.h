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
 
#ifndef OCEANBASE_CDC_MSG_CONVERT_H__
#define OCEANBASE_CDC_MSG_CONVERT_H__

#include <oblogmsg/LogRecord.h>
#include <oblogmsg/LogMsgFactory.h>
#include <oblogmsg/MetaInfo.h>
#include <oblogmsg/StrArray.h>

using namespace oceanbase::logmessage;
namespace oceanbase
{
namespace liboblog
{
// class
#define DRCMessageFactory oceanbase::logmessage::LogMsgFactory
#define IBinlogRecord oceanbase::logmessage::ILogRecord
#define BinlogRecordImpl oceanbase::logmessage::LogRecordImpl
#define RecordType oceanbase::logmessage::RecordType
#define IDBMeta oceanbase::logmessage::IDBMeta
#define ITableMeta oceanbase::logmessage::ITableMeta
#define IColMeta oceanbase::logmessage::IColMeta
#define binlogBuf oceanbase::logmessage::BinLogBuf
#define IStrArray oceanbase::logmessage::StrArray
#define drcmsg_field_types oceanbase::logmessage::logmsg_field_types
#define DRCMSG_TYPE_ORA_BINARY_FLOAT LOGMSG_TYPE_ORA_BINARY_FLOAT
#define DRCMSG_TYPE_ORA_BINARY_DOUBLE LOGMSG_TYPE_ORA_BINARY_DOUBLE
#define DRCMSG_TYPE_ORA_XML LOGMSG_TYPE_ORA_XML
// method
#define createBinlogRecord createLogRecord

} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_CDC_MSG_CONVERT_H__ */
