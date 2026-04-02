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

#ifdef MINER_SCHEMA_DEF

MINER_SCHEMA_DEF(TENANT_ID, 0)
MINER_SCHEMA_DEF(TRANS_ID, 1)
MINER_SCHEMA_DEF(PRIMARY_KEY, 2)
// MINER_SCHEMA_DEF(ROW_UNIQUE_ID, 3)
// MINER_SCHEMA_DEF(SEQ_NO, 4)
MINER_SCHEMA_DEF(TENANT_NAME, 5)
MINER_SCHEMA_DEF(DATABASE_NAME, 6)
MINER_SCHEMA_DEF(TABLE_NAME, 7)
MINER_SCHEMA_DEF(OPERATION, 8)
MINER_SCHEMA_DEF(OPERATION_CODE, 9)
MINER_SCHEMA_DEF(COMMIT_SCN, 10)
MINER_SCHEMA_DEF(COMMIT_TIMESTAMP, 11)
MINER_SCHEMA_DEF(SQL_REDO, 12)
MINER_SCHEMA_DEF(SQL_UNDO, 13)
MINER_SCHEMA_DEF(ORG_CLUSTER_ID, 14)

#endif
