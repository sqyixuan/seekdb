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

#ifndef OCEANBASE_UNITTEST_OBJECT_STORAGE_AUTHORIZATION_H_
#define OCEANBASE_UNITTEST_OBJECT_STORAGE_AUTHORIZATION_H_

namespace oceanbase
{
namespace unittest
{
// S3 CONFIG
const char *S3_BUCKET = "##S3_BUCKET##";
const char *S3_REGION = "##S3_REGION##";
const char *S3_ENDPOINT = "##S3_ENDPOINT##";
const char *S3_AK = "##S3_AK##";
const char *S3_SK = "##S3_SK##";
const char *S3_ROLE_ARN = "##S3_ROLE_ARN##";
const char *S3_EXTERNAL_ID = "##S3_EXTERNAL_ID##";

// OBS CONFIG
const char *OBS_BUCKET = "##OBS_BUCKET##";
const char *OBS_REGION = "##OBS_REGION##";
const char *OBS_ENDPOINT = "##OBS_ENDPOINT##";
const char *OBS_AK = "##OBS_AK##";
const char *OBS_SK = "##OBS_SK##";
const char *OBS_ROLE_ARN = "##OBS_ROLE_ARN##";
const char *OBS_EXTERNAL_ID = "##OBS_EXTERNAL_ID##";

// OSS CONFIG
const char *OSS_BUCKET = "##OSS_BUCKET##";
const char *OSS_ENDPOINT = "##OSS_ENDPOINT##";
const char *OSS_AK = "##OSS_AK##";
const char *OSS_SK = "##OSS_SK##";
const char *OSS_ROLE_ARN = "##OSS_ROLE_ARN##";
const char *OSS_EXTERNAL_ID = "##OSS_EXTERNAL_ID##";

// COS CONFIG
const char *COS_BUCKET = "##COS_BUCKET##";
const char *COS_ENDPOINT = "##COS_ENDPOINT##";
const char *COS_AK = "##COS_AK##";
const char *COS_SK = "##COS_SK##";
const char *COS_APPID = "##COS_APPID##";
const char *COS_ROLE_ARN = "##COS_ROLE_ARN##";
const char *COS_EXTERNAL_ID = "##COS_EXTERNAL_ID##";

// GCS CONFIG
const char *GCS_BUCKET = "##GCS_BUCKET##";
const char *GCS_ENDPOINT = "##GCS_ENDPOINT##";
const char *GCS_AK = "##GCS_AK##";
const char *GCS_SK = "##GCS_SK##";

// AZBLOB CONFIG
const char *AZBLOB_BUCKET = "##AZBLOB_BUCKET##";
const char *AZBLOB_ENDPOINT = "##AZBLOB_ENDPOINT##";
const char *AZBLOB_AK = "##AZBLOB_AK##";
const char *AZBLOB_SK = "##AZBLOB_SK##";

// STS CREDENTIALS
const char *STS_CREDENTIAL = "##STS_CREDENTIAL##";


// S3 CONFIG for test_object_storage_list
const char *S3_BUCKET2 = "##S3_BUCKET2##";
const char *S3_REGION2 = "##S3_REGION2##";
const char *S3_ENDPOINT2 = "##S3_ENDPOINT2##";
const char *S3_AK2 = "##S3_AK2##";
const char *S3_SK2 = "##S3_SK2##";

// OBS CONFIG for test_object_storage_list
const char *OBS_BUCKET2 = "##OBS_BUCKET2##";
const char *OBS_REGION2 = "##OBS_REGION2##";
const char *OBS_ENDPOINT2 = "##OBS_ENDPOINT2##";
const char *OBS_AK2 = "##OBS_AK2##";
const char *OBS_SK2 = "##OBS_SK2##";

// OSS CONFIG for test_object_storage_list
const char *OSS_BUCKET2 = "##OSS_BUCKET2##";
const char *OSS_ENDPOINT2 = "##OSS_ENDPOINT2##";
const char *OSS_AK2 = "##OSS_AK2##";
const char *OSS_SK2 = "##OSS_SK2##";

// COS CONFIG for test_object_storage_list
const char *COS_BUCKET2 = "##COS_BUCKET2##";
const char *COS_ENDPOINT2 = "##COS_ENDPOINT2##";
const char *COS_AK2 = "##COS_AK2##";
const char *COS_SK2 = "##COS_SK2##";
const char *COS_APPID2 = "##COS_APPID2##";

// GCS CONFIG for test_object_storage_list
const char *GCS_BUCKET2 = "##GCS_BUCKET2##";
const char *GCS_ENDPOINT2 = "##GCS_ENDPOINT2##";
const char *GCS_AK2 = "##GCS_AK2##";
const char *GCS_SK2 = "##GCS_SK2##";

// AZBLOB CONFIG
const char *AZBLOB_BUCKET2 = "##AZBLOB_BUCKET2##";
const char *AZBLOB_ENDPOINT2 = "##AZBLOB_ENDPOINT2##";
const char *AZBLOB_AK2 = "##AZBLOB_AK2##";
const char *AZBLOB_SK2 = "##AZBLOB_SK2##";

} // end of unittest
} // end of oceanbase

#endif
