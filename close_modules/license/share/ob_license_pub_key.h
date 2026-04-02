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

#define PUBLIC_KEY_SIZE 256
#ifndef OCEANBASE_SHARE_OB_LICENSE_PUB_KEY_H
#define OCEANBASE_SHARE_OB_LICENSE_PUB_KEY_H

#define TEST_LICENSE_PUBLIC_KEY_PEM                                                                \
  "-----BEGIN PUBLIC KEY-----\n"                                                                   \
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyuMQuAUAq1z3li7STl8I\n"                             \
  "2ki8FzCYOswX2P+yFxKN5gEbY4kZCNgcY+nSbhhEr1kxFZG9EtwDzLng2VRT/ceD\n"                             \
  "CiIHBALLE8KoMW1wq/MI/9BkHD6LEstAc3HTaVUK1z2yWx8UvGnn9CZG+tHNrq2s\n"                             \
  "sajiv3ntBwXtiKbKnTelz3so5DEXcahjG5earftKWXVq3ltkFYHGSfads1SQtmAp\n"                             \
  "Vxwp14xZx56LmwIxgUsQAr5nklr5a071WFOQkuFte6gY0ZP6Ukkb4CKlO8rMJl6g\n"                             \
  "XxL9CMslcL6mX8LqBRRQ1yblLI/e4d7clSPB6yP9NVikM4Qy2PqVyCMYjPetJIUQ\n"                             \
  "QQIDAQAB\n"                                                                                     \
  "-----END PUBLIC KEY-----\n"

#define COMMERCIAL_LICENSE_PUBLIC_KEY_PEM                                                          \
  "-----BEGIN PUBLIC KEY-----\n"                                                                   \
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAz3EZR+9rUEGBCXGA7tXD\n"                             \
  "ZJ4okSdNlPH4M5HWdR7czCoV6/xyV0uvl7FyqJ+bQk8y8deBzAvxqE2ZBpYhtooH\n"                             \
  "fiLrMpVIjBW1ClhcpmkCP0UDf+Uj5wX1iqGiw2VbUVecuogAnlFHDHRLaHZSxSdt\n"                             \
  "IpNzDFwbPAzuJYVA5Z8PELf6WHGlQAxwdz6l8W+vCR65Bz66DuRuHYWB+U9yDRMx\n"                             \
  "hMO0R9OyOmxp/qsasZaEbuafgpnBTr91MngJvNzTRl6QHHaBULLQmldglGaUn96E\n"                             \
  "IhNFxDi9aVRojgdnxf8ONkLnMTC1kAxtkvhT2nbUEKJCQfQieY3SB8WGGCGUzRvm\n"                             \
  "OwIDAQAB\n"                                                                                     \
  "-----END PUBLIC KEY-----\n"

#ifdef OB_USE_TEST_PUBKEY
#define OB_LICENSE_PUBLIC_KEY_PEM TEST_LICENSE_PUBLIC_KEY_PEM
#else
#define OB_LICENSE_PUBLIC_KEY_PEM COMMERCIAL_LICENSE_PUBLIC_KEY_PEM
#endif // OB_USE_TEST_PUBKEY

#endif // OCEANBASE_SHARE_OB_LICENSE_PUB_KEY_H
