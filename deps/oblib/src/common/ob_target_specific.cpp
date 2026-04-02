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

#define USING_LOG_PREFIX COMMON
#include "common/ob_target_specific.h"
#include <cstdio>

namespace oceanbase
{
namespace common
{

uint32_t arches;
void init_arches()
{
  fprintf(stderr, "[TRACE] init_arches: enter\n"); fflush(stderr);
  arches = 0;
  const CpuFlagSet flags;
  fprintf(stderr, "[TRACE] init_arches: CpuFlagSet constructed, checking flags\n"); fflush(stderr);
  if (flags.have_flag(CpuFlag::SSE4_2)) {
    arches |= static_cast<uint32_t>(ObTargetArch::SSE42);
  }
  fprintf(stderr, "[TRACE] init_arches: SSE42 done\n"); fflush(stderr);
  if (flags.have_flag(CpuFlag::AVX)) {
    arches |= static_cast<uint32_t>(ObTargetArch::AVX);
  }
  if (flags.have_flag(CpuFlag::AVX2)) {
    arches |= static_cast<uint32_t>(ObTargetArch::AVX2);
  }
  if (flags.have_flag(CpuFlag::AVX512BW)) {
    arches |= static_cast<uint32_t>(ObTargetArch::AVX512);
  }
  if (flags.have_flag(CpuFlag::NEON)) {
    arches |= static_cast<uint32_t>(ObTargetArch::NEON);
  }
  fprintf(stderr, "[TRACE] init_arches: all flags done, arches=%u, returning\n", arches); fflush(stderr);
}

} // namespace common
} // namespace oceanbase
