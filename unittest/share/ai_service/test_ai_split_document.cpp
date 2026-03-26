/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define protected public
#define private public
#include "share/ai_service/ob_ai_spilit_document.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace oceanbase::share;
using namespace oceanbase::common;
namespace unittest
{
class TestAiSplitDocument : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

::testing::AssertionResult EqUtf8(const char* expected_expr, const char* actual_expr,
  const std::string& expected, const std::string& actual) {
if (expected == actual) return ::testing::AssertionSuccess();
return ::testing::AssertionFailure()
<< expected_expr << " vs " << actual_expr << " mismatch\n"
<< "expected: " << expected << "\n"
<< "actual: " << actual << "\n";
}

#define EXPECT_EQ_UTF8(expected, actual) \
EXPECT_PRED_FORMAT2(EqUtf8, expected, actual)


static void assert_chunks_result(
    const std::string &md,
    int level,
    const std::vector<std::string> &expected_contexts,
    const std::vector<std::string> &expected_titles,
    bool print_debug = false)
{
  auto res = to_chunks_with_order_guarantee(md, level);
  if (print_debug) {
    fprintf(stdout, "res.first: \n");
    for (size_t i = 0; i < res.first.size(); ++i) {
      fprintf(stdout, "context %d: %s\n", i, res.first[i].c_str());
    }
    fprintf(stdout, "\n");
    fprintf(stdout, "res.second: \n");
    for (int i = 0; i < res.second.size(); ++i) {
      fprintf(stdout, "title %d: %s\n", i, res.second[i].c_str());
    }
    fprintf(stdout, "\n");
  }
  ASSERT_EQ(expected_contexts.size(), res.first.size());
  ASSERT_EQ(expected_titles.size(), res.second.size());
  for (size_t i = 0; i < expected_contexts.size(); ++i) {
    EXPECT_EQ_UTF8(expected_contexts[i], res.first[i]) << "context mismatch at index " << i;
    EXPECT_EQ_UTF8(expected_titles[i],  res.second[i]) << "title mismatch at index " << i;
  }
}

TEST_F(TestAiSplitDocument, EmptyInput)
{
  assert_chunks_result("", 1, {}, {});
}

TEST_F(TestAiSplitDocument, InvalidLevelReturnsWhole)
{
  const std::string md = "no headers\ncontent";
  assert_chunks_result(md, 0, {"no headers\ncontent"}, {""});
  assert_chunks_result(md, 6, {"no headers\ncontent"}, {""});
}

TEST_F(TestAiSplitDocument, SingleHeaderAndBody)
{
  const std::string md = "# Title\nline1\nline2\n";
  assert_chunks_result(md, 1, {"# Title\nline1\nline2"}, {"Title"});
}

TEST_F(TestAiSplitDocument, MultipleHeaders)
{
  const std::string md =
      "# A\nA1\n\n## sub should be included in A chunk\nA2\n\n# B\nB1\n";
  assert_chunks_result(
      md,
      1,
      {
        "# A\nA1\n\n## sub should be included in A chunk\nA2",
        "# B\nB1",
      },
      {"A", "B"});
}

TEST_F(TestAiSplitDocument, IndentedHeaderIgnored)
{
  const std::string md = "  # NotHeader\nreal content\n# Header\nX\n";
  assert_chunks_result(
      md,
      1,
      {
        "# NotHeader\nreal content",
        "# Header\nX",
      },
      {"", "Header"});
}

TEST_F(TestAiSplitDocument, NoHeaderReturnsWhole)
{
  const std::string md = "  leading\ntext only\n";
  assert_chunks_result(md, 1, {"leading\ntext only"}, {""});
}

TEST_F(TestAiSplitDocument, ChineseLevel1HeaderTest)
{
  const std::string md = "# 标题\n内容\n## 子标题\n子内容\n# 标题2\n内容2\n";
  assert_chunks_result(
      md,
      1,
      {
        "# 标题\n内容\n## 子标题\n子内容",
        "# 标题2\n内容2",
      },
      {"标题", "标题2"});
}

TEST_F(TestAiSplitDocument, ChineseLevel2HeaderTest)
{
  const std::string md = "# 标题\n内容\n## 子标题\n子内容\n# 标题2\n内容2\n# 标题3\n内容3\n## 子标题2\n子内容2\n";
  assert_chunks_result(
      md,
      2,
      {
        "# 标题\n内容",
        "## 子标题\n子内容\n# 标题2\n内容2\n# 标题3\n内容3",
        "## 子标题2\n子内容2",
      },
      {"", "子标题", "子标题2"});
}
} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ai_split_document.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
