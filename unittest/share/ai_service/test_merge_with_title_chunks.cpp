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
#include <iostream>
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
class TestMergeWithTitleChunks : public ::testing::Test
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

TEST_F(TestMergeWithTitleChunks, NoSplit_UseProvidedTitle)
{
  using oceanbase::share::Section;
  std::vector<Section> secs = {
    Section{ "## 小节\n这里是正文内容A B C", "标题一" }
  };
  auto res = merge_with_title_chunks(secs, /*chunkTokenNum*/ 512);
  ASSERT_EQ(1u, res.size());
  EXPECT_EQ_UTF8("标题一", res[0].second);
  EXPECT_EQ_UTF8("标题一\n## 小节\n这里是正文内容A B C", res[0].first);
}

TEST_F(TestMergeWithTitleChunks, ExtractTitle_WhenNoProvided)
{
  std::vector<std::string> secs = {
    "# 总标题\n正文第一行\n正文第二行"
  };
  auto res = merge_with_title_chunks(secs, /*chunkTokenNum*/ 512, /*delims*/ "\n。；！？", /*titleLevel*/ 3);
  ASSERT_EQ(1u, res.size());
  EXPECT_EQ_UTF8("# 总标题\n正文第一行\n正文第二行", res[0].first);
  EXPECT_EQ_UTF8("总标题", res[0].second);
}

TEST_F(TestMergeWithTitleChunks, SplitByChineseCharsWithTitlePrefix)
{
  // 低阈值强制拆分；包含中文标点作为分隔符
  std::string md =
      "# 标题一\n"
      "这是第一句。这里是第二句，包含一些中文字符。\n"
      "第三句！第四句？第五句；第六句。\n"
      "结尾一段话。";
  // 将阈值设得很小，触发按分隔符拆分
  auto res = merge_with_title_chunks(std::vector<std::string>{md}, /*chunkTokenNum*/ 32, "\n。；！？", 3);
  // fprintf(stdout, "res.size(): %zu\n", res.size());
  // for (size_t i = 0; i < 1; ++i) {
  //   fprintf(stdout, "res[%zu].first: %s\n", i, res[i].first.c_str());
  //   fprintf(stdout, "res[%zu].second: %s\n", i, res[i].second.c_str());
  // }
  ASSERT_EQ(2, res.size());

  EXPECT_EQ_UTF8("标题一", res[0].second);
  EXPECT_EQ_UTF8("# 标题一\n这是第一句。这里是第二句，包含一些中文字符。\n", res[0].first);
  EXPECT_EQ_UTF8("标题一", res[1].second);
  EXPECT_EQ_UTF8("标题一\n第三句！第四句？第五句；第六句。\n结尾一段话。", res[1].first);
}

//todo@dazhi: is_title_only_chunk 好像没有处理好
TEST_F(TestMergeWithTitleChunks, NotSplitInsideCodeBlock)
{
  // 代码块内部包含大量换行和标点，但不应在保护区内部拆分
  std::string md =
      "# 代码段\n"
      "```cpp\n"
      "int main() {\n"
      "  // 这里有很多很多很多内容。。。！！！？？？\n"
      "  return 0;\n"
      "}\n"
      "```\n"
      "结尾。";
  // 设置很小的阈值，若未考虑保护区会强拆；应保持为单段或至少保证代码块不被断开
  auto res = merge_with_title_chunks(std::vector<std::string>{md}, /*chunkTokenNum*/ 8, "\n。；！？", 3);
  // std::cout << "res.size(): " << res.size() << std::endl;
  ASSERT_GE(res.size(), 1u);
  // for (size_t i = 0; i < res.size(); ++i) {
  //   std::cout << "res[" << i << "].first: " << res[i].first << std::endl;
  //   std::cout << "res[" << i << "].second: " << res[i].second << std::endl;
  // }

  // 校验每段中代码块成对存在，未被断开
  auto count_triple_backticks = [](const std::string &s) {
    size_t cnt = 0, pos = 0;
    while ((pos = s.find("```", pos)) != std::string::npos) { cnt++; pos += 3; }
    return cnt;
  };
  size_t total_ticks = 0;
  for (const auto &p : res) total_ticks += count_triple_backticks(p.first);
  EXPECT_EQ(2u, total_ticks); // 一对 ``` 成对出现
}

TEST_F(TestMergeWithTitleChunks, test_split_xiyouji_to_chunks)
{
  const char* test_content = R"(sub_title[[403, 222, 608, 250]]
  ## 第一回 

  title[[269, 289, 746, 312]]
  灵根育孕源流出 心性修持大道生 

  text[[191, 347, 260, 367]]
  诗曰: 

  text[[245, 378, 662, 401]]
  混沌未分天地乱,茫茫渺渺无人见。 

  text[[245, 407, 662, 429]]
  自从盘古破鸿蒙,开辟从兹清浊辨。 

  text[[245, 435, 662, 459]]
  覆载群生仰至仁,发明万物皆成善。 

  text[[243, 465, 712, 489]]
  欲知造化会元功,须看《西游释厄传》①。 

  text[[138, 495, 886, 724]]
  盖闻天地之数,有十二万九千六百岁为一元。将一元分为十二会,乃子、丑、寅、卯、辰、巳、午、未、申、酉、戌、亥之十二支也。每会该一万八百岁。且就一日而论:子时得阳气,而丑则鸡鸣;寅不通光,而卯则日出;辰时食后,而已则挨排;日午天中,而未则西蹉;申时哺而日落酉;戌黄昏而入定亥。譬于大数,若到戌会之终,则天地昏眩而万物否矣。再去五千四百岁,交亥会之初,则当黑暗,而两间②人物俱无矣,故日混沌。又五千四百年岁,亥会将终,贞下起元,近子之会,而复逐渐开明。邵康节③

  <--- Page Split --->
  "冬至子之半, 天心无改移。一阳初动处, 万物未生时。"到此, 天始有根。再五千四百岁, 正当子会, 轻清上腾, 有日, 有月, 有星, 有辰。日、月、星、辰, 谓之四象。故曰, 天开于子。又经五千四百岁, 子会将终, 近丑之会, 而逐渐坚实。《易》曰①: "大哉乾元! 至哉坤元! 万物资生, 乃顺承天。"至此, 地始凝结。再五千四百岁, 正当丑会, 重浊下凝, 有水, 有火, 有山, 有石, 有土。水、火、山、石、土, 谓之五形。故曰, 地辟于丑。又经五千四百岁, 丑会终而寅会之初, 发生万物。历曰: "天气下降, 地气上升; 天地交合, 群物皆生。"至此, 天清地爽, 阴阳交合。再五千四百岁, 正当寅会, 生人, 生兽, 生禽, 正谓天地人, 三才定位。故曰, 人生于寅。感盘古开辟, 三皇治世, 五帝定伦, 世界之间, 遂分为四大部洲: 日东胜神洲, 日西牛贺洲, 日南瞻部洲, 日北俱芦洲。这部书单表东胜神洲。海外有一国土, 名曰傲来国。国近大海, 海中有一座名山, 唤为花果山。此山乃十洲之祖脉, 三岛之来龙, 自开清浊而立, 鸿蒙判后而成。真个好山! 有词赋为证。赋曰: 

  text[[125, 81, 605, 855]]
  : "冬至子之半, 天心无改移。一阳初动处, 万物未生时。"到 此, 天始有根。再五千四百岁, 正当子会, 轻清上腾, 有日, 有月, 有星, 有辰。日、月、星、辰, 谓之四象。 （《易》曰①: “大哉 乾元! 至哉坤元! 万物资生, 乃顺承天。”）至此, 地始凝结。再五 千四百岁, 正当丑会, 重浊下凝, 有水, 有火, 有山, 有石, 有土。 水、火、山、石、土, 谓之五形。故曰, 地辟于丑。又经五干四百 岁, 丑会终而寅会之初, 发生万物。 历曰：“天气下降, 地气上升; 天地交合, 群物皆生。”至此, 天清地爽, 阴阳交合。再五于四百 岁, 正当寅会, 生人, 生兽, 生禽, 正谓天地人, 三才定位。故日, 人生于寅。 感盘古开辟, 三皇治世, 五帝而定伦, 世界之间, 遂分为四大部 洲】日东胜神洲, 日西牛贺洲, 日南瞻部洲, 日北俱芦洲。这部书 单表东胜神洲。海外有一国土, 名曰傲来国。国近大海, 海中 有一座名山, 唤为花果山。此山乃十洲之祖脉, 三岛之来龙, 自开清浊而立, 鸿蒙判后而成。真个好山! 有词赋为证。赋 曰: 

  text[[187, 665, 879, 807]]
  形镇汪洋, 威宁瑶海。势镇汪洋, 潮涌银山鱼入穴; 威 宁瑶海, 波翻雪浪属离渊。水火方隅高积土, 东海之处耸崇 巅。丹崖怪石, 削壁奇峰。丹崖上, 彩凤双鸣; 削壁前, 麒麟 独卧。峰头时听锦鸡鸣, 石窟每观龙出入。林中有寿庵仙 孤, 树上有灵禽玄鹤。瑶草奇花不逊, 青松翠柏长春。仙桃

  <--- Page Split --->)";
  std::vector<Section> secs;
  auto chunks = to_chunks_with_order_guarantee(test_content, 3);
  fprintf(stdout, "chunks.size(): %zu\n", chunks.first.size());
  for (size_t i = 0; i < chunks.first.size(); ++i) {
    fprintf(stdout, "chunks[%zu].first: %s\n", i, chunks.first[i].c_str());
    fprintf(stdout, "chunks[%zu].second: %s\n", i, chunks.second[i].c_str());
  }
}

TEST_F(TestMergeWithTitleChunks, test_clean_text_content)
{
  const char* test_content = R"(<--- Page Split --->)";
  auto res = clean_text_content(test_content);
  fprintf(stdout, "res: %s\n", res.c_str());
  EXPECT_EQ_UTF8("", res);
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_merge_with_title_chunks.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
 