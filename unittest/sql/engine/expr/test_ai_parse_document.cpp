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

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>
#include "ob_expr_test_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "pdfium/fpdfview.h"
#include "pdfium/fpdf_edit.h"
#include "pdfium/fpdf_save.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace std;

class ObAIParseDocumentTest: public ::testing::Test
{
public:
    ObAIParseDocumentTest();
    virtual ~ObAIParseDocumentTest();
    virtual void SetUp();
    virtual void TearDown();
private:
    // disallow copy
    ObAIParseDocumentTest(const ObAIParseDocumentTest &other);
    ObAIParseDocumentTest& operator=(const ObAIParseDocumentTest &other);
protected:
    // data members
    static bool pdfium_initialized_;
    
    // 创建一个简单的 PDF 测试数据
    bool create_test_pdf(const char* filename);
    bool file_exists(const char* filename);
};

bool ObAIParseDocumentTest::pdfium_initialized_ = false;

ObAIParseDocumentTest::ObAIParseDocumentTest()
{
}

ObAIParseDocumentTest::~ObAIParseDocumentTest()
{
}

void ObAIParseDocumentTest::SetUp()
{
    // 初始化 PDFium 库（只初始化一次）
    if (!pdfium_initialized_) {
        FPDF_LIBRARY_CONFIG config;
        config.version = 2;
        config.m_pUserFontPaths = NULL;
        config.m_pIsolate = NULL;
        config.m_v8EmbedderSlot = 0;
        FPDF_InitLibraryWithConfig(&config);
        pdfium_initialized_ = true;
    }
}

void ObAIParseDocumentTest::TearDown()
{
}

bool ObAIParseDocumentTest::file_exists(const char* filename)
{
    struct stat buffer;
    return (stat(filename, &buffer) == 0);
}

bool ObAIParseDocumentTest::create_test_pdf(const char* filename)
{
    // 使用 PDFium 创建一个简单的测试 PDF
    FPDF_DOCUMENT doc = FPDF_CreateNewDocument();
    if (!doc) {
        return false;
    }
    
    // 添加一个空白页（A4 尺寸：595x842 点）
    FPDF_PAGE page = FPDFPage_New(doc, 0, 595, 842);
    if (!page) {
        FPDF_CloseDocument(doc);
        return false;
    }
    
    FPDFPage_GenerateContent(page);
    FPDF_ClosePage(page);
    
    // 保存 PDF 文件
    FILE* fp = fopen(filename, "wb");
    if (!fp) {
        FPDF_CloseDocument(doc);
        return false;
    }
    
    // PDFium 保存回调
    struct FileWriter {
        FPDF_FILEWRITE base;
        FILE* fp;
        
        static int WriteBlock(FPDF_FILEWRITE* pThis, const void* data, unsigned long size) {
            FileWriter* writer = reinterpret_cast<FileWriter*>(pThis);
            return fwrite(data, 1, size, writer->fp) == size ? 1 : 0;
        }
    };
    
    FileWriter writer;
    writer.base.version = 1;
    writer.base.WriteBlock = FileWriter::WriteBlock;
    writer.fp = fp;
    
    bool success = FPDF_SaveAsCopy(doc, &writer.base, 0) != 0;
    
    fclose(fp);
    FPDF_CloseDocument(doc);
    
    return success;
}

// 测试用例 1: 测试从文件转换 PDF 到 PNG
// TEST_F(ObAIParseDocumentTest, test_convert_pdf_file_to_png)
// {
//     const char* test_pdf = "/data/1/fengsun.cx/lite_base/build_debug/unittest/sql/engine/expr/test_oceanbase.pdf";
//     const char* output_png = "/data/1/fengsun.cx/lite_base/build_debug/unittest/sql/engine/expr/test_oceanbase_output.png";
    
//     // 创建测试 PDF 文件
//     ASSERT_TRUE(create_test_pdf(test_pdf));
//     ASSERT_TRUE(file_exists(test_pdf));
    
//     // 转换 PDF 第一页为 PNG
//     int ret = ObAIFuncDocumentUtils::convert_pdf_file_to_png(test_pdf, 0, output_png,
//                                       0, 0,  // 自动计算尺寸
//                                       150,   // 150 DPI
//                                       FPDF_ANNOT);
    
//     ASSERT_EQ(OB_SUCCESS, ret);
//     ASSERT_TRUE(file_exists(output_png));
    
//     // 验证输出文件大小 > 0
//     struct stat st;
//     ASSERT_EQ(0, stat(output_png, &st));
//     ASSERT_GT(st.st_size, 0);
    
//     // 清理测试文件
//     unlink(test_pdf);
//     unlink(output_png);
// }

TEST_F(ObAIParseDocumentTest, test_convert_pdf_file_to_png)
{
    const char* test_pdf = "/data/1/shenyunlong.syl/ocr-data/2024.ccl-2.6-2.pdf";
    const char* output_png = "/data/1/shenyunlong.syl/ocr-data/2024.ccl-2.6-2.png";
    
    ObArenaAllocator allocator;
    ObString pdf_content;
    ASSERT_EQ(OB_SUCCESS, load_file_to_string(test_pdf, allocator, pdf_content));
    ObArray<ObString> images;
    ASSERT_EQ(OB_SUCCESS,ObAIFuncDocumentUtils::convert_pdf_to_images(allocator, pdf_content, images));
    ASSERT_EQ(1, images.count());
    ASSERT_EQ(OB_SUCCESS, ObAIFuncDocumentUtils::save_image_to_file(allocator, images.at(0), output_png));
}


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    
    // 清理 PDFium 库
    FPDF_DestroyLibrary();
    
    return result;
}
 