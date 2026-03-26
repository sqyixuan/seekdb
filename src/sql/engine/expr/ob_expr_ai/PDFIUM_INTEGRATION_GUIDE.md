# PDFium 库集成指南

## 概述

本文档说明如何将 PDFium 动态库（.so 文件）集成到 OceanBase 项目中，实现 PDF 文档解析和转换功能。

---

## 一、前置准备

### 1.1 PDFium 库文件位置

PDFium 库文件应放置在以下目录结构中：

```
/data/1/fengsun.cx/lite_base/deps/3rd/usr/local/oceanbase/deps/devel/
├── include/
│   └── pdfium/              # PDFium 头文件目录
│       ├── fpdfview.h
│       ├── fpdf_text.h
│       ├── fpdf_edit.h
│       └── fpdf_save.h
└── lib/
    └── libpdfium.so         # PDFium 动态库
```

### 1.2 辅助库文件

将 `stb_image_write.h`（图像写入库）放置到：

```bash
/data/1/fengsun.cx/lite_base/deps/3rd/usr/local/oceanbase/deps/devel/include/stb_image_write.h
```

**执行命令：**
```bash
cp /data/opt/pdf_to_png/stb_image_write.h \
   /data/1/fengsun.cx/lite_base/deps/3rd/usr/local/oceanbase/deps/devel/include/
```

---

## 二、CMake 配置

### 2.1 主模块配置 (src/sql/CMakeLists.txt)

在 `src/sql/CMakeLists.txt` 文件末尾添加以下配置：

```cmake
# ============== PDFium 库配置 ==============

# 添加 PDFium 和 stb_image_write 头文件路径
target_include_directories(ob_sql PUBLIC
  ${DEP_DIR}/include/pdfium      # PDFium 头文件
  ${DEP_DIR}/include              # stb_image_write.h
)

# 链接 PDFium 库
target_link_libraries(ob_sql PUBLIC 
  ob_base
  ${DEP_DIR}/lib/libpdfium.so
)

# 设置 RPATH，确保运行时能找到 pdfium 动态库
set_target_properties(ob_sql PROPERTIES
  INSTALL_RPATH "${DEP_DIR}/lib"
  BUILD_RPATH "${DEP_DIR}/lib"
)
```

**关键说明：**
- `${DEP_DIR}` 在 `cmake/Env.cmake` 中定义为 `${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel`
- `target_include_directories`：添加头文件搜索路径
- `target_link_libraries`：链接 PDFium 动态库
- `set_target_properties`：设置运行时库搜索路径（RPATH）

### 2.2 单元测试配置 (unittest/sql/engine/expr/CMakeLists.txt)

在单元测试的 CMakeLists.txt 中添加：

```cmake
# 为 test_ai_parse_document 添加 PDFium 库链接
target_link_libraries(test_ai_parse_document PRIVATE 
  ${DEP_DIR}/lib/libpdfium.so
)

target_include_directories(test_ai_parse_document PRIVATE
  ${DEP_DIR}/include/pdfium
  ${DEP_DIR}/include
)
```

---

## 三、代码实现

### 3.1 头文件引入

在 `ob_ai_func_utils.cpp` 中引入 PDFium 和 stb_image_write：

```cpp
// PDFium 头文件
#include "pdfium/fpdfview.h"
#include "pdfium/fpdf_edit.h"

// stb_image_write 实现（禁用编译警告）
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-field-initializers"
#endif

#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif
```

**关键点：**
- 使用 `pdfium/` 前缀引入 PDFium 头文件
- 使用 `#pragma clang diagnostic` 禁用 stb_image_write 的编译警告
- `#define STB_IMAGE_WRITE_IMPLEMENTATION` 在引入前声明，以包含实现代码

### 3.2 函数实现位置

PDF 转换功能实现在 `ObAIFuncDocumentUtils` 工具类中：

**头文件：** `src/sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h`

```cpp
class ObAIFuncDocumentUtils
{
public:
  // 从文件路径转换 PDF 为 PNG
  static int convert_pdf_file_to_png(const char* pdf_path, int page_index, 
                                      const char* output_path, 
                                      int render_width, int render_height, 
                                      int dpi, int flags);
  
  // 从内存数据转换 PDF 为 PNG
  static int convert_pdf_page_to_png(const unsigned char* pdf_data, 
                                      size_t pdf_data_len,
                                      int page_index, const char* output_path,
                                      int render_width, int render_height, 
                                      int dpi, int flags,
                                      unsigned char** out_png_data, 
                                      size_t* out_png_len);
  
  // 将 PDF 转换为图片数组
  static int convert_pdf_file_to_images(ObIAllocator &allocator, 
                                         ObString &pdf, 
                                         ObArray<ObString> &images);
  
  // 将图片转换为 base64 字符串
  static int convert_image_to_base64(ObIAllocator &allocator, 
                                      ObString &image, 
                                      ObString &base64);
};
```

**实现文件：** `src/sql/engine/expr/ob_expr_ai/ob_ai_func_utils.cpp`

---

## 四、编译配置

### 4.1 DEP_DIR 变量

`DEP_DIR` 定义在 `cmake/Env.cmake` 中：

```cmake
set(DEP_DIR ${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel)
```

### 4.2 编译选项说明

**头文件搜索路径：**
```cmake
-I${DEP_DIR}/include/pdfium
-I${DEP_DIR}/include
```

**库链接选项：**
```cmake
-L${DEP_DIR}/lib -lpdfium
```

**运行时库路径（RPATH）：**
```cmake
-Wl,-rpath,${DEP_DIR}/lib
```

---

## 五、常见问题及解决方案

### 5.1 编译错误：找不到头文件

**错误信息：**
```
fatal error: fpdfview.h: No such file or directory
```

**解决方案：**
1. 检查 PDFium 头文件是否在正确位置
2. 确认 CMakeLists.txt 中添加了 `target_include_directories`
3. 使用 `pdfium/` 前缀引入头文件：`#include "pdfium/fpdfview.h"`

### 5.2 链接错误：找不到符号

**错误信息：**
```
ld.lld: error: undefined symbol: FPDF_InitLibraryWithConfig
```

**解决方案：**
1. 检查 `libpdfium.so` 是否存在于 `${DEP_DIR}/lib/`
2. 确认 CMakeLists.txt 中添加了 `target_link_libraries`
3. 对于单元测试，需要单独配置链接

### 5.3 编译警告：missing-field-initializers

**错误信息：**
```
stb_image_write.h:514:32: error: missing field 'context' initializer
```

**解决方案：**
使用 `#pragma` 指令禁用警告：

```cpp
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-field-initializers"
#endif

#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif
```

### 5.4 运行时错误：找不到动态库

**错误信息：**
```
error while loading shared libraries: libpdfium.so: cannot open shared object file
```

**解决方案：**
1. 确认 CMakeLists.txt 中设置了 RPATH：
   ```cmake
   set_target_properties(ob_sql PROPERTIES
     INSTALL_RPATH "${DEP_DIR}/lib"
     BUILD_RPATH "${DEP_DIR}/lib"
   )
   ```

2. 或者临时设置 `LD_LIBRARY_PATH`：
   ```bash
   export LD_LIBRARY_PATH=${DEP_DIR}/lib:$LD_LIBRARY_PATH
   ```

### 5.5 链接器版本问题

**错误信息：**
```
ld: unrecognized option '--export-dynamic-symbol=obp_*'
```

**解决方案：**
启用 lld 链接器：

```bash
cd /data/1/fengsun.cx/lite_base/build_debug
cmake .. -DOB_USE_LLD=ON -DCMAKE_BUILD_TYPE=Debug
make -j$(nproc)
```

---

## 六、使用示例

### 6.1 基本使用

```cpp
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "pdfium/fpdfview.h"

// 初始化 PDFium 库（程序启动时调用一次）
FPDF_LIBRARY_CONFIG config;
config.version = 2;
config.m_pUserFontPaths = NULL;
config.m_pIsolate = NULL;
config.m_v8EmbedderSlot = 0;
FPDF_InitLibraryWithConfig(&config);

// 转换 PDF 为 PNG
int ret = ObAIFuncDocumentUtils::convert_pdf_file_to_png(
    "/path/to/input.pdf",    // PDF 文件路径
    0,                        // 页面索引（从 0 开始）
    "/path/to/output.png",   // 输出 PNG 路径
    0,                        // 渲染宽度（0=自动）
    0,                        // 渲染高度（0=自动）
    150,                      // DPI
    FPDF_ANNOT               // 渲染标志
);

if (ret == OB_SUCCESS) {
    // 成功
}

// 程序退出时清理
FPDF_DestroyLibrary();
```

### 6.2 从内存转换

```cpp
// 读取 PDF 文件到内存
unsigned char* pdf_data = ...;
size_t pdf_len = ...;

// 输出变量
unsigned char* png_data = nullptr;
size_t png_len = 0;

// 转换（同时保存到文件和内存）
int ret = ObAIFuncDocumentUtils::convert_pdf_page_to_png(
    pdf_data, pdf_len,       // PDF 数据
    0,                       // 页面索引
    "/path/to/output.png",   // 输出文件路径
    800, 600,                // 自定义尺寸
    150,                     // DPI
    FPDF_ANNOT,             // 渲染标志
    &png_data,              // 输出 PNG 数据（调用者需要 free）
    &png_len                // 输出 PNG 数据长度
);

if (ret == OB_SUCCESS && png_data != nullptr) {
    // 使用 PNG 数据
    // ...
    
    // 释放内存（重要！）
    free(png_data);
}
```

### 6.3 转换所有页面为图片数组

```cpp
ObArenaAllocator allocator;
ObString pdf_content;  // PDF 文件内容
ObArray<ObString> images;

// 转换所有页面为 BMP 图片数组
int ret = ObAIFuncDocumentUtils::convert_pdf_file_to_images(
    allocator, 
    pdf_content, 
    images
);

if (ret == OB_SUCCESS) {
    // 处理每个图片
    for (int i = 0; i < images.count(); i++) {
        ObString image = images.at(i);
        
        // 转换为 base64
        ObString base64;
        ObAIFuncDocumentUtils::convert_image_to_base64(allocator, image, base64);
        
        // 或保存到文件
        char filename[256];
        snprintf(filename, sizeof(filename), "page_%d.bmp", i);
        ObAIFuncDocumentUtils::save_image_to_file(allocator, image, filename);
    }
}
```

---

## 七、单元测试

### 7.1 测试文件位置

```
unittest/sql/engine/expr/test_ai_parse_document.cpp
```

### 7.2 运行测试

```bash
cd /data/1/fengsun.cx/lite_base/build_debug

# 编译测试
ob-make test_ai_parse_document

# 运行测试
./unittest/sql/engine/expr/test_ai_parse_document
```

### 7.3 测试用例

- `test_convert_pdf_file_to_png`：从文件转换 PDF 到 PNG
- `test_convert_with_custom_size`：自定义尺寸转换
- `test_convert_pdf_memory_to_png`：从内存转换 PDF
- `test_convert_to_memory_only`：仅输出到内存
- `test_invalid_parameters`：无效参数测试
- `test_invalid_page_index`：页面索引超出范围测试

---

## 八、文件清单

### 8.1 新增/修改的文件

| 文件路径 | 说明 |
|---------|------|
| `src/sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h` | 添加 ObAIFuncDocumentUtils 类声明 |
| `src/sql/engine/expr/ob_expr_ai/ob_ai_func_utils.cpp` | 实现 PDF 转换功能（约 400 行） |
| `src/sql/CMakeLists.txt` | 配置 PDFium 库链接和头文件路径 |
| `unittest/sql/engine/expr/CMakeLists.txt` | 配置测试的 PDFium 库链接 |
| `unittest/sql/engine/expr/test_ai_parse_document.cpp` | PDF 转换功能单元测试 |

### 8.2 三方库文件

| 文件路径 | 说明 |
|---------|------|
| `deps/3rd/.../include/pdfium/fpdfview.h` | PDFium 主头文件 |
| `deps/3rd/.../include/pdfium/fpdf_text.h` | PDF 文本处理 |
| `deps/3rd/.../include/pdfium/fpdf_edit.h` | PDF 编辑功能 |
| `deps/3rd/.../include/pdfium/fpdf_save.h` | PDF 保存功能 |
| `deps/3rd/.../lib/libpdfium.so` | PDFium 动态库 |
| `deps/3rd/.../include/stb_image_write.h` | 图像写入库（header-only） |

---

## 九、架构说明

### 9.1 模块关系

```
┌─────────────────────────────────────┐
│     OceanBase SQL Engine            │
│  (ob_sql / oceanbase.so)            │
└──────────────┬──────────────────────┘
               │ links
               ▼
┌─────────────────────────────────────┐
│   ObAIFuncDocumentUtils              │
│   (ob_ai_func_utils.cpp)             │
│                                      │
│  - convert_pdf_file_to_png()         │
│  - convert_pdf_page_to_png()         │
│  - convert_pdf_file_to_images()      │
└──────────────┬──────────────────────┘
               │ uses
               ▼
┌─────────────────────────────────────┐
│   Third-party Libraries              │
│                                      │
│  - libpdfium.so (PDF 处理)           │
│  - stb_image_write.h (PNG 写入)      │
└─────────────────────────────────────┘
```

### 9.2 命名空间

```cpp
namespace oceanbase {
namespace common {
  class ObAIFuncDocumentUtils {
    // PDF 转换功能
  };
}
}
```

---

## 十、编译流程

```bash
# 1. 确保依赖库文件在正确位置
ls -l deps/3rd/usr/local/oceanbase/deps/devel/lib/libpdfium.so
ls -l deps/3rd/usr/local/oceanbase/deps/devel/include/pdfium/fpdfview.h
ls -l deps/3rd/usr/local/oceanbase/deps/devel/include/stb_image_write.h

# 2. 配置构建（启用 lld 链接器）
cd build_debug
cmake .. -DOB_USE_LLD=ON -DCMAKE_BUILD_TYPE=Debug

# 3. 编译主项目
ob-make -j$(nproc)

# 4. 编译单元测试
ob-make test_ai_parse_document

# 5. 运行测试
./unittest/sql/engine/expr/test_ai_parse_document
```

---

## 十一、性能考虑

### 11.1 PDFium 初始化

- `FPDF_InitLibraryWithConfig()` 应在程序启动时调用一次
- `FPDF_DestroyLibrary()` 应在程序退出时调用
- 避免在每次转换时重复初始化

### 11.2 内存管理

- PNG 数据由 `stbi_write_png_to_mem()` 分配，需要调用 `free()` 释放
- BMP 数据使用 OceanBase 的 `ObIAllocator` 分配，生命周期由 allocator 管理
- 注意大文件处理时的内存峰值

### 11.3 DPI 设置

- 默认 150 DPI 平衡质量和性能
- 72 DPI：快速预览（文件小）
- 300 DPI：高质量打印（文件大）

---

## 十二、参考资料

- PDFium 官方文档：https://pdfium.googlesource.com/pdfium/
- stb_image_write：https://github.com/nothings/stb
- OceanBase 构建系统：`cmake/` 目录
- 参考实现：`/data/opt/pdf_to_png/pdf_to_png.cpp`

---

## 附录：完整的 CMakeLists.txt 配置示例

### src/sql/CMakeLists.txt (末尾添加)

```cmake
# ============== PDFium 库配置 ==============
# 作者：[您的名字]
# 日期：2025-01-XX
# 说明：集成 PDFium 库用于 PDF 文档解析

# 添加 PDFium 和 stb_image_write 头文件路径
target_include_directories(ob_sql PUBLIC
  ${DEP_DIR}/include/pdfium      # PDFium 头文件目录
  ${DEP_DIR}/include              # stb_image_write.h
)

# 链接 PDFium 库
target_link_libraries(ob_sql PUBLIC 
  ob_base
  ${DEP_DIR}/lib/libpdfium.so    # PDFium 动态库
)

# 设置 RPATH，确保运行时能找到 pdfium 动态库
set_target_properties(ob_sql PROPERTIES
  INSTALL_RPATH "${DEP_DIR}/lib"
  BUILD_RPATH "${DEP_DIR}/lib"
)
```

### unittest/sql/engine/expr/CMakeLists.txt (相应位置添加)

```cmake
# 为 test_ai_parse_document 添加 PDFium 库链接
target_link_libraries(test_ai_parse_document PRIVATE 
  ${DEP_DIR}/lib/libpdfium.so
)

target_include_directories(test_ai_parse_document PRIVATE
  ${DEP_DIR}/include/pdfium
  ${DEP_DIR}/include
)
```

---

**文档版本：** v1.0  
**最后更新：** 2025-01-29  
**维护者：** OceanBase AI 团队

