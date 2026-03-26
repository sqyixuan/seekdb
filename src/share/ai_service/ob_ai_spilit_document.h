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
 * This file contains implementation support for the ai split document.
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_SPLIT_DOCUMENT_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_SPLIT_DOCUMENT_H_
#include <string>
#include <vector>
#include <utility>
#include <algorithm>
#include <functional>
#include <iostream>
#include <cstring>
#include <regex>

namespace oceanbase
{
namespace share
{

/// to_chunks_with_order_guarantee
// trim helpers
static inline std::string rtrim_copy(const std::string& s) {
    std::string res = s;
    while (!res.empty() && (res.back() == ' ' || res.back() == '\t' || res.back() == '\r')) {
        res.pop_back();
    }
    return res;
}

static inline std::string ltrim_copy(const std::string& s) {
    std::string res = s;
    size_t i = 0;
    while (i < res.size() && (res[i] == ' ' || res[i] == '\t')) {
        ++i;
    }
    return res.substr(i);
}

// split by '\n' preserving structure
static inline std::vector<std::string> split_lines(const std::string& text) {
    std::vector<std::string> lines;
    size_t start = 0;
    while (start <= text.size()) {
        size_t pos = text.find('\n', start);
        if (pos == std::string::npos) {
            lines.emplace_back(text.substr(start));
            break;
        }
        lines.emplace_back(text.substr(start, pos - start));
        start = pos + 1;
    }
    return lines;
}

// header detector: must be at line start, exactly `level` #'s then a space
static inline std::pair<bool, std::string> is_header_of_level(const std::string& line, int level) {
    std::string right_trimmed = rtrim_copy(line);

    // must start at column 0 (no leading whitespace)
    if (right_trimmed != ltrim_copy(right_trimmed)) {
        return {false, ""};
    }

    // expect exact number of #'s followed by a space
    if (right_trimmed.size() < static_cast<size_t>(level + 1)) {
        return {false, ""};
    }
    for (int i = 0; i < level; ++i) {
        if (right_trimmed[i] != '#') {
            return {false, ""};
        }
    }
    if (right_trimmed[level] != ' ') {
        return {false, ""};
    }

    // extract title content (remove '#' * level + space)
    std::string title = right_trimmed.substr(level + 1);
    // trim remaining heading text
    // (匹配 Python 实现：只做右修剪来判定前导空白，标题文本做常规左右修剪更合理)
    // 但为保持一致性，仅去掉两端空白
    // 这里用简单的手动 trim
    auto trim = [](const std::string& s) {
        size_t b = 0, e = s.size();
        while (b < e && (s[b] == ' ' || s[b] == '\t')) ++b;
        while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t')) --e;
        return s.substr(b, e - b);
    };
    title = trim(title);
    return {true, title};
}

// Check if a line is a tag line like "text[[241, 436, 663, 459]]", "title[[241, 436, 663, 459]]", "sub_title[[241, 436, 663, 459]]", "table[[241, 436, 663, 459]]" or "image[[241, 436, 663, 459]]"
// Also returns true for empty lines and page split markers like "<--- Page Split --->"
static inline bool is_tag_or_empty_line(const std::string& line) {
    std::string trimmed = ltrim_copy(rtrim_copy(line));
    // Return true for empty lines
    if (trimmed.empty()) return true;
    
    // Check for page split marker "<--- Page Split --->"
    if (trimmed == "<--- Page Split --->") return true;
    
    // Check for "text[[...", "title[[...", "sub_title[[...", "table[[..." or "image[[..."
    if (trimmed.size() < 6) return false; // minimum: "text[]"
    
    // Check if starts with "text", "title", "sub_title", "table" or "image"
    bool starts_with_text = (trimmed.size() >= 6 && 
                             trimmed[0] == 't' && trimmed[1] == 'e' && 
                             trimmed[2] == 'x' && trimmed[3] == 't');
    bool starts_with_title = (trimmed.size() >= 7 && 
                              trimmed[0] == 't' && trimmed[1] == 'i' && 
                              trimmed[2] == 't' && trimmed[3] == 'l' && trimmed[4] == 'e');
    bool starts_with_table = (trimmed.size() >= 7 && 
                              trimmed[0] == 't' && trimmed[1] == 'a' && 
                              trimmed[2] == 'b' && trimmed[3] == 'l' && trimmed[4] == 'e');
    bool starts_with_image = (trimmed.size() >= 7 && 
                              trimmed[0] == 'i' && trimmed[1] == 'm' && 
                              trimmed[2] == 'a' && trimmed[3] == 'g' && trimmed[4] == 'e');
    bool starts_with_sub_title = (trimmed.size() >= 11 && 
                                  trimmed[0] == 's' && trimmed[1] == 'u' && 
                                  trimmed[2] == 'b' && trimmed[3] == '_' && 
                                  trimmed[4] == 't' && trimmed[5] == 'i' && 
                                  trimmed[6] == 't' && trimmed[7] == 'l' && trimmed[8] == 'e');
    
    if (!starts_with_text && !starts_with_title && !starts_with_table && !starts_with_image && !starts_with_sub_title) return false;
    
    // Find the position after "text", "title", "table", "image" or "sub_title"
    size_t prefix_len;
    if (starts_with_sub_title) {
        prefix_len = 9;
    } else if (starts_with_text) {
        prefix_len = 4;
    } else if (starts_with_title || starts_with_table || starts_with_image) {
        prefix_len = 5;
    } else {
        return false; // Should not reach here
    }
    if (trimmed.size() < prefix_len + 2) return false;
    
    // Check for "[["
    if (trimmed[prefix_len] != '[' || trimmed[prefix_len + 1] != '[') return false;
    
    // Check for closing "]]"
    size_t last_pos = trimmed.size() - 1;
    if (last_pos < prefix_len + 3) return false;
    if (trimmed[last_pos] != ']' || trimmed[last_pos - 1] != ']') return false;
    
    // Check if the content between [[ and ]] contains only digits, spaces, commas
    for (size_t i = prefix_len + 2; i < last_pos - 1; ++i) {
        char c = trimmed[i];
        if (c != ' ' && c != ',' && c != '\t' && !(c >= '0' && c <= '9')) {
            return false;
        }
    }
    
    return true;
}

// Clean text content: remove ref/det tags, replace LaTeX commands, normalize newlines
static inline std::string clean_text_content(const std::string& text) {
    if (text.empty()) return text;
    
    std::string result = text;
    
    // Remove ref/det tags: <|ref|>(.*?)<|/ref|><|det|>(.*?)<|/det|>
    // In raw string literal, \ needs to be escaped as \\, so \\| becomes \| in regex
    std::regex ref_det_pattern(R"(<\|ref\|>.*?<\|/ref\|><\|det\|>.*?<\|/det\|>)");
    result = std::regex_replace(result, ref_det_pattern, "");
    
    // Remove <--- Page Split ---> markers
    std::string page_split_marker = "<--- Page Split --->";
    size_t pos = 0;
    while ((pos = result.find(page_split_marker, pos)) != std::string::npos) {
        result.replace(pos, page_split_marker.length(), "");
        // Don't increment pos since we removed characters
    }
    
    // Replace \\coloneqq with :=
    std::string coloneqq_pattern = "\\\\coloneqq";
    pos = 0;
    while ((pos = result.find(coloneqq_pattern, pos)) != std::string::npos) {
        result.replace(pos, coloneqq_pattern.length(), ":=");
        pos += 2; // Move past the replacement
    }
    
    // Replace \\eqqcolon with =:
    std::string eqqcolon_pattern = "\\\\eqqcolon";
    pos = 0;
    while ((pos = result.find(eqqcolon_pattern, pos)) != std::string::npos) {
        result.replace(pos, eqqcolon_pattern.length(), "=:");
        pos += 2; // Move past the replacement
    }
    
    // Replace \n\n\n\n with \n\n
    std::string four_newlines = "\n\n\n\n";
    pos = 0;
    while ((pos = result.find(four_newlines, pos)) != std::string::npos) {
        result.replace(pos, four_newlines.length(), "\n\n");
        pos += 2; // Move past the replacement
    }
    
    // Replace \n\n\n with \n\n
    std::string three_newlines = "\n\n\n";
    pos = 0;
    while ((pos = result.find(three_newlines, pos)) != std::string::npos) {
        result.replace(pos, three_newlines.length(), "\n\n");
        pos += 2; // Move past the replacement
    }
    
    return result;
}

// C++ version of to_chunks_with_order_guarantee
// Return: pair {chunks, titles}
static inline std::pair<std::vector<std::string>, std::vector<std::string>>
to_chunks_with_order_guarantee(const std::string& md_content, int title_level) {
    if (md_content.empty() || title_level < 1 || title_level > 4) {
        if (!md_content.empty()) {
            std::string cleaned = clean_text_content(md_content);
            return {{cleaned}, {""}};
        }
        return {{}, {}};
    }

    // Clean the content first: remove ref/det tags, replace LaTeX commands, normalize newlines
    std::string cleaned_content = clean_text_content(md_content);
    std::vector<std::string> lines = split_lines(cleaned_content);
    std::vector<std::string> chunks;
    std::vector<std::string> titles;

    std::vector<std::string> current_chunk_lines;
    std::string current_title;

    auto flush_current_chunk = [&](size_t current_line_index_end_inclusive) {
        if (current_chunk_lines.empty()) return;
        std::string content;
        content.reserve(256);
        for (size_t i = 0; i < current_chunk_lines.size(); ++i) {
            content.append(current_chunk_lines[i]);
            if (i + 1 < current_chunk_lines.size()) {
                content.push_back('\n');
            }
        }
        // mimic Python's .strip(): remove leading/trailing whitespace including newlines/spaces/tabs
        auto strip = [](const std::string& s) {
            size_t b = 0, e = s.size();
            while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\n' || s[b] == '\r')) ++b;
            while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\n' || s[e - 1] == '\r')) --e;
            return s.substr(b, e - b);
        };
        content = strip(content);
        if (!content.empty()) {
            chunks.emplace_back(std::move(content));
            titles.emplace_back(current_title);
        }
        current_chunk_lines.clear();
    };

    for (size_t i = 0; i < lines.size(); ++i) {
        const std::string& line = lines[i];
        // Skip tag lines like "text[[...]]" or "title[[...]]", empty lines, and page split markers
        if (is_tag_or_empty_line(line)) {
            continue;
        }
        auto [is_header, title_content] = is_header_of_level(line, title_level);
        if (is_header) {
            // finish previous chunk
            flush_current_chunk(i == 0 ? 0 : (i - 1));
            // start new chunk with this header line
            // current_chunk_lines.clear();
            current_chunk_lines.push_back(line);
            current_title = title_content;
        } else {
            current_chunk_lines.push_back(line);
        }
    }

    // final chunk
    if (!current_chunk_lines.empty()) {
        flush_current_chunk(lines.empty() ? 0 : (lines.size() - 1));
    }

    // todo@dahzi: maybe donot get here
    // If no chunks were created (no headers found), return entire content as one chunk
    if (chunks.empty()) {
        // Filter out tag lines before returning (use cleaned_content which is already cleaned)
        std::vector<std::string> filtered_lines;
        std::vector<std::string> all_lines = split_lines(cleaned_content);
        for (const auto& line : all_lines) {
            if (!is_tag_or_empty_line(line)) {
                filtered_lines.push_back(line);
            }
        }
        // Reconstruct content without tag lines
        std::string content;
        content.reserve(cleaned_content.size());
        for (size_t i = 0; i < filtered_lines.size(); ++i) {
            content.append(filtered_lines[i]);
            if (i + 1 < filtered_lines.size()) {
                content.push_back('\n');
            }
        }
        auto strip = [](const std::string& s) {
            size_t b = 0, e = s.size();
            while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\n' || s[b] == '\r')) ++b;
            while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\n' || s[e - 1] == '\r')) --e;
            return s.substr(b, e - b);
        };
        content = strip(content);
        if (!content.empty()) {
            return {{content}, {""}};
        }
        return {{}, {}};
    }

    return {chunks, titles};
}

/// merge_with_title_chunks
// --------- 基础工具 ---------
static std::string ltrimCopy(const std::string& s) {
    size_t i = 0;
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
    return s.substr(i);
}
static std::string rtrimCopy(const std::string& s) {
    if (s.empty()) return s;
    size_t i = s.size();
    while (i > 0 && std::isspace(static_cast<unsigned char>(s[i - 1]))) --i;
    return s.substr(0, i);
}
static std::string trimCopy(const std::string& s) { return rtrimCopy(ltrimCopy(s)); }

// 计算 UTF-8 文本的 codepoint 数，作为 token 粗略估计（适配中英文）
static size_t utf8CodePointCount(const std::string& s) {
    size_t count = 0;
    for (size_t i = 0; i < s.size(); ) {
        unsigned char c = static_cast<unsigned char>(s[i]);
        if ((c & 0x80) == 0x00) { // 1-byte
            i += 1;
        } else if ((c & 0xE0) == 0xC0 && i + 1 < s.size()) { // 2-byte
            i += 2;
        } else if ((c & 0xF0) == 0xE0 && i + 2 < s.size()) { // 3-byte
            i += 3;
        } else if ((c & 0xF8) == 0xF0 && i + 3 < s.size()) { // 4-byte
            i += 4;
        } else {
            // 非法字节，跳过避免死循环
            i += 1;
        }
        ++count;
    }
    return count;
}

// 检查 text 是否以 title 开头（考虑 text 和 title 都可能包含 # 前缀）
// 此函数正确处理 UTF-8 编码，避免在字符中间截断
static bool is_start_with_title(const std::string& text, const std::string& title) {
    if (title.empty() || text.empty()) return false;
    
    // 处理 title：跳过前导的 # 和空白字符，获取标题内容
    size_t titlePos = 0;
    while (titlePos < title.size() && title[titlePos] == '#') {
        ++titlePos;
    }
    while (titlePos < title.size() && std::isspace(static_cast<unsigned char>(title[titlePos]))) {
        ++titlePos;
    }
    if (titlePos >= title.size()) {
        return false;
    }
    std::string titleContent = title.substr(titlePos);
    
    // 处理 text：跳过前导的 # 和空白字符
    size_t textPos = 0;
    while (textPos < text.size() && text[textPos] == '#') {
        ++textPos;
    }
    while (textPos < text.size() && std::isspace(static_cast<unsigned char>(text[textPos]))) {
        ++textPos;
    }
    
    // 检查剩余 text 是否以 titleContent 开头（在 UTF-8 字符边界处比较）
    if (text.size() - textPos < titleContent.size()) {
        return false;
    }
    
    // 使用 compare 进行字符串比较（在 UTF-8 字符边界处比较是安全的）
    return text.compare(textPos, titleContent.size(), titleContent) == 0;
}

// --------- 默认回调实现 ---------
// 1) numTokensFromString：以 UTF-8 codepoint 数作为近似 token 数
static size_t defaultNumTokensFromString(const std::string& text) {
    return utf8CodePointCount(text);
}

// 2) extractTitleFromMarkdown：提取不超过 titleLevel 的第一个 Markdown 标题
// 支持 ATX 风格：# / ## / ### 等
// todo@dazhi: 对于 ident 场景好像没有处理，需要处理
// todo@dazhi: 这个接口可能使用不到，需要确认
static std::string defaultExtractTitleFromMarkdown(const std::string& markdown, int titleLevel) {
    if (titleLevel <= 0) titleLevel = 1;
    size_t start = 0;
    int bestLevel = 1000000;
    std::string bestTitle;

    auto nextLine = [&](size_t from) -> std::pair<size_t, size_t> {
        if (from >= markdown.size()) return { std::string::npos, std::string::npos };
        size_t nl = markdown.find('\n', from);
        if (nl == std::string::npos) return { from, markdown.size() };
        return { from, nl };
    };

    while (true) {
        auto seg = nextLine(start);
        if (seg.first == std::string::npos) break;
        size_t l = seg.first, r = seg.second;
        std::string line = markdown.substr(l, r - l);
        std::string tline = ltrimCopy(line);

        // 统计前缀 # 的数量
        int hashes = 0;
        size_t i = 0;
        while (i < tline.size() && tline[i] == '#') { ++hashes; ++i; }
        if (hashes > 0 && i < tline.size() && std::isspace(static_cast<unsigned char>(tline[i]))) {
            if (hashes <= titleLevel && hashes < bestLevel) {
                std::string candidate = trimCopy(tline.substr(i + 1)); // 跳过一个空格
                if (!candidate.empty()) {
                    bestLevel = hashes;
                    bestTitle = candidate;
                    if (bestLevel == 1) break; // 已经是最高优先级
                }
            }
        }

        if (r == markdown.size()) break;
        start = r + 1;
    }
    return bestTitle;
}

// 3) isInProtectedRegion：避免在保护区（代码块/行内代码/数学块）处分割
// - 代码块：``` ... ```（语言标识行也视作开启）
// - 行内代码：` ... `
// - 数学：$$ ... $$ 或 $ ... $（简单启停，未处理复杂逃逸）
// 注意：此实现为启发式，覆盖常见 Markdown 模式
// todo@dazhi: 嵌套的情况好像没有处理，例如 $`$
// todo@dazhi: 可以补充下测试
static bool defaultIsInProtectedRegion(const std::string& text, size_t position) {
    bool inCodeBlock = false;          // ``` 三引号代码块
    bool inInlineCode = false;     // ` 行内代码 `
    bool inMathBlock = false;      // $$ 数学块 $$
    bool inMathInline = false;     // $ 行内数学 $

    size_t i = 0;
    auto aheadEq = [&](size_t p, const char* lit) {
        size_t n = strlen(lit);
        if (p + n > text.size()) return false;
        for (size_t k = 0; k < n; ++k) if (text[p + k] != lit[k]) return false;
        return true;
    };

    while (i < text.size() && i < position) {
        // 优先检查 ``` 和 $$
        if (!inInlineCode && !inMathInline) {
            if (!inCodeBlock && aheadEq(i, "```")) {
                inCodeBlock = true;
                i += 3;
                continue;
            } else if (inCodeBlock && aheadEq(i, "```")) {
                inCodeBlock = false;
                i += 3;
                continue;
            }
            if (!inMathBlock && aheadEq(i, "$$")) {
                inMathBlock = true;
                i += 2;
                continue;
            } else if (inMathBlock && aheadEq(i, "$$")) {
                inMathBlock = false;
                i += 2;
                continue;
            }
        }
        // 行内 code 与行内 math（避免与代码块/数学块冲突）
        if (!inCodeBlock && !inMathBlock) {
            if (!inInlineCode && text[i] == '`') {
                inInlineCode = true; ++i; continue;
            } else if (inInlineCode && text[i] == '`') {
                inInlineCode = false; ++i; continue;
            }
            if (!inMathInline && text[i] == '$') {
                inMathInline = true; ++i; continue;
            } else if (inMathInline && text[i] == '$') {
                inMathInline = false; ++i; continue;
            }
        }
        ++i;
    }

    return inCodeBlock || inInlineCode || inMathBlock || inMathInline;
}

// --------- 主逻辑：merge_with_title_chunks ---------
struct Section {
    std::string text;
    std::string title; // 可为空
};

static bool isTitleOnlyChunk(const std::string& chunkText, const std::string& title) {
    if (chunkText.empty() || title.empty()) return false;
    const std::string strippedChunk = trimCopy(chunkText);
    const std::string strippedTitle = trimCopy(title);
    if (strippedChunk.empty()) return false;
    if (strippedChunk == strippedTitle) return true;
    if (strippedChunk.size() >= strippedTitle.size() &&
        std::equal(strippedTitle.begin(), strippedTitle.end(), strippedChunk.begin())) {
        const std::string remainder = trimCopy(strippedChunk.substr(strippedTitle.size()));
        if (remainder.empty() || remainder == "\n") return true;
    }
    return false;
}

static std::vector<std::pair<std::string, std::string>> splitChunkByDelimiter(
    const std::string& text,
    size_t targetSize,
    const std::string& originalTitle,
    const std::string& delimiters,
    const std::function<size_t(const std::string&)>& numTokensFromString,
    const std::function<bool(const std::string&, size_t)>& isInProtectedRegion
) {
    if (numTokensFromString(text) <= targetSize) {
        if (!is_start_with_title(text, originalTitle)) {
            return { { originalTitle + "\n" + text, originalTitle } };
        }
        return { { text, originalTitle } };
    }

    for (char delim : delimiters) {
        std::vector<size_t> delimiterPositions;
        size_t start = 0;
        while (true) {
            size_t pos = text.find(delim, start);
            if (pos == std::string::npos) break;
            if (!isInProtectedRegion(text, pos)) {
                delimiterPositions.push_back(pos);
            }
            start = pos + 1;
        }
        if (delimiterPositions.empty()) {
            continue;
        }

        std::vector<std::pair<std::string, std::string>> result;
        std::string currentChunk;
        size_t lastPos = 0;

        auto flush = [&]() {
            if (currentChunk.empty()) return;
            std::string chunkWithTitle = currentChunk;
            if (!originalTitle.empty() && !is_start_with_title(currentChunk, originalTitle)) {
                chunkWithTitle = originalTitle + "\n" + currentChunk;
            }
            if (!isTitleOnlyChunk(chunkWithTitle, originalTitle)) {
                result.emplace_back(std::move(chunkWithTitle), originalTitle);
            }
            currentChunk.clear();
        };

        for (size_t pos : delimiterPositions) {
            std::string piece = text.substr(lastPos, pos - lastPos + 1); // 保留分隔符
            if (currentChunk.empty()) {
                currentChunk = std::move(piece);
            } else {
                std::string tentative = currentChunk + piece;
                if (numTokensFromString(tentative) <= targetSize) {
                    currentChunk = std::move(tentative);
                } else {
                    flush();
                    currentChunk = std::move(piece);
                }
            }
            lastPos = pos + 1;
        }

        if (lastPos < text.size()) {
            std::string remaining = text.substr(lastPos);
            if (currentChunk.empty()) {
                currentChunk = std::move(remaining);
            } else {
                std::string tentative = currentChunk + remaining;
                if (numTokensFromString(tentative) <= targetSize) {
                    currentChunk = std::move(tentative);
                } else {
                    flush();
                    currentChunk = std::move(remaining);
                }
            }
        }

        flush();

        if (result.size() > 1) {
            std::vector<std::pair<std::string, std::string>> filtered;
            filtered.reserve(result.size());
            for (auto& it : result) {
                const std::string& chunk = it.first;
                bool allSpace = std::all_of(chunk.begin(), chunk.end(), [](unsigned char c){ return std::isspace(c); });
                if (!chunk.empty() && !allSpace) {
                    filtered.push_back(std::move(it));
                }
            }
            if (!filtered.empty()) return filtered;
        }
    }

    return { { text, originalTitle } };
}

static std::vector<std::pair<std::string, std::string>> merge_with_title_chunks(
    const std::vector<Section>& sections,
    size_t chunkTokenNum = 512,
    const std::string& delimiters = "\n。；！？",
    int titleLevel = 3,
    const std::function<size_t(const std::string&)>& numTokensFromString = defaultNumTokensFromString,
    const std::function<std::string(const std::string&, int)>& extractTitleFromMarkdown = defaultExtractTitleFromMarkdown,
    const std::function<bool(const std::string&, size_t)>& isInProtectedRegion = defaultIsInProtectedRegion
) {
    std::vector<std::pair<std::string, std::string>> result;
    if (sections.empty()) return result;

    for (const auto& sec : sections) {
        const std::string& section = sec.text;
        const std::string& titleIn = sec.title;

        const size_t sectionTokens = numTokensFromString(section);
        const std::string extractedTitle =
            !titleIn.empty() ? titleIn : extractTitleFromMarkdown(section, titleLevel);

        if (sectionTokens > static_cast<size_t>(chunkTokenNum * 1.5)) {
            auto parts = splitChunkByDelimiter(
                section, chunkTokenNum, extractedTitle, delimiters,
                numTokensFromString, isInProtectedRegion
            );
            result.insert(result.end(), parts.begin(), parts.end());
        } else {
            if (!titleIn.empty() && !is_start_with_title(section, titleIn)) {
                result.emplace_back(titleIn + "\n" + section, titleIn);
            } else {
                result.emplace_back(section, extractedTitle);
            }
        }
    }
    return result;
}

static std::vector<std::pair<std::string, std::string>> merge_with_title_chunks(
    const std::vector<std::string>& sections,
    size_t chunkTokenNum = 512,
    const std::string& delimiters = "\n。；！？",
    int titleLevel = 3,
    const std::function<size_t(const std::string&)>& numTokensFromString = defaultNumTokensFromString,
    const std::function<std::string(const std::string&, int)>& extractTitleFromMarkdown = defaultExtractTitleFromMarkdown,
    const std::function<bool(const std::string&, size_t)>& isInProtectedRegion = defaultIsInProtectedRegion
) {
    std::vector<Section> wrapped;
    wrapped.reserve(sections.size());
    for (const auto& s : sections) wrapped.push_back(Section{ s, "" });
    return merge_with_title_chunks(
        wrapped, chunkTokenNum, delimiters, titleLevel,
        numTokensFromString, extractTitleFromMarkdown, isInProtectedRegion
    );
}


} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_SPLIT_DOCUMENT_H_