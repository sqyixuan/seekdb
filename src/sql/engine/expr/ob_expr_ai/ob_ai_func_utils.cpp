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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_ai_func_utils.h"
#include "ob_ai_func_client.h"
#include "pdfium/fpdfview.h"
#include "pdfium/fpdf_edit.h"

// 禁用第三方库的编译警告
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-field-initializers"
#endif

#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace oceanbase 
{
namespace common 
{

int ObOpenAIUtils::get_header(common::ObIAllocator &allocator,
                              ObString &api_key,
                              common::ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  if (api_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("API key is empty", K(ret));
  } else {
    //["Authorization: Bearer %.*s", "Content-Type: application/json"]
    int auth_header_len = 1024;
    char *auth_header_str = (char *)allocator.alloc(auth_header_len);
    ObString content_type_str("Content-Type: application/json");
    ObString content_type_c_str;
    if (OB_FAIL(ob_write_string(allocator, content_type_str, content_type_c_str, true))) {
      LOG_WARN("fail to write content type string", K(ret));
    } else if (OB_ISNULL(auth_header_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for auth header string and content type string", K(ret));
    } else {
      int auth_header_pos = snprintf(auth_header_str, auth_header_len,
                         "Authorization: Bearer %.*s", api_key.length(), api_key.ptr());
      if (auth_header_pos < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to snprintf", K(ret));
      } else if (OB_FAIL(headers.push_back(ObString(auth_header_str)))) {
        LOG_WARN("Failed to push back auth header", K(ret));
      } else if (OB_FAIL(headers.push_back(content_type_c_str))) {
        LOG_WARN("Failed to push back content type", K(ret));
      }
    }
  }
  return ret;
}
int ObOpenAIUtils::ObOpenAIComplete::get_header(common::ObIAllocator &allocator,
                                                ObString &api_key,
                                                common::ObArray<ObString> &headers) 
{
  return ObOpenAIUtils::get_header(allocator, api_key, headers);
}

int ObOpenAIUtils::ObOpenAIComplete::get_body(common::ObIAllocator &allocator,
                                              common::ObString &model,
                                              common::ObString &prompt,
                                              common::ObString &content,
                                              common::ObJsonObject *config,
                                              common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (model.empty() || content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or content is empty", K(ret));
  } else {
    // {"model": "*", "messages": [{"role": "system", "content": "*"}, {"role": "user", "content": "*"}]}
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonArray *messages_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(construct_messages_array(allocator, prompt, content, messages_array))) {
      LOG_WARN("Failed to construct messages", K(ret));
    } else if (OB_FAIL(body_obj->add("messages", messages_array))) {
      LOG_WARN("Failed to add messages", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
      LOG_WARN("Failed to compact json object", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIComplete::construct_messages_array(ObIAllocator &allocator, ObString &prompt, ObString &content, ObJsonArray *&messages)
{
  int ret = OB_SUCCESS;
  if (content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("content is empty", K(ret));
  } else {
    //messages: [{"role": "system", "content": "You are a helpful assistant."}, {"role": "user", "content": "What is the capital of France?"}]
    ObJsonArray *messages_array = nullptr;
    ObJsonObject *sys_message_obj = nullptr;
    ObJsonObject *user_message_obj = nullptr;
    ObString system_str("system");
    ObString user_str("user");
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, messages_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if(!prompt.empty()) {
      if (OB_FAIL(construct_message_obj(allocator, system_str, prompt, sys_message_obj))) {
        LOG_WARN("Failed to construct message object", K(ret));
      } else if (OB_FAIL(messages_array->append(sys_message_obj))) {
        LOG_WARN("Failed to append member", K(ret));
      }
    } 
    if (OB_SUCC(ret)) {
      if (OB_FAIL(construct_message_obj(allocator, user_str, content, user_message_obj))) {
        LOG_WARN("Failed to construct message object", K(ret));
      } else if (OB_FAIL(messages_array->append(user_message_obj))) {
        LOG_WARN("Failed to append member", K(ret));
      } else {
        messages = messages_array;
      }
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIComplete::construct_message_obj(ObIAllocator &allocator, ObString &role, ObString &content, ObJsonObject *&message)
{
  int ret = OB_SUCCESS;
  if (role.empty() || content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("role or content is empty", K(ret));
  } else {
    ObJsonObject *message_obj = nullptr;
    ObJsonString *role_json_str = nullptr;
    ObJsonString *content_json_str = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, message_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, role, role_json_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(message_obj->add("role", role_json_str))) {
      LOG_WARN("Failed to add member", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, content, content_json_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(message_obj->add("content", content_json_str))) {
      LOG_WARN("Failed to add member", K(ret));
    } else {
      message = message_obj;
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIComplete::set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", K(ret));
  } else {
    // {"response_format":{"type":"json_object"}}
    ObString json_str("json_object");
    ObJsonString *json_str_obj = nullptr;
    ObJsonObject *json_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, json_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, json_str, json_str_obj))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(json_obj->add("type", json_str_obj))) {
      LOG_WARN("Failed to add member", K(ret));
    } else if (OB_FAIL(config->add("response_format", json_obj))) {
      LOG_WARN("Failed to add member", K(ret));
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIComplete::parse_output(common::ObIAllocator &allocator, 
                                                  common::ObJsonObject *http_response,
                                                  common::ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObIJsonBase *j_tree = http_response;
    common::ObString path_text("$.choices[0].message.content");
    ObJsonPath j_path(path_text, &allocator);
    ObJsonSeekResult hit;
    if (OB_FAIL(j_path.parse_path())) {
      LOG_WARN("fail to parse path", K(ret));
    } else if (OB_FAIL(j_tree->seek(j_path, j_path.path_node_cnt(), false, false, hit))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit.size() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hit is empty", K(ret));
    } else {
      result = hit[0];
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIEmbed::get_header(common::ObIAllocator &allocator,
                                             ObString &api_key,
                                             common::ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIEmbed::get_body(common::ObIAllocator &allocator,
                                           common::ObString &model,
                                           common::ObArray<ObString> &contents,
                                           common::ObJsonObject *config,
                                           common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or contents is empty", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonArray *input_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::transform_array_to_json_array(allocator, contents, input_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_FAIL(body_obj->add("input", input_array))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
      LOG_WARN("Failed to compact json object", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObOpenAIUtils::ObOpenAIEmbed::parse_output(common::ObIAllocator &allocator, 
                                               common::ObJsonObject *http_response,
                                               common::ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObJsonArray *result_array = nullptr;
    ObJsonNode *data_node = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_ISNULL(data_node = http_response->get_value("data"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get data", K(ret));
    } else {
      ObJsonArray *data_array = static_cast<ObJsonArray *>(data_node);
      ObJsonNode *embedding_node = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < data_array->element_count(); i++) {
        if (OB_ISNULL(embedding_node = data_array->get_value(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Failed to get embedding", K(ret));
        } else if (embedding_node->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Failed to get embedding node", K(ret));
        } else {
          ObJsonObject *embedding_obj = static_cast<ObJsonObject *>(embedding_node);
          ObJsonNode *embedding = embedding_obj->get_value("embedding");
          if (OB_ISNULL(embedding)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to get embedding", K(ret));
          } else if (OB_FAIL(result_array->append(embedding))) {
            LOG_WARN("Failed to append embedding", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        result = result_array;
      }
    }
  }
  return ret;
}

int ObDeepSeekUtils::ObDeepSeekParseDocument::get_header(common::ObIAllocator &allocator,
                                        common::ObString &api_key,
                                        common::ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  //["Authorization: Bearer %.*s", "Content-Type: application/json"]
  ObString content_type_str("Content-Type: application/json");
  ObString content_type_c_str;
  if (OB_FAIL(ob_write_string(allocator, content_type_str, content_type_c_str, true))) {
    LOG_WARN("fail to write content type string", K(ret));
  } else if (OB_FAIL(headers.push_back(content_type_c_str))) {
    LOG_WARN("Failed to push back content type", K(ret));
  } else if (api_key.empty()) {
  } else {
    int auth_header_len = 1024;
    char *auth_header_str = static_cast<char *>(allocator.alloc(auth_header_len));
    if (OB_ISNULL(auth_header_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for auth header string", K(ret));
    } else {
      int auth_header_pos = snprintf(auth_header_str, auth_header_len,
                          "Authorization: Bearer %.*s", api_key.length(), api_key.ptr());
      if (auth_header_pos < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to snprintf", K(ret));
      } else if (OB_FAIL(headers.push_back(auth_header_str))) {
        LOG_WARN("Failed to push back auth header", K(ret));
      }
    }
  }  
  return ret;
}

int ObDeepSeekUtils::ObDeepSeekParseDocument::get_body(common::ObIAllocator &allocator,
                                      common::ObString &model,
                                      common::ObString &image,
                                      bool is_base64_encoded,
                                      bool is_markdown,
                                      common::ObJsonObject *config,
                                      common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (model.empty() || image.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or image is empty", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonArray *messages_array = nullptr;
    // {"model": "default", "messages": [ { "role": "user", "content": [ { "type": "image_url", "image_url": { "url": "替换成你的base64串" } }, { "type": "text", "text": "<|grounding|>Convert the document to markdown." } ] } ]}
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(construct_messages_array(allocator, image, is_base64_encoded, is_markdown, messages_array))) {
      LOG_WARN("Failed to construct messages", K(ret));
    } else if (OB_FAIL(body_obj->add("messages", messages_array))) {
      LOG_WARN("Failed to add messages", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObDeepSeekUtils::ObDeepSeekParseDocument::construct_messages_array(ObIAllocator &allocator, ObString &image, bool is_base64_encoded, bool is_markdown, ObJsonArray *&messages_array)
{
  int ret = OB_SUCCESS;
  if (image.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Image is empty", K(ret));
  } else {
    // "messages": [ { "role": "user", "content": [ { "type": "image_url", "image_url": { "url": "替换成你的base64串" } }, { "type": "text", "text": "<|grounding|>Convert the document to markdown." } ] } ]
    ObString text;
    if (is_markdown) {
      text = "<|grounding|>Convert the document to markdown.";
    } else {
      text = "Free OCR.";
    }
    ObString role("user");
    ObJsonArray *tmp_messages_array = nullptr;
    ObJsonObject *tmp_message_obj = nullptr;
    ObJsonString *role_json_str = nullptr;
    ObJsonArray *content_json_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, tmp_messages_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, tmp_message_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, role, role_json_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(tmp_message_obj->add("role", role_json_str))) {
      LOG_WARN("Failed to add role", K(ret));
    } else if (OB_FAIL(construct_content_array(allocator, image, is_base64_encoded, text, content_json_array))) {
      LOG_WARN("Failed to construct content array", K(ret));
    } else if (OB_FAIL(tmp_message_obj->add("content", content_json_array))) {
      LOG_WARN("Failed to add content", K(ret));
    } else if (OB_FAIL(tmp_messages_array->append(tmp_message_obj))) {
      LOG_WARN("Failed to append message object", K(ret));
    } else {
      messages_array = tmp_messages_array;
    }
  }
  return ret;
}

int ObDeepSeekUtils::ObDeepSeekParseDocument::construct_content_array(ObIAllocator &allocator, ObString &image, bool is_base64_encoded, ObString &text, ObJsonArray *&content_json_array)
{
  // [ { "type": "image_url", "image_url": { "url": "替换成你的base64串" } }, { "type": "text", "text": "<|grounding|>Convert the document to markdown." } ]
  int ret = OB_SUCCESS;
  ObJsonArray *tmp_content_json_array = nullptr;
  ObJsonObject *image_json_obj = nullptr;
  ObJsonObject *text_json_obj = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, tmp_content_json_array))) {
    LOG_WARN("Failed to get json array", K(ret));
  } else if (OB_FAIL(construct_image_obj(allocator, image, is_base64_encoded, image_json_obj))) {
    LOG_WARN("Failed to construct image object", K(ret));
  } else if (OB_FAIL(tmp_content_json_array->append(image_json_obj))) {
    LOG_WARN("Failed to append content", K(ret));
  } else if (OB_FAIL(construct_text_obj(allocator, text, text_json_obj))) {
    LOG_WARN("Failed to construct text object", K(ret));
  } else if (OB_FAIL(tmp_content_json_array->append(text_json_obj))) {
    LOG_WARN("Failed to append content", K(ret));
  } else {
    content_json_array = tmp_content_json_array;
  }
  return ret;
}

int ObDeepSeekUtils::ObDeepSeekParseDocument::construct_image_obj(ObIAllocator &allocator, ObString &image, bool is_base64_encoded, ObJsonObject *&image_obj)
{
  // { "type": "image_url", "image_url": { "url": "your image_url_jstr" } }
  int ret = OB_SUCCESS;
  if (image.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Image is empty", K(ret));
  }
  ObJsonObject *image_current_obj = nullptr;
  ObJsonString *image_url_str_key_jstr = nullptr;
  ObString image_url_str_key("image_url");
  
  /*{"type": "image_url"} */
  /* add key*/
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, image_current_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, image_url_str_key, image_url_str_key_jstr))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(image_current_obj->add("type", image_url_str_key_jstr))) {
      LOG_WARN("Failed to add type", K(ret));
    }
  }
  /* add value */
  if (OB_SUCC(ret)) {
    ObString image_url_str;
    ObJsonString *image_url_jstr = nullptr;
    ObJsonObject *image_url_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, image_url_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(get_image_url_string(allocator, image, is_base64_encoded, image_url_str))) {
      LOG_WARN("Failed to get image url string", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, image_url_str, image_url_jstr))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(image_url_obj->add("url", image_url_jstr))) {
      LOG_WARN("Failed to add url", K(ret));
    } else if (OB_FAIL(image_current_obj->add("image_url", image_url_obj))) {
      LOG_WARN("Failed to add image_url", K(ret));
    }
  }
  
  if (OB_SUCC(ret)) {
    image_obj = image_current_obj;
  }
  return ret;
}

int ObDeepSeekUtils::ObDeepSeekParseDocument::get_image_url_string(ObIAllocator &allocator, ObString &image, bool is_base64_encoded, ObString &image_url_str)
{
  int ret = OB_SUCCESS;
  if (image.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Image is empty", K(ret));
  }
  /*{"url": "https://developer.qcloudimg.com/http-save/yehe-5426717/3f411ffc5e8c36055049bbe412d22761.png"} */
  ObString url_str;
  if (OB_SUCC(ret)) {
    if (is_base64_encoded) {
      //"data:image/png;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAA..."
      ObStringBuffer url_str_buffer(&allocator);
      url_str_buffer.reserve(image.length() + 64);
      if (OB_FAIL(url_str_buffer.append("data:image/png;base64,"))) {
        LOG_WARN("Failed to append data:image/png;base64,", K(ret));
      } else if (OB_FAIL(url_str_buffer.append(image))) {
        LOG_WARN("Failed to append image", K(ret));
      } else {
        url_str = url_str_buffer.string();
      }
    } else {
      /*{"url": "https://developer.qcloudimg.com/http-save/yehe-5426717/3f411ffc5e8c36055049bbe412d22761.png"} */
      /* do nothing */
      url_str = image;
    }
  }

  if (OB_SUCC(ret)) {
    image_url_str = url_str;
  }
  return ret;
}

int ObDeepSeekUtils::ObDeepSeekParseDocument::construct_text_obj(ObIAllocator &allocator, ObString &text, ObJsonObject *&text_obj)
{
  // { "type": "text", "text": "<|grounding|>Convert the document to markdown." }
  int ret = OB_SUCCESS;
  if (text.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Text is empty", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObJsonString *type_str = nullptr;
    ObJsonString *text_str = nullptr;
    ObString type_value = ObString::make_string("text");
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, text_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, type_value, type_str))) {
      LOG_WARN("Failed to get type string", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, text, text_str))) {
      LOG_WARN("Failed to get text string", K(ret));
    } else if (OB_FAIL(text_obj->add("type", type_str))) {
      LOG_WARN("Failed to add type", K(ret));
    } else if (OB_FAIL(text_obj->add("text", text_str))) {
      LOG_WARN("Failed to add text", K(ret));
    }
  }
  return ret;
}


int ObDeepSeekUtils::ObDeepSeekParseDocument::parse_output(common::ObIAllocator &allocator,
                                          common::ObJsonObject *http_response,
                                          common::ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObIJsonBase *j_tree = http_response;
    common::ObString path_text("$.choices[0].message.content");
    ObJsonPath j_path(path_text, &allocator);
    ObJsonSeekResult hit;
    if (OB_FAIL(j_path.parse_path())) {
      LOG_WARN("fail to parse path", K(ret));
    } else if (OB_FAIL(j_tree->seek(j_path, j_path.path_node_cnt(), false, false, hit))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit.size() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find content in response, hit is empty", K(ret), K(path_text));
    } else {
      result = hit[0];
    }
  }
  return ret;
}

int ObOllamaUtils::get_header(common::ObIAllocator &allocator,
                              common::ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  // ollama header is empty, do nothing
  return ret;
}

int ObOllamaUtils::ObOllamaComplete::get_header(common::ObIAllocator &allocator,
                                                common::ObString &api_key,
                                                common::ObArray<ObString> &headers) 
{
  return ObOllamaUtils::get_header(allocator, headers);
}

int ObOllamaUtils::ObOllamaComplete::get_body(common::ObIAllocator &allocator,
                                              common::ObString &model,
                                              common::ObString &prompt,
                                              common::ObString &content,
                                              common::ObJsonObject *config,
                                              common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (model.empty() || content.empty()) { 
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or content is empty", K(ret));
  } else {
    // {"model": "llama3.1", "prompt": "What is the capital of France?"}
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonString *content_str = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, content, content_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("prompt", content_str))) {
      LOG_WARN("Failed to add prompt", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
      LOG_WARN("Failed to compact json object", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObOllamaUtils::ObOllamaComplete::set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", K(ret));
  } else {
    // {"format": "json"}
    ObString json_str("json");
    ObJsonString *json_str_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, json_str, json_str_obj))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(config->add("format", json_str_obj))) {
      LOG_WARN("Failed to add format", K(ret));
    } 
  }
  return ret;
}

int ObOllamaUtils::ObOllamaComplete::parse_output(common::ObIAllocator &allocator,
                                                  common::ObJsonObject *http_response,
                                                  common::ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObIJsonBase *j_tree = http_response;
    common::ObString path_text("$.response");
    ObJsonPath j_path(path_text, &allocator);
    ObJsonSeekResult hit;
    if (OB_FAIL(j_path.parse_path())) {
      LOG_WARN("fail to parse path", K(ret));
    } else if (OB_FAIL(j_tree->seek(j_path, j_path.path_node_cnt(), false, false, hit))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit.size() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hit is empty", K(ret));
    } else {
      result = hit[0];
    }
  }
  return ret;
}

int ObOllamaUtils::ObOllamaEmbed::get_header(common::ObIAllocator &allocator,
                                             ObString &api_key,
                                             common::ObArray<ObString> &headers) 
{
  return ObOllamaUtils::get_header(allocator, headers);
}

int ObOllamaUtils::ObOllamaEmbed::get_body(common::ObIAllocator &allocator,
                                           common::ObString &model,
                                           common::ObArray<ObString> &contents,
                                           common::ObJsonObject *config,
                                           common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (model.empty() || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Model name or contents is empty", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonArray *input_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::transform_array_to_json_array(allocator, contents, input_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_FAIL(body_obj->add("input", input_array))) {
      LOG_WARN("Failed to add input", K(ret));
    } else {
      body = body_obj;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body))) {
        LOG_WARN("Failed to compact json object", K(ret));
      }
    }
  }
  return ret;
}

int ObOllamaUtils::ObOllamaEmbed::parse_output(common::ObIAllocator &allocator,
                                               common::ObJsonObject *http_response,
                                               common::ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObIJsonBase *j_tree = http_response;
    common::ObString path_text("$.embeddings");
    ObJsonPath j_path(path_text, &allocator);
    ObJsonSeekResult hit;
    if (OB_FAIL(j_path.parse_path())) {
      LOG_WARN("fail to parse path", K(ret));
    } else if (OB_FAIL(j_tree->seek(j_path, j_path.path_node_cnt(), false, false, hit))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit.size() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hit is empty", K(ret));
    } else {
      result = hit[0];
    }
  }
  return ret;
}

int ObDashscopeUtils::get_header(common::ObIAllocator &allocator,
                                 common::ObString &api_key,
                                 common::ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::get_header(common::ObIAllocator &allocator, 
                                                      common::ObString &api_key,
                                                      common::ObArray<ObString> &headers) 
{
  return ObOpenAIUtils::get_header(allocator, api_key, headers);
}

int ObDashscopeUtils::ObDashscopeComplete::get_body(common::ObIAllocator &allocator,
                                                    common::ObString &model,
                                                    common::ObString &prompt,
                                                    common::ObString &content,
                                                    common::ObJsonObject *config,
                                                    common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(model) || OB_ISNULL(content)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or content is null", K(ret));
  } else {
    // {"model": "*", "input": {"messages": [{"role": "system", "content": "*"}, {"role": "user", "content": "*"}]}, "parameters": {}}
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;

    if (OB_ISNULL(config)) {
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
        LOG_WARN("Failed to get json object", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
        LOG_WARN("Failed to get json object", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
        LOG_WARN("Failed to get json string", K(ret));
      } else if (OB_FAIL(body_obj->add("model", model_str))) {
        LOG_WARN("Failed to add model", K(ret));
      } else if (OB_FAIL(ObDashscopeUtils::ObDashscopeComplete::construct_input_obj(allocator, prompt, content, input_obj))) {
        LOG_WARN("Failed to construct input object", K(ret));
      } else if (OB_FAIL(body_obj->add("input", input_obj))) {
        LOG_WARN("Failed to add input", K(ret));
      } else if (OB_FAIL(ObDashscopeUtils::ObDashscopeComplete::set_config_result_format(allocator, config))) {
        LOG_WARN("Failed to set config result format", K(ret));
      } else if (OB_FAIL(body_obj->add("parameters", config))) {
        LOG_WARN("Failed to add parameters", K(ret));
      } else {
        body = body_obj;
      }
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::set_config_result_format(ObIAllocator &allocator, ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", K(ret));
  } else if (OB_NOT_NULL(config->get_value("result_format"))) {
    if (OB_FAIL(config->remove("result_format"))) {
      LOG_WARN("Failed to remove result_format", K(ret));
    }
  } 
  if (OB_SUCC(ret)) {
    // {"result_format": "message"}
    ObJsonString *result_format_str = nullptr;
    ObString result_format_str_val("message");
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, result_format_str_val, result_format_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(config->add("result_format", result_format_str))) {
      LOG_WARN("Failed to add result_format", K(ret));
    }
  } 
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::construct_input_obj(ObIAllocator &allocator, ObString &prompt, ObString &content, ObJsonObject *&input_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(content)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("content is null", K(ret));
  } else {
    ObJsonObject *obj = nullptr;
    ObJsonArray *messages_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObOpenAIUtils::ObOpenAIComplete::construct_messages_array(allocator, prompt, content, messages_array))) {
      LOG_WARN("Failed to construct messages array", K(ret));
    } else if (OB_FAIL(obj->add("messages", messages_array))) {
      LOG_WARN("Failed to add messages", K(ret));
    } else {
      input_obj = obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::set_config_json_format(ObIAllocator &allocator, ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", K(ret));
  } else {
    //{"type": "json_object"}
    ObJsonString *type_str = nullptr;
    ObString type_str_val("json_object");
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, type_str_val, type_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(config->add("type", type_str))) {
      LOG_WARN("Failed to add type", K(ret));
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeComplete::parse_output(ObIAllocator &allocator, ObJsonObject *http_response, ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    ObJsonObject *output_obj = nullptr;
    ObJsonArray *choices_array = nullptr;
    ObJsonObject *choice_obj = nullptr;
    ObJsonObject *message_obj = nullptr;
    ObJsonString *content_str = nullptr;
    ObString response_str;
    if (OB_ISNULL(output_obj = static_cast<ObJsonObject *>(http_response->get_value("output")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output_obj is null", K(ret));
    } else if (OB_ISNULL(choices_array = static_cast<ObJsonArray *>(output_obj->get_value("choices")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("choices_array is null", K(ret));
    } else if (OB_ISNULL(choice_obj = static_cast<ObJsonObject *>(choices_array->get_value(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("choice_obj is null", K(ret));
    } else if (OB_ISNULL(message_obj = static_cast<ObJsonObject *>(choice_obj->get_value("message")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("message_obj is null", K(ret));
    } else if (OB_ISNULL(content_str = static_cast<ObJsonString *>(message_obj->get_value("content")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("content_str is null", K(ret));
    } else {
      result = content_str;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeEmbed::get_header(common::ObIAllocator &allocator,
                                                   common::ObString &api_key,
                                                   common::ObArray<ObString> &headers) 
{
  return ObDashscopeUtils::get_header(allocator, api_key, headers);
}

int ObDashscopeUtils::ObDashscopeEmbed::get_body(common::ObIAllocator &allocator,
                                                 common::ObString &model,
                                                 common::ObArray<ObString> &contents,
                                                 common::ObJsonObject *config,
                                                 common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(model) || contents.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or contents is empty", K(ret));
  } else {
    // {"model": "*", "input": {"texts": ["*"]}, "parameters": {}}
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;
    ObJsonArray *texts_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, input_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::transform_array_to_json_array(allocator, contents, texts_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_FAIL(input_obj->add("texts", texts_array))) {
      LOG_WARN("Failed to add texts", K(ret));
    } else if (OB_FAIL(body_obj->add("input", input_obj))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (config != nullptr && config->element_count() > 0) {
      ObJsonNode *dimensions_node = config->get_value("dimensions");
      if (OB_ISNULL(dimensions_node)) {
        // do nothing
      } else if (OB_FAIL(config->rename_key("dimensions", "dimension"))) {
        LOG_WARN("Failed to rename key", K(ret));
      } 
      if (OB_SUCC(ret)) {
        if (OB_FAIL(body_obj->add("parameters", config))) {
          LOG_WARN("Failed to add parameters", K(ret));
        }
      }
    } 
    if (OB_SUCC(ret)) {
      body = body_obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeEmbed::parse_output(common::ObIAllocator &allocator,
                                                    common::ObJsonObject *http_response,
                                                    common::ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else {
    // {"output": {"embeddings": [{"embedding": ["*"]}]}}
    ObJsonObject *output_obj = nullptr;
    ObJsonArray *embeddings_array = nullptr;
    ObJsonObject *embedding_obj = nullptr;
    ObJsonArray *embedding_array = nullptr;
    ObJsonArray *result_array = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    } else if (OB_ISNULL(output_obj = static_cast<ObJsonObject *>(http_response->get_value("output")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output_obj is null", K(ret));
    } else if (OB_ISNULL(embeddings_array = static_cast<ObJsonArray *>(output_obj->get_value("embeddings")))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("embeddings_array is null", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < embeddings_array->element_count(); ++i) {
        if (OB_ISNULL(embedding_obj = static_cast<ObJsonObject *>(embeddings_array->get_value(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("embedding_obj is null", K(ret));
        } else if (OB_ISNULL(embedding_array = static_cast<ObJsonArray *>(embedding_obj->get_value("embedding")))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("embedding_array is null", K(ret));
        } else if (OB_FAIL(result_array->append(embedding_array))) {
          LOG_WARN("Failed to append embedding array", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        result = result_array;
      }
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeRerank::get_header(common::ObIAllocator &allocator,
                                                    common::ObString &api_key,
                                                    common::ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeRerank::get_body(common::ObIAllocator &allocator,
                                                  common::ObString &model,
                                                  common::ObString &query,
                                                  common::ObJsonArray *document_array,
                                                  common::ObJsonObject *config,
                                                  common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(model) || OB_ISNULL(query) || OB_ISNULL(document_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or query or document_array is null", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonObject *input_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObDashscopeUtils::ObDashscopeRerank::get_input_obj(allocator, query, document_array, input_obj))) {
      LOG_WARN("Failed to get input object", K(ret));
    } else if (OB_FAIL(body_obj->add("input", input_obj))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (config != nullptr && config->element_count() > 0) {
      if (OB_FAIL(body_obj->add("parameters", config))) {
        LOG_WARN("Failed to add parameters", K(ret));
      }
    } 
    if (OB_SUCC(ret)) {
      body = body_obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeRerank::get_input_obj(common::ObIAllocator &allocator,
                                                      common::ObString &query,
                                                      common::ObJsonArray *document_array,
                                                      common::ObJsonObject *&input_obj) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query) || OB_ISNULL(document_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("query or document_array is null", K(ret));
  } else {
    ObJsonObject *obj = nullptr;
    ObJsonString *query_str = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, query, query_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(obj->add("query", query_str))) {
      LOG_WARN("Failed to add query", K(ret));
    } else if (OB_FAIL(obj->add("documents", document_array))) {
      LOG_WARN("Failed to add documents", K(ret));
    } else {
      input_obj = obj;
    }
  }
  return ret;
}

int ObDashscopeUtils::ObDashscopeRerank::parse_output(common::ObIAllocator &allocator,
                                                      common::ObJsonObject *http_response,
                                                      common::ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  ObJsonObject *output_obj = nullptr;
  ObJsonArray *results_array = nullptr;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else if (OB_ISNULL(output_obj = static_cast<ObJsonObject *>(http_response->get_value("output")))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output_obj is null", K(ret));
  } else if (OB_ISNULL(results_array = static_cast<ObJsonArray *>(output_obj->get_value("results")))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results_array is null", K(ret));
  } else {
    result = results_array;
  }
  return ret;
}

int ObSiliconflowUtils::get_header(common::ObIAllocator &allocator,
                                 common::ObString &api_key,
                                 common::ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObSiliconflowUtils::ObSiliconflowRerank::get_header(common::ObIAllocator &allocator,
                                                    common::ObString &api_key,
                                                    common::ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpenAIUtils::get_header(allocator, api_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  }
  return ret;
}

int ObSiliconflowUtils::ObSiliconflowRerank::get_body(common::ObIAllocator &allocator,
                                                  common::ObString &model,
                                                  common::ObString &query,
                                                  common::ObJsonArray *document_array,
                                                  common::ObJsonObject *config,
                                                  common::ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(model) || OB_ISNULL(query) || OB_ISNULL(document_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model or query or document_array is null", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonString *model_str = nullptr;
    ObJsonString *query_str = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model, model_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("model", model_str))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, query, query_str))) {
      LOG_WARN("Failed to get json string", K(ret));
    } else if (OB_FAIL(body_obj->add("query", query_str))) {
      LOG_WARN("Failed to add query", K(ret));
    } else if (OB_FAIL(body_obj->add("documents", document_array))) {
      LOG_WARN("Failed to add documents", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::compact_json_object(allocator, config, body_obj))) {
      LOG_WARN("Failed to compact json object", K(ret));  
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObSiliconflowUtils::ObSiliconflowRerank::parse_output(common::ObIAllocator &allocator,
                                                      common::ObJsonObject *http_response,
                                                      common::ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  ObJsonArray *results_array = nullptr;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else if (OB_ISNULL(results_array = static_cast<ObJsonArray *>(http_response->get_value("results")))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results_array is null", K(ret));
  } else {
    result = results_array;
  }
  return ret;
}


int ObAIFuncUtils::get_header(ObIAllocator &allocator, 
                              const ObAIFuncExprInfo &info,
                              const ObAiModelEndpointInfo &endpoint_info,
                              ObArray<ObString> &headers) 
{
  int ret = OB_SUCCESS;
  ObString unencrypted_access_key;
  if (OB_FAIL(endpoint_info.get_unencrypted_access_key(allocator, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (ObAIFuncUtils::is_completion_type(&info)) {
    ObAIFuncIComplete *complete_provider = nullptr;
    if (OB_FAIL(get_complete_provider(allocator, endpoint_info.get_provider(), complete_provider))) {
      LOG_WARN("Failed to get complete provider", K(ret));
    } else if (OB_FAIL(complete_provider->get_header(allocator, unencrypted_access_key, headers))) {
      LOG_WARN("Failed to get header from complete provider", K(ret));
    }
  } else if (ObAIFuncUtils::is_dense_embedding_type(&info)) {
    ObAIFuncIEmbed *embed_provider = nullptr;
    if (OB_FAIL(get_embed_provider(allocator, endpoint_info.get_provider(), embed_provider))) {
      LOG_WARN("Failed to get embed provider", K(ret));
    } else if (OB_FAIL(embed_provider->get_header(allocator, unencrypted_access_key, headers))) {
      LOG_WARN("Failed to get header from embed provider", K(ret));
    }
  } else if (ObAIFuncUtils::is_rerank_type(&info)) {
    ObAIFuncIRerank *rerank_provider = nullptr;
    if (OB_FAIL(get_rerank_provider(allocator, endpoint_info.get_provider(), rerank_provider))) {
      LOG_WARN("Failed to get rerank provider", K(ret));
    } else if (OB_FAIL(rerank_provider->get_header(allocator, unencrypted_access_key, headers))) {
      LOG_WARN("Failed to get header from rerank provider", K(ret));
    }
  }
  return ret;
}

int ObAIFuncUtils::get_complete_body(ObIAllocator &allocator, 
                                    const ObAIFuncExprInfo &info, 
                                    const ObAiModelEndpointInfo &endpoint_info,
                                    ObString &prompt, 
                                    ObString &content, 
                                    ObJsonObject *config,
                                    ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  ObString request_model_name = info.model_;
  if (!endpoint_info.get_request_model_name().empty()) {
    request_model_name = endpoint_info.get_request_model_name();
  }

  ObAIFuncIComplete *complete_provider = nullptr;
  if (OB_FAIL(get_complete_provider(allocator, endpoint_info.get_provider(), complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(complete_provider->get_body(allocator, request_model_name, prompt, content, config, body))) {
    LOG_WARN("Failed to get body from complete provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::set_json_format_config(ObIAllocator &allocator, const ObString &provider, ObJsonObject *config)
{
  int ret = OB_SUCCESS;
  ObAIFuncIComplete *complete_provider = nullptr;
  if (OB_FAIL(get_complete_provider(allocator, provider, complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(complete_provider->set_config_json_format(allocator, config))) {
    LOG_WARN("Failed to set json format config from complete provider", K(ret));
  } 
  return ret;
}

int ObAIFuncUtils::get_embed_body(ObIAllocator &allocator, 
                                  const ObAIFuncExprInfo &info, 
                                  const ObAiModelEndpointInfo &endpoint_info,
                                  ObArray<ObString> &contents, 
                                  ObJsonObject *config,
                                  ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  ObString request_model_name = info.model_;
  if (!endpoint_info.get_request_model_name().empty()) {
    request_model_name = endpoint_info.get_request_model_name();
  }

  ObAIFuncIEmbed *embed_provider = nullptr;
  if (OB_FAIL(get_embed_provider(allocator, endpoint_info.get_provider(), embed_provider))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(embed_provider->get_body(allocator, request_model_name, contents, config, body))) {
    LOG_WARN("Failed to get body from embed provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_rerank_body(ObIAllocator &allocator, 
                                   const ObAIFuncExprInfo &info,
                                   const ObAiModelEndpointInfo &endpoint_info,
                                   ObString &query,
                                   ObJsonArray *document_array,
                                   ObJsonObject *config,
                                   ObJsonObject *&body) 
{
  int ret = OB_SUCCESS;
  ObString request_model_name = info.model_;
  if (!endpoint_info.get_request_model_name().empty()) {
    request_model_name = endpoint_info.get_request_model_name();
  }

  ObAIFuncIRerank *rerank_provider = nullptr;
  if (OB_FAIL(get_rerank_provider(allocator, endpoint_info.get_provider(), rerank_provider))) {
    LOG_WARN("Failed to get rerank provider", K(ret));
  } else if (OB_FAIL(rerank_provider->get_body(allocator, request_model_name, query, document_array, config, body))) {
    LOG_WARN("Failed to get body from rerank provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::parse_complete_output(ObIAllocator &allocator, 
                                        const ObAiModelEndpointInfo &endpoint_info,
                                        ObJsonObject *http_response,
                                        ObIJsonBase *&result)                                                 
{
  int ret = OB_SUCCESS;
  ObAIFuncIComplete *complete_provider = nullptr;
  if (OB_FAIL(get_complete_provider(allocator, endpoint_info.get_provider(), complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(complete_provider->parse_output(allocator, http_response, result))) {
    LOG_WARN("Failed to parse output from complete provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::parse_embed_output(ObIAllocator &allocator, 
                                      const ObAiModelEndpointInfo &endpoint_info,
                                      ObJsonObject *http_response,
                                      ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  ObAIFuncIEmbed *embed_provider = nullptr;
  if (OB_FAIL(get_embed_provider(allocator, endpoint_info.get_provider(), embed_provider))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(embed_provider->parse_output(allocator, http_response, result))) {
    LOG_WARN("Failed to parse output from embed provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::parse_rerank_output(ObIAllocator &allocator, 
                                       const ObAiModelEndpointInfo &endpoint_info,
                                       ObJsonObject *http_response,
                                       ObIJsonBase *&result) 
{
  int ret = OB_SUCCESS;
  ObAIFuncIRerank *rerank_provider = nullptr;
  if (OB_FAIL(get_rerank_provider(allocator, endpoint_info.get_provider(), rerank_provider))) {
    LOG_WARN("Failed to get rerank provider", K(ret));
  } else if (OB_FAIL(rerank_provider->parse_output(allocator, http_response, result))) {
    LOG_WARN("Failed to parse output from rerank provider", K(ret));
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_object(ObIAllocator &allocator, ObJsonObject *&obj_node)
{
  int ret = OB_SUCCESS;
  ObJsonObject *j_obj = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(j_obj)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_obj", K(ret));
  } else {
    obj_node = j_obj;
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_array(ObIAllocator &allocator, ObJsonArray *&array_node)
{
  int ret = OB_SUCCESS;
  ObJsonArray *j_array = OB_NEWx(ObJsonArray, &allocator, &allocator);
  if (OB_ISNULL(j_array)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_array", K(ret));
  } else {
    array_node = j_array; 
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_string(ObIAllocator &allocator, ObString &str, ObJsonString *&str_node)
{
  int ret = OB_SUCCESS;
  ObJsonString *j_str = OB_NEWx(ObJsonString, &allocator,str);
  if (OB_ISNULL(j_str)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_str", K(ret));
  } else {
    str_node = j_str;
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_int(ObIAllocator &allocator, int64_t num, ObJsonInt *&int_node)
{
  int ret = OB_SUCCESS;
  ObJsonInt *j_int = OB_NEWx(ObJsonInt, &allocator, num);
  if (OB_ISNULL(j_int)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_int", K(ret));
  } else {
    int_node = j_int;
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_boolean(ObIAllocator &allocator, bool value, ObJsonBoolean *&boolean_node)
{
  int ret = OB_SUCCESS;
  ObJsonBoolean *j_bool = OB_NEWx(ObJsonBoolean, &allocator, value);
  if (OB_ISNULL(j_bool)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for j_bool", K(ret));
  } else {
    boolean_node = j_bool;
  }
  return ret;
}

int ObAIFuncJsonUtils::print_json_to_str(ObIAllocator &allocator, ObIJsonBase *base_node, ObString &str)
{
  int ret = OB_SUCCESS;
  ObJsonBuffer j_buf(&allocator);
  if (OB_FAIL(base_node->print(j_buf, 0))) {
    LOG_WARN("Failed to print json", K(ret));
  } else {
    str = j_buf.string();
  }
  return ret;
}

int ObAIFuncJsonUtils::get_json_object_form_str(ObIAllocator &allocator, ObString &str, ObJsonObject *&obj_node)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_base = NULL;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, str, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base))) {
    LOG_WARN("fail to get json base", K(ret), K(str));
  } else {
    obj_node = static_cast<ObJsonObject *>(j_base);
  }
  return ret;
}

int ObAIFuncJsonUtils::compact_json_object(ObIAllocator &allocator, ObJsonObject *obj_node, ObJsonObject *compact_obj)
{
  int ret = OB_SUCCESS;
  // add all members of obj_node to compact_obj
  if (OB_ISNULL(compact_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compact_obj is null", K(ret));
  } else if (OB_ISNULL(obj_node)) {
    // do nothing
  } else {
    ObString key;
    int64_t count = obj_node->element_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObJsonNode *j_node = obj_node->get_value(i);
      if (OB_ISNULL(j_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("j_node is null", K(ret));
      } else if (OB_FAIL(obj_node->get_key(i, key))) {
        LOG_WARN("Failed to get key", K(ret));
      } else if (OB_FAIL(compact_obj->add(key, j_node))) {
        LOG_WARN("Failed to add member", K(ret));
      }
    }
  }
  return ret;
}

int ObAIFuncPromptUtils::replace_meta_prompt(ObIAllocator &allocator, ObString &meta_prompt, ObString &key, ObString &content, ObString &result)
{
  int ret = OB_SUCCESS;
  if (meta_prompt.empty() || key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_prompt or key is empty", K(ret));
  } else {
    const char *key_pos = nullptr;
    if (OB_ISNULL(key_pos = STRSTR(meta_prompt.ptr(), key.ptr()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("key not found in meta_prompt", K(ret));
    } else {
      int64_t before_key_len = key_pos - meta_prompt.ptr();
      int64_t after_key_len = meta_prompt.length() - before_key_len - key.length();
      int64_t new_len = before_key_len + content.length() + after_key_len;
      char *new_str = static_cast<char *>(allocator.alloc(new_len));
      if (OB_ISNULL(new_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for new string", K(ret));
      } else {
        MEMCPY(new_str, meta_prompt.ptr(), before_key_len);
        MEMCPY(new_str + before_key_len, content.ptr(), content.length());
        MEMCPY(new_str + before_key_len + content.length(), key_pos + key.length(), after_key_len);
        result.assign_ptr(new_str, static_cast<ObString::obstr_size_t>(new_len));
      }
    }
  }
  return ret;
}

int ObAIFuncJsonUtils::transform_array_to_json_array(ObIAllocator &allocator, ObArray<ObString> &contents, ObJsonArray *&array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array = OB_NEWx(ObJsonArray, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for array", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < contents.count(); i++) {
      ObString content = contents[i];
      ObJsonString *j_str = OB_NEWx(ObJsonString, &allocator, content);
      if (OB_ISNULL(j_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for j_str", K(ret));
      } else {
        array->append(j_str);
      }
    }
  }
  return ret;
}

int ObAIFuncUtils::get_complete_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIComplete *&complete_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::OPENAI) 
      || ob_provider_check(provider, ObAIFuncProviderUtils::ALIYUN) 
      || ob_provider_check(provider, ObAIFuncProviderUtils::DEEPSEEK)
      || ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)
      || ob_provider_check(provider, ObAIFuncProviderUtils::HUNYUAN)) {
    complete_provider = OB_NEWx(ObOpenAIUtils::ObOpenAIComplete, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    complete_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeComplete, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current not support");
  } 
  if (OB_SUCC(ret) && OB_ISNULL(complete_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for complete_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_embed_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIEmbed *&embed_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::OPENAI) 
      || ob_provider_check(provider, ObAIFuncProviderUtils::ALIYUN)
      || ob_provider_check(provider, ObAIFuncProviderUtils::HUNYUAN)
      || ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)) {
    embed_provider = OB_NEWx(ObOpenAIUtils::ObOpenAIEmbed, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    embed_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeEmbed, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current not support");
  } 
  if (OB_SUCC(ret) && OB_ISNULL(embed_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for embed_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_rerank_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIRerank *&rerank_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW)) {
    rerank_provider = OB_NEWx(ObSiliconflowUtils::ObSiliconflowRerank, &allocator);
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DASHSCOPE)) {
    rerank_provider = OB_NEWx(ObDashscopeUtils::ObDashscopeRerank, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current not support");
  } 
  if (OB_SUCC(ret) && OB_ISNULL(rerank_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for rerank_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::get_parse_document_provider(ObIAllocator &allocator, const ObString &provider, ObAIFuncIParseDocument *&parse_document_provider)
{
  int ret = OB_SUCCESS;
  if (provider.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "provider is empty");
  } else if (ob_provider_check(provider, ObAIFuncProviderUtils::DEEPSEEK)) {
    parse_document_provider = OB_NEWx(ObDeepSeekUtils::ObDeepSeekParseDocument, &allocator);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this provider current not support", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this provider current not support");
  }

  if (OB_SUCC(ret) && OB_ISNULL(parse_document_provider)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for parse_document_provider", K(ret));
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_completion(const ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info is null");
  } else if (!is_completion_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not completion");
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_dense_embedding(const ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info is null");
  } else if (!is_dense_embedding_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not dense embedding");
  }
  return ret;
}

int ObAIFuncUtils::check_info_type_rerank(const ObAIFuncExprInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info is null");
  } else if (!is_rerank_type(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not rerank", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not rerank");
  }
  return ret;
}

int ObAIFuncUtils::set_string_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObString &res_str)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
  int64_t res_len = res_str.length();
  if (OB_FAIL(text_result.init(res_len))) {
    LOG_WARN("fail to init string result length", K(ret), K(text_result), K(res_len));
  } else if (OB_FAIL(text_result.append(res_str))) {
    LOG_WARN("fail to append string", K(ret), K(res_str), K(text_result));
  } else {
    text_result.set_result();
  }
  return ret;
}

int ObAIFuncUtils::get_ai_func_info(ObIAllocator &allocator, const ObString &model_id,
                                    share::schema::ObSchemaGetterGuard &guard,
                                    ObAIFuncExprInfo *&info)
{
  int ret = OB_SUCCESS;
  if (model_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model_id is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model_id is empty");
  } else {
    ObAIFuncExprInfo *info_obj = OB_NEWx(ObAIFuncExprInfo, (&allocator), allocator, T_FUN_SYS_AI_COMPLETE);
    if (OB_ISNULL(info_obj)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for info_obj", K(ret));
    } else if (OB_FAIL(info_obj->init(allocator, model_id, guard))) {
      LOG_WARN("Failed to init info_obj", K(ret));
    } else {
      info = info_obj;
    }
  }
  return ret;
}

bool ObAIFuncUtils::is_provider_support_base64(const ObString &provider)
{
  return ob_provider_check(provider, ObAIFuncProviderUtils::OPENAI)
      || ob_provider_check(provider, ObAIFuncProviderUtils::SILICONFLOW);
}

int ObAIFuncUtils::decode_base64_embedding_array(const ObIJsonBase &embedding_jbase,
                                                 ObIAllocator &allocator,
                                                 const int64_t dimension,
                                                 float *&vector)
{
  int ret = OB_SUCCESS;
  if (embedding_jbase.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("embedding_jbase is not a string", K(ret));
  } else {
    const char *encoded_embedding = embedding_jbase.get_data();
    uint64_t encoded_embedding_len = embedding_jbase.get_data_length();
    uint64_t decoded_buf_len = ObBase64Encoder::needed_decoded_length(encoded_embedding_len);
    uint8_t *decoded_buf = nullptr;
    int64_t pos = 0;
    if (decoded_buf_len <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("decoded_buf_len is not valid", K(ret), K(decoded_buf_len));
    } else if (OB_ISNULL(decoded_buf = static_cast<uint8_t *>(allocator.alloc(decoded_buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(decoded_buf_len));
    } else if (OB_FAIL(ObBase64Encoder::decode(encoded_embedding, encoded_embedding_len,
                                               decoded_buf, decoded_buf_len, pos))) {
      LOG_WARN("failed to decode encoded_embedding", K(ret));
    } else if (pos != dimension * sizeof(float)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decode result length mismatch", K(ret), K(pos), K(dimension));
    } else {
      vector = reinterpret_cast<float *>(decoded_buf);
    }
  }
  return ret;
}

int ObAIFuncUtils::decode_float_embedding_array(const ObIJsonBase &embedding_jbase,
                                                ObIAllocator &allocator,
                                                ObJsonReaderHelper &json_reader,
                                                const int64_t dimension,
                                                float *&vector)
{
  int ret = OB_SUCCESS;
  float *tmp_vector = nullptr;
  if (!ObJsonHelper::is_array_type(&embedding_jbase)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("embedding field is not an array", K(ret));
  } else {
    uint64_t embedding_size = json_reader.get_array_size(&embedding_jbase);
    if (embedding_size != dimension) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("embedding size mismatch", K(ret), K(embedding_size), K(dimension));
    } else {
      tmp_vector = static_cast<float*>(allocator.alloc(dimension * sizeof(float)));
      if (OB_ISNULL(tmp_vector)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        for (uint64_t i = 0; i < dimension && OB_SUCC(ret); i++) {
          ObIJsonBase *value = nullptr;
          if (OB_FAIL(json_reader.get_array_element(&embedding_jbase, i, value))) {
            LOG_WARN("failed to get array element", K(ret), K(i));
          } else if (!ObJsonHelper::is_number_type(value)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("value is not a number", K(ret), K(i));
          } else {
            float f_value = 0.0;
            if (OB_FAIL(json_reader.get_float_value(value, f_value))) {
              LOG_WARN("failed to get float value", K(ret), K(i));
            } else {
              tmp_vector[i] = f_value;
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    vector = tmp_vector;
  }
  return ret;
}

int ObAIFuncUtils::get_ai_func_info(ObIAllocator &allocator, const ObString &model_id, ObAIFuncExprInfo *&info)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_ai_func_info(allocator, model_id, guard, info))) {
    LOG_WARN("Failed to init info_obj", K(ret));
  }
  return ret;
}

int ObAIFuncModel::call_completion(ObString &prompt, ObJsonObject *config, ObString &result)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIComplete *complete_provider = nullptr;
  ObString prompt_str;
  ObString result_str;
  ObAIFuncClient client;
  ObString unencrypted_access_key;
  ObString request_model_name = get_request_model_name();
  if (!is_completion_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not completion");
  } else if (OB_FAIL(ObAIFuncUtils::get_complete_provider(*allocator_, endpoint_info_.get_provider(), complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(complete_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(complete_provider->get_body(*allocator_, request_model_name, prompt_str, prompt, config, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, endpoint_info_.get_url(), headers, body, response))) {
    LOG_WARN("Failed to send post", K(ret));
  } else if (OB_FAIL(complete_provider->parse_output(*allocator_, response, result_base))) {
    LOG_WARN("Failed to parse output", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(*allocator_, result_base, result_str))) {
    LOG_WARN("Failed to print json to string", K(ret));
  } else {
    result = result_str;
  }
  return ret;
}

int ObAIFuncModel::call_completion_vector(ObArray<ObString> &prompts, ObJsonObject *config, ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObArray<ObJsonObject *> body_array;
  ObArray<ObJsonObject *> response_array;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIComplete *complete_provider = nullptr;
  ObString prompt_str;
  ObString result_str;
  ObAIFuncClient client;
  ObString unencrypted_access_key;
  ObString request_model_name = get_request_model_name();
  if (!is_completion_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not completion", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not completion");
  } else if (OB_FAIL(ObAIFuncUtils::get_complete_provider(*allocator_, endpoint_info_.get_provider(), complete_provider))) {
    LOG_WARN("Failed to get complete provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(complete_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < prompts.count(); i++) {
      ObString prompt = prompts[i];
      if (OB_FAIL(complete_provider->get_body(*allocator_, request_model_name, prompt_str, prompt, config, body))) {
        LOG_WARN("Failed to get body", K(ret));
      } else if (OB_FAIL(body_array.push_back(body))) {
        LOG_WARN("Failed to append body", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(client.send_post_batch(*allocator_, endpoint_info_.get_url(), headers, body_array, response_array))) {
    LOG_WARN("Failed to send post", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < response_array.count(); i++) {
      ObJsonObject *response = response_array[i];
      ObStringBuffer result_buf(allocator_);
      if (OB_FAIL(complete_provider->parse_output(*allocator_, response, result_base))) {
        LOG_WARN("Failed to parse output", K(ret));
      } else if (OB_FAIL(result_base->print(result_buf, 0))) {
        LOG_WARN("Failed to print json", K(ret));
      } else {
        results.push_back(result_buf.string());
      }
    }
  }
  return ret;
}

int ObAIFuncModel::call_dense_embedding(ObString &content, ObJsonObject *config, ObString &result)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> contents;
  ObArray<ObString> results;
  if (content.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("content is empty", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "input is empty");
  } else if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not dense embedding");
  } else if (OB_FAIL(contents.push_back(content))) {
    LOG_WARN("Failed to push back content", K(ret));
  } else if (OB_FAIL(call_dense_embedding_vector_v2(contents, config, results))) {
    LOG_WARN("Failed to call dense embedding vector v2", K(ret));
  } else if (results.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results is not equal to 1", K(ret));
  } else {
    result = results[0];
  }
  return ret;
}

int ObAIFuncModel::call_dense_embedding_vector(ObArray<ObString> &contents, ObJsonObject *config, ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObArray<ObJsonObject *> body_array;
  ObArray<ObJsonObject *> response_array;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIEmbed *embed_provider = nullptr;
  ObAIFuncClient client;
  ObString request_model_name = get_request_model_name();
  ObString unencrypted_access_key;
  if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not dense embedding");
  } else if (OB_FAIL(ObAIFuncUtils::get_embed_provider(*allocator_, endpoint_info_.get_provider(), embed_provider))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(embed_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < contents.count(); i++) {
      ObString content = contents[i];
      ObArray<ObString> content_array;
      if (OB_FAIL(content_array.push_back(content))) {
        LOG_WARN("Failed to push back content", K(ret));
      } else if (OB_FAIL(embed_provider->get_body(*allocator_, request_model_name, content_array, config, body))) {
        LOG_WARN("Failed to get body", K(ret));
      } else if (OB_FAIL(body_array.push_back(body))) {
        LOG_WARN("Failed to append body", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(client.send_post_batch(*allocator_, endpoint_info_.get_url(), headers, body_array, response_array))) {
    LOG_WARN("Failed to send post", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < response_array.count(); i++) {
      ObJsonObject *response = response_array[i];
      ObStringBuffer result_buf(allocator_);
      if (OB_FAIL(embed_provider->parse_output(*allocator_, response, result_base))) {
        LOG_WARN("Failed to parse output", K(ret));
      } else if (OB_FAIL(result_base->print(result_buf, 0))) {
        LOG_WARN("Failed to print json", K(ret));
      } else {
        results.push_back(result_buf.string());
      }
    }
  }
  return ret;
}


int ObAIFuncModel::call_dense_embedding_vector_v2(ObArray<ObString> &content, ObJsonObject *config, ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIEmbed *embed_provider = nullptr;
  ObString result_str;
  ObAIFuncClient client;
  int64_t dimension = 0;
  if (OB_NOT_NULL(config)) {
    ObJsonNode *dimension_node = config->get_value("dimensions");
    if (OB_ISNULL(dimension_node)) {
      // do nothing
    } else if (dimension_node->json_type() != ObJsonNodeType::J_INT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dimension is not int", K(ret));
    } else {
      dimension = static_cast<ObJsonInt *>(dimension_node)->get_int();
    }
  }
  
  ObString unencrypted_access_key;
  ObString request_model_name = get_request_model_name();
  if (!is_dense_embedding_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not dense embedding", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not dense embedding");
  } else if (OB_FAIL(ObAIFuncUtils::get_embed_provider(*allocator_, endpoint_info_.get_provider(), embed_provider))) {
    LOG_WARN("Failed to get embed provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(embed_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(embed_provider->get_body(*allocator_, request_model_name, content, config, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, endpoint_info_.get_url(), headers, body, response))) {
    LOG_WARN("Failed to send post", K(ret));
  } else if (OB_FAIL(embed_provider->parse_output(*allocator_, response, result_base))) {
    LOG_WARN("Failed to parse output", K(ret));
  } else {
    ObJsonArray *result_array = static_cast<ObJsonArray *>(result_base);
    int64_t count = result_array->element_count();
    if (content.count() != count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("content count is not equal to result array count", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        ObIJsonBase *j_base = result_array->get_value(i);
        if (OB_ISNULL(j_base)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("j_base is null", K(ret));
        } else if (dimension > 0 && static_cast<ObJsonArray *>(j_base)->element_count() != dimension) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("result array is not equal to dimension", K(ret), K(dimension), K(static_cast<ObJsonArray *>(j_base)->element_count()));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "result dimension is not equal to dimension");
        } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(*allocator_, j_base, result_str))) {
          LOG_WARN("Failed to print json to string", K(ret));
        } else {
          results.push_back(result_str);
        }
      }
    }
  }
  return ret;
}

int ObAIFuncModel::call_rerank(ObString &query, ObJsonArray *contents, ObJsonArray *&results)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncIRerank *rerank_provider = nullptr;
  ObAIFuncClient client;
  ObString unencrypted_access_key;
  ObString request_model_name = get_request_model_name();
  if (!is_rerank_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info type is not rerank", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "info type is not rerank");
  } else if (OB_FAIL(ObAIFuncUtils::get_rerank_provider(*allocator_, endpoint_info_.get_provider(), rerank_provider))) {
    LOG_WARN("Failed to get rerank provider", K(ret));
  } else if (OB_FAIL(endpoint_info_.get_unencrypted_access_key(*allocator_, unencrypted_access_key))) {
    LOG_WARN("Failed to get unencrypted access key", K(ret));
  } else if (OB_FAIL(rerank_provider->get_header(*allocator_, unencrypted_access_key, headers))) {
    LOG_WARN("Failed to get header", K(ret));
  } else if (OB_FAIL(rerank_provider->get_body(*allocator_, request_model_name, query, contents, nullptr, body))) {
    LOG_WARN("Failed to get body", K(ret));
  } else if (OB_FAIL(client.send_post(*allocator_, endpoint_info_.get_url(), headers, body, response))) {
    LOG_WARN("Failed to send post", K(ret));
  } else if (OB_FAIL(rerank_provider->parse_output(*allocator_, response, result_base))) {
    LOG_WARN("Failed to parse output", K(ret));
  } else {
    results = static_cast<ObJsonArray *>(result_base);
  }
  return ret;
}

bool ObAIFuncJsonUtils::ob_is_json_array_all_str(ObJsonArray* json_array)
{
  bool is_all_str = true;
  if (OB_ISNULL(json_array)) {
    is_all_str = false;
  }
  for (int64_t i = 0; is_all_str && i < json_array->element_count(); i++) {
    ObJsonNode *node = json_array->get_value(i);
    if (OB_ISNULL(node)) {
      is_all_str = false;
    } else if (node->json_type() != ObJsonNodeType::J_STRING) {
      is_all_str = false;
    }
  }
  return is_all_str;
}

int ObAIFuncPromptObjectUtils::construct_prompt_object(ObIAllocator &allocator, ObString &template_str, ObJsonArray *args_array, ObJsonObject *&prompt_object)
{
  INIT_SUCC(ret);
  ObJsonObject *prompt_obj = NULL;
  ObJsonString *template_json_str = NULL;
  if (template_str.empty() || args_array == NULL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, template_str, template_json_str))) {
    LOG_WARN("fail to get json string", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, prompt_obj))) {
    LOG_WARN("fail to get json object", K(ret));
  } else if (OB_FAIL(prompt_obj->add(ObAIFuncPromptObjectUtils::prompt_template_key, template_json_str))) {
    LOG_WARN("fail to set template", K(ret));
  } else if (OB_FAIL(prompt_obj->add(ObAIFuncPromptObjectUtils::prompt_args_key, args_array))) {
    LOG_WARN("fail to set args", K(ret), K(args_array));
  }
  if (OB_SUCC(ret)) {
    prompt_object = prompt_obj;
  }
  return ret;
}

bool ObAIFuncPromptObjectUtils::is_valid_prompt_object(ObJsonObject *prompt_object)
{
  bool is_valid = true;
  ObJsonNode *template_node = NULL;
  ObJsonNode *args_node = NULL;
  if (OB_ISNULL(prompt_object)) {
    is_valid = false;
  } else if (prompt_object->element_count() != 2) {
    is_valid = false;
  } else if (OB_ISNULL(template_node = prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_template_key)) ||
            (OB_ISNULL(args_node = prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key)))) {
    is_valid = false;
  } else if (template_node->json_type() != ObJsonNodeType::J_STRING || args_node->json_type() != ObJsonNodeType::J_ARRAY) {
    is_valid = false;
  }
  if (is_valid) {
    ObJsonArray *args_array = static_cast<ObJsonArray *>(args_node);
    for (int64_t i = 0; is_valid && i < args_array->element_count(); i++) {
      ObJsonNode *node = args_array->get_value(i);
      if (OB_ISNULL(node)) {  
        is_valid = false;
      } else if (node->json_type() != ObJsonNodeType::J_STRING && node->json_type() != ObJsonNodeType::J_OBJECT) {
        is_valid = false;
      }
    }
  }
  return is_valid;
}

int ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(ObIAllocator &allocator, ObJsonObject* prompt_object, ObString& replaced_prompt_str)
{
  INIT_SUCC(ret);
  ObJsonString *template_json_str = NULL;
  ObJsonArray *args_array = NULL;
  ObString template_str;
  ObString result_str;
  if (OB_ISNULL(prompt_object) ||
      OB_ISNULL(template_json_str = static_cast<ObJsonString *>(prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_template_key))) ||
      OB_ISNULL(args_array = static_cast<ObJsonArray *>(prompt_object->get_value(ObAIFuncPromptObjectUtils::prompt_args_key))) ||
      OB_ISNULL(template_str = template_json_str->get_str()) ||
      (template_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    uint64_t args_count = args_array->element_count();
    int64_t max_result_len = template_str.length();
    char *result_buf = NULL;
    for (uint64_t i = 0; OB_SUCC(ret) && i < args_count; i++) {
      ObJsonNode *arg_node = args_array->get_value(i);
      if (OB_NOT_NULL(arg_node) && arg_node->json_type() == ObJsonNodeType::J_STRING) {
        ObJsonString *arg_str = static_cast<ObJsonString *>(arg_node);
        max_result_len += arg_str->get_str().length();
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(result_buf = static_cast<char *>(allocator.alloc(max_result_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for result buffer", K(ret), K(max_result_len));
    } else {
      int64_t result_pos = 0;
      const char *template_ptr = template_str.ptr();
      int64_t template_len = template_str.length();
      
      for (int64_t i = 0; i < template_len && OB_SUCC(ret); i++) {
        if (template_ptr[i] == '{') {
          int64_t start_pos = i;
          int64_t end_pos = start_pos;
          bool found_end = false;
          
          for (int64_t j = start_pos + 1; j < template_len && !found_end; j++) {
            if (template_ptr[j] == '}') {
              end_pos = j;
              found_end = true;
            } else if (template_ptr[j] < '0' || template_ptr[j] > '9') {
              break;
            }
          }
          
          if (found_end && end_pos > start_pos + 1) {
            ObString index_str;
            index_str.assign_ptr(template_ptr + start_pos + 1, static_cast<int32_t>(end_pos - start_pos - 1));
            
            int64_t index = 0;
            bool valid_index = true;
            for (int64_t k = 0; k < index_str.length() && valid_index; k++) {
              if (index_str.ptr()[k] >= '0' && index_str.ptr()[k] <= '9') {
                index = index * 10 + (index_str.ptr()[k] - '0');
              } else {
                valid_index = false;
              }
            }
            
            if (valid_index && index >= 0 && static_cast<uint64_t>(index) < args_count) {
              ObJsonNode *arg_node = args_array->get_value(static_cast<uint64_t>(index));
              if (OB_NOT_NULL(arg_node) && arg_node->json_type() == ObJsonNodeType::J_STRING) {
                ObJsonString *arg_str = static_cast<ObJsonString *>(arg_node);
                ObString arg_value = arg_str->get_str();
                
                if (result_pos + arg_value.length() <= max_result_len) {
                  MEMCPY(result_buf + result_pos, arg_value.ptr(), arg_value.length());
                  result_pos += arg_value.length();
                } else {
                  ret = OB_BUF_NOT_ENOUGH;
                  LOG_WARN("result buffer not enough", K(ret), K(result_pos), K(arg_value.length()), K(max_result_len));
                }
              } else {
                //do nothing
              }
              
              i = end_pos;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid placeholder index", K(ret), K(index), K(args_count));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_prompt: invalid placeholder index");
            }
          } else {
            if (result_pos < max_result_len) {
              result_buf[result_pos++] = template_ptr[i];
            } else {
              ret = OB_BUF_NOT_ENOUGH;
              LOG_WARN("result buffer not enough", K(ret), K(result_pos), K(max_result_len));
            }
          }
        } else {
          if (result_pos < max_result_len) {
            result_buf[result_pos++] = template_ptr[i];
          } else {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("result buffer not enough", K(ret), K(result_pos), K(max_result_len));
          }
        }
      }
      
      if (OB_SUCC(ret)) {
        replaced_prompt_str.assign_ptr(result_buf, static_cast<int32_t>(result_pos));
      }
    }
  }
  return ret;
}
const ObString ObAIFuncModel::get_request_model_name()
{
  ObString request_model_name = info_.model_;
  if (!endpoint_info_.get_request_model_name().empty()) {
    request_model_name = endpoint_info_.get_request_model_name();
  }
  return request_model_name;
}

// ============== ObAIFuncDocumentUtils Implementation ==============

// Context for collecting PNG data via callback
struct PngWriteContext {
  ObIAllocator* allocator;
  char* buffer;
  size_t buffer_capacity;
  size_t current_size;
  int error_code;
  
  PngWriteContext(ObIAllocator* alloc) 
    : allocator(alloc), buffer(nullptr), buffer_capacity(0), current_size(0), error_code(OB_SUCCESS) {}
};

// Callback function for stbi_write_png_to_func
// This function is called multiple times by stb to write PNG data chunks
static void png_write_callback(void* context, void* data, int size) {
  PngWriteContext* ctx = static_cast<PngWriteContext*>(context);
  
  if (OB_SUCCESS != ctx->error_code) {
    return;  // Already failed, skip
  }
  
  // Check if we need to expand the buffer
  if (ctx->current_size + size > ctx->buffer_capacity) {
    // Calculate new capacity (double the required size for efficiency)
    size_t new_capacity = (ctx->current_size + size) * 2;
    if (new_capacity < 4096) {
      new_capacity = 4096;  // Minimum initial size
    }
    
    // Allocate new buffer
    char* new_buffer = static_cast<char*>(ctx->allocator->alloc(new_capacity));
    if (OB_ISNULL(new_buffer)) {
      ctx->error_code = OB_ALLOCATE_MEMORY_FAILED;
      // Cannot use LOG_WARN here as it requires 'ret' variable in scope
      return;
    }
    
    // Copy existing data if any
    if (ctx->buffer != nullptr && ctx->current_size > 0) {
      MEMCPY(new_buffer, ctx->buffer, ctx->current_size);
    }
    
    ctx->buffer = new_buffer;
    ctx->buffer_capacity = new_capacity;
  }
  
  // Append the new data
  MEMCPY(ctx->buffer + ctx->current_size, data, size);
  ctx->current_size += size;
}

// Helper function: Convert FPDF_BITMAP to PNG data in memory
// Converts BGRA format from PDFium to RGBA and encodes as PNG
static int convert_bitmap_to_png_data(FPDF_BITMAP bitmap, 
                                       ObIAllocator &allocator,
                                       char** out_png_buffer,
                                       size_t* out_png_len)
{
  int ret = OB_SUCCESS;
  unsigned char* rgba_buffer = nullptr;
  ObArenaAllocator tmp_allocator;
  
  int bitmap_width = FPDFBitmap_GetWidth(bitmap);
  int bitmap_height = FPDFBitmap_GetHeight(bitmap);
  int stride = FPDFBitmap_GetStride(bitmap);
  unsigned char* src_buffer = static_cast<unsigned char*>(FPDFBitmap_GetBuffer(bitmap));
  
  if (OB_ISNULL(src_buffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get bitmap buffer", K(ret));
  }
  
  // Allocate RGBA buffer
  if (OB_SUCC(ret)) {
    rgba_buffer = static_cast<unsigned char*>(tmp_allocator.alloc(bitmap_width * bitmap_height * 4));
    if (OB_ISNULL(rgba_buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate rgba buffer", K(ret), K(bitmap_width), K(bitmap_height));
    }
  }
  
  // Convert BGRA to RGBA
  if (OB_SUCC(ret)) {
    for (int y = 0; y < bitmap_height; y++) {
      for (int x = 0; x < bitmap_width; x++) {
        int src_idx = y * stride + x * 4;
        int dst_idx = (y * bitmap_width + x) * 4;
        
        rgba_buffer[dst_idx + 0] = src_buffer[src_idx + 2];  // R
        rgba_buffer[dst_idx + 1] = src_buffer[src_idx + 1];  // G
        rgba_buffer[dst_idx + 2] = src_buffer[src_idx + 0];  // B
        rgba_buffer[dst_idx + 3] = src_buffer[src_idx + 3];  // A
      }
    }
  }
  
  // Use stb_image_write to generate PNG data via callback
  if (OB_SUCC(ret)) {
    PngWriteContext ctx(&allocator);
    int stbi_result = stbi_write_png_to_func(png_write_callback, &ctx,
                                              bitmap_width, bitmap_height, 4,
                                              rgba_buffer, bitmap_width * 4);
    
    if (!stbi_result || OB_FAIL(ctx.error_code)) {
      ret = OB_FAIL(ctx.error_code) ? ctx.error_code : OB_ERR_UNEXPECTED;
      LOG_WARN("failed to encode png data", K(ret), K(stbi_result));
    } else if (OB_ISNULL(ctx.buffer) || ctx.current_size == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("png encoding produced no data", K(ret));
    } else {
      *out_png_buffer = ctx.buffer;
      *out_png_len = ctx.current_size;
    }
  }
  
  return ret;
}

// generate png images from pdf
int ObAIFuncDocumentUtils::convert_pdf_to_images(ObIAllocator &allocator, ObString &pdf, ObArray<ObString> &images, double dpi /*=144.0*/)
{
  int ret = OB_SUCCESS;
  FPDF_DOCUMENT doc = nullptr;
  FPDF_PAGE page = nullptr;
  FPDF_BITMAP bitmap = nullptr;

  if (OB_UNLIKELY(pdf.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pdf data is empty", K(ret));
  } else {
    // Load PDF document from memory
    doc = FPDF_LoadMemDocument(pdf.ptr(), pdf.length(), nullptr);
    if (OB_ISNULL(doc)) {
      ret = OB_ERR_UNEXPECTED;
      unsigned long error = FPDF_GetLastError();
      LOG_WARN("failed to load PDF document", K(ret), K(error));
    } else {
      int page_count = FPDF_GetPageCount(doc);
      for (int page_index = 0; page_index < page_count && OB_SUCC(ret); page_index++) {
        page = FPDF_LoadPage(doc, page_index);
        if (OB_ISNULL(page)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to load page", K(ret), K(page_index));
        } else {
          // Get page dimensions
          double width = FPDF_GetPageWidthF(page);
          double height = FPDF_GetPageHeightF(page);
          double scale = dpi / 72.0;  // 72 DPI is the standard for PDF
          int bitmap_width = static_cast<int>(width * scale);
          int bitmap_height = static_cast<int>(height * scale);
          bitmap = FPDFBitmap_Create(bitmap_width, bitmap_height, 0);
          if (OB_ISNULL(bitmap)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to create bitmap", K(ret), K(bitmap_width), K(bitmap_height), K(page_index));
          } else {
            // Fill with white background
            FPDFBitmap_FillRect(bitmap, 0, 0, bitmap_width, bitmap_height, 0xFFFFFFFF);
            // Render page to bitmap
            FPDF_RenderPageBitmap(bitmap, page, 0, 0, bitmap_width, bitmap_height, 0, 0);
            
            // Convert bitmap to PNG
            char* png_buffer = nullptr;
            size_t png_len = 0;
            if (OB_FAIL(convert_bitmap_to_png_data(bitmap, allocator, &png_buffer, &png_len))) {
              LOG_WARN("failed to convert bitmap to PNG", K(ret), K(page_index));
            } else {
              ObString image_str;
              image_str.assign_ptr(png_buffer, static_cast<int32_t>(png_len));
              if (OB_FAIL(images.push_back(image_str))) {
                LOG_WARN("failed to push image to array", K(ret), K(page_index));
              }
            }
          }
          FPDFBitmap_Destroy(bitmap);
        }
        FPDF_ClosePage(page);
      }
    }
    FPDF_CloseDocument(doc);
  }
  return ret;
}

int ObAIFuncDocumentUtils::encode_image_to_base64(ObIAllocator &allocator, ObString &image, ObString &base64)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(image.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("image data is empty", K(ret));
  } else {
    int64_t pos = 0;
    int64_t encoded_len = ObBase64Encoder::needed_encoded_length(image.length());
    char *encoded_buf = static_cast<char *>(allocator.alloc(encoded_len));
    if (OB_ISNULL(encoded_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for base64 encoding", K(ret), K(encoded_len));
    } else if (OB_FAIL(ObBase64Encoder::encode(reinterpret_cast<const uint8_t*>(image.ptr()),
                                        image.length(),
                                        encoded_buf,
                                        encoded_len,
                                        pos))) {
      LOG_WARN("failed to encode image to base64", K(ret), K(image.length()));
    } else {
      base64.assign_ptr(encoded_buf, pos);
    }
  }
  return ret;
}

int ObAIFuncDocumentUtils::save_image_to_file(ObIAllocator &allocator, ObString &image, const char* filename)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(image.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("image data is empty", K(ret));
  } else {
    FILE *file = fopen(filename, "w");
    if (OB_ISNULL(file)) {
      ret = OB_FILE_NOT_OPENED;
      LOG_WARN("failed to open file", K(ret), K(filename));
    } else {
      fwrite(image.ptr(), 1, image.length(), file);
      fclose(file);
      LOG_INFO("successfully saved image to file", K(filename));
    }
  }
  return ret;
}

// Get image format name string
const char* ObAIFuncDocumentUtils::get_image_format_name(ImageFormat format) 
{
  switch (format) {
    case IMAGE_FORMAT_PNG:     return "PNG";
    case IMAGE_FORMAT_JPEG:    return "JPEG";
    case IMAGE_FORMAT_BMP:     return "BMP";
    case IMAGE_FORMAT_GIF:     return "GIF";
    case IMAGE_FORMAT_WEBP:    return "WebP";
    case IMAGE_FORMAT_TIFF:    return "TIFF";
    case IMAGE_FORMAT_SVG:     return "SVG";
    case IMAGE_FORMAT_UNKNOWN:
    default:                   return "Unknown";
  }
}

// Detect image format from data by checking magic bytes
ObAIFuncDocumentUtils::ImageFormat ObAIFuncDocumentUtils::detect_image_format(const char* data, size_t size)
{
  if (OB_ISNULL(data) || size < 4) {
    return IMAGE_FORMAT_UNKNOWN;
  }
  
  const unsigned char* bytes = reinterpret_cast<const unsigned char*>(data);
  
  // PNG: 89 50 4E 47 0D 0A 1A 0A
  if (size >= 8 && 
      bytes[0] == 0x89 && bytes[1] == 0x50 && 
      bytes[2] == 0x4E && bytes[3] == 0x47 &&
      bytes[4] == 0x0D && bytes[5] == 0x0A &&
      bytes[6] == 0x1A && bytes[7] == 0x0A) {
    return IMAGE_FORMAT_PNG;
  }
  
  // JPEG: FF D8 FF
  if (size >= 3 && 
      bytes[0] == 0xFF && bytes[1] == 0xD8 && bytes[2] == 0xFF) {
    return IMAGE_FORMAT_JPEG;
  }
  
  // BMP: 42 4D (BM)
  if (size >= 2 && 
      bytes[0] == 0x42 && bytes[1] == 0x4D) {
    return IMAGE_FORMAT_BMP;
  }
  
  // GIF: 47 49 46 38 (GIF8)
  if (size >= 4 && 
      bytes[0] == 0x47 && bytes[1] == 0x49 && 
      bytes[2] == 0x46 && bytes[3] == 0x38) {
    return IMAGE_FORMAT_GIF;
  }
  
  // WebP: 52 49 46 46 ... 57 45 42 50 (RIFF...WEBP)
  if (size >= 12 && 
      bytes[0] == 0x52 && bytes[1] == 0x49 && 
      bytes[2] == 0x46 && bytes[3] == 0x46 &&
      bytes[8] == 0x57 && bytes[9] == 0x45 && 
      bytes[10] == 0x42 && bytes[11] == 0x50) {
    return IMAGE_FORMAT_WEBP;
  }
  
  // TIFF: 49 49 2A 00 (little endian) or 4D 4D 00 2A (big endian)
  if (size >= 4 && 
      ((bytes[0] == 0x49 && bytes[1] == 0x49 && bytes[2] == 0x2A && bytes[3] == 0x00) ||
       (bytes[0] == 0x4D && bytes[1] == 0x4D && bytes[2] == 0x00 && bytes[3] == 0x2A))) {
    return IMAGE_FORMAT_TIFF;
  }
  
  // SVG: check for "<svg" or "<?xml" at the beginning (text-based format)
  if (size >= 4) {
    if ((bytes[0] == '<' && bytes[1] == 's' && bytes[2] == 'v' && bytes[3] == 'g') ||
        (bytes[0] == '<' && bytes[1] == '?' && bytes[2] == 'x' && bytes[3] == 'm')) {
      return IMAGE_FORMAT_SVG;
    }
  }
  
  return IMAGE_FORMAT_UNKNOWN;
}

bool ObAIFuncUtils::is_http_url(const ObString &url)
{
  bool is_http = false;
  if (!url.empty()) {
    ObString http_prefix = "http://";
    ObString https_prefix = "https://";
    
    if (url.prefix_match_ci(http_prefix) || url.prefix_match_ci(https_prefix)) {
      is_http = true;
    }
  }
  
  return is_http;
}


} // namespace common
} // namespace oceanbase
