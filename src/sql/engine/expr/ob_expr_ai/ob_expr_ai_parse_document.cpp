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
#include "ob_expr_ai_parse_document.h"
#include "ob_ai_func_utils.h"
#include "ob_ai_func_client.h"
#include "lib/utility/utility.h"
#include "lib/json_type/ob_json_common.h"
#include "share/ai_service/ob_ai_service_struct.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{

ObExprAIParseDocument::ObExprAIParseDocument(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                    T_FUN_SYS_AI_PARSE_DOCUMENT, 
                    N_AI_PARSE_DOCUMENT, 
                    MORE_THAN_ZERO,
                    NOT_VALID_FOR_GENERATED_COL, 
                    NOT_ROW_DIMENSION) 
{
}

ObExprAIParseDocument::~ObExprAIParseDocument() 
{
}

int ObExprAIParseDocument::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_stack,
                                    int64_t param_num,
                                    common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      ObObjType in_type = types_stack[PARAM_IDX].get_type();
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, PARAM_IDX, N_AI_COMPLETE))) {
        LOG_WARN("wrong type for json config.", K(ret), K(types_stack[PARAM_IDX].get_type()));
      } else if (ob_is_string_type(in_type) && types_stack[PARAM_IDX].get_collation_type() != CS_TYPE_BINARY) {
        if (types_stack[PARAM_IDX].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[PARAM_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAIParseDocument::eval_ai_parse_document(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  int ret = OB_SUCCESS;
  ObDatum *arg_model_key = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_param = nullptr;
  ParseDocumentParam parse_document_param;
  if (OB_FAIL(expr.eval_param_value(ctx, arg_model_key, arg_content, arg_param))) {
    LOG_WARN("failed to eval parameters", K(ret));
  } else if (arg_model_key->is_null() || arg_content->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model key or content is null", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model key or content is null");
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    ObString model_key = arg_model_key->get_string();
    ObString content; 
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *arg_content, expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), content))) {
      LOG_WARN("fail to get real content string data", K(ret));
    } else if (model_key.empty() || content.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("model key or content is empty", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "model key or content is empty");
    } else if (OB_NOT_NULL(arg_param) && !arg_param->is_null()) {
      ObJsonObject *param_object = nullptr;
      bool is_null = false;
      ObString param_str;
      if (ObTextStringHelper::read_real_string_data(temp_allocator, *arg_param, expr.args_[2]->datum_meta_, expr.args_[2]->obj_meta_.has_lob_header(), param_str)) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(temp_allocator, param_str, param_object))) {
        LOG_WARN("failed to get json object", K(ret));
      } else if (OB_ISNULL(param_object)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param_object is null", K(ret));
      } else if (OB_FAIL(parse_document_param.parse_from_json_base(*param_object))) {
        LOG_WARN("failed to parse param", K(ret));
      }
    }


    FLOG_INFO("finish parse document param", K(ret), K(parse_document_param));
    if (OB_SUCC(ret)) {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
      MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_PARSE_DOCUMENT));
      bool is_http_url = false;
      ObString result;
      ObArray<ObString> images;
      ObString temp_result_str;
      ObStringBuffer result_buffer(&temp_allocator);
      if (OB_FALSE_IT(is_http_url = ObAIFuncUtils::is_http_url(content))) {
      } else if (!is_http_url) {
        //从文件读取pdf
        ObString pdf_content;
        if (parse_document_param.input_format_ == ParseDocumentParam::INPUT_FORMAT_TYPE::BINARY) {
          pdf_content = content;
        } else if (parse_document_param.input_format_ == ParseDocumentParam::INPUT_FORMAT_TYPE::URL_OR_PATH) {
          if (OB_FAIL (load_file_to_string(content.ptr(), temp_allocator, pdf_content))) {
            LOG_WARN("failed to load pdf file", K(ret), K(content));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid input format", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid input format");
        }
        
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL (ObAIFuncDocumentUtils::convert_pdf_to_images(temp_allocator, pdf_content, images, static_cast<double>(parse_document_param.dpi_)))) {
          LOG_WARN("failed to convert pdf to images", K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < images.count(); i++) {
            ObString image = images.at(i);
            ObString base64_image;
            if (parse_document_param.save_png_ == true) {
              // convert int i to cstring
              char filename[256];
              snprintf(filename, sizeof(filename), "%.*s_%d.png", static_cast<int>(content.length()), content.ptr(), i);
              if (OB_FAIL (ObAIFuncDocumentUtils::save_image_to_file(temp_allocator, image, filename))) {
                LOG_WARN("failed to save image to file", K(ret));
              }
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL (ObAIFuncDocumentUtils::encode_image_to_base64(temp_allocator, image, base64_image))) {
              LOG_WARN("failed to encode image to base64", K(ret));
            } else {
              images.at(i) = base64_image;
            }
          }
        }
      } 

      // get result from each image and append to result_buffer
      if (OB_SUCC(ret)) {
        for (int i = 0; OB_SUCC(ret) && i < images.count(); i++) {
          ObString image = images.at(i);
          bool is_markdown = parse_document_param.output_format_ == ParseDocumentParam::OUTPUT_FORMAT_TYPE::MARKDOWN;
          if (OB_FAIL (parse_document_image_by_model(temp_allocator, model_key, image, !is_http_url, is_markdown, temp_result_str))) {
            LOG_WARN("failed to parse document image by model", K(ret));
          } else {
            result_buffer.append(temp_result_str);
            result_buffer.append("\n\n<--- Page Split --->\n");
          } 
        }
      }

      // set result to res
      if (OB_SUCC(ret)) {
        if (OB_FALSE_IT(result_buffer.get_result_string(result))) {
          LOG_WARN("failed to get result string", K(ret));
        } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
          LOG_WARN("failed to set raw str res", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprAIParseDocument::parse_document_image_by_model(ObIAllocator &allocator, ObString &model_key, ObString &document, bool is_base64_encoded, bool is_markdown, ObString &result)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *info = nullptr;
  omt::ObAiServiceGuard ai_service_guard;
  omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
  const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
  if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_key, info))) {
    LOG_WARN("failed to get ai func info", K(ret));
  } else if (OB_ISNULL(ai_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai service is null", K(ret));
  } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
    LOG_WARN("failed to get ai service guard", K(ret));
  } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(model_key, endpoint_info))) {
    LOG_WARN("failed to get endpoint info", K(ret), K(model_key));
  } else if (OB_ISNULL(endpoint_info)) {
    ret = OB_ERR_UNEXPECTED;  
    LOG_WARN("endpoint info is null", K(ret));
  } else {
    ObArray<ObString> headers;
    ObJsonObject *config = nullptr;
    ObJsonObject *body = nullptr;
    ObJsonObject *http_response = nullptr;
    ObIJsonBase *result_base = nullptr;
    ObString result_str;
    ObAIFuncClient client;
    ObAIFuncIParseDocument *parse_document_provider = nullptr;
    ObString unencrypted_access_key;
    if (OB_FAIL(ObAIFuncUtils::get_parse_document_provider(allocator, endpoint_info->get_provider(), parse_document_provider))) {
      LOG_WARN("failed to get parse document provider", K(ret));
    } else if (OB_FAIL(endpoint_info->get_unencrypted_access_key(allocator, unencrypted_access_key))) {
      LOG_WARN("failed to get unencrypted access key", K(ret));
    } else if (OB_FAIL(parse_document_provider->get_header(allocator, unencrypted_access_key, headers))) {
      LOG_WARN("failed to get header", K(ret));
    } else if (OB_FAIL(parse_document_provider->get_body(allocator, info->model_, document, is_base64_encoded, is_markdown, config, body))) {
      LOG_WARN("failed to get body", K(ret));
    } else if (OB_FAIL(client.send_post(allocator, endpoint_info->get_url(), headers, body, http_response))) {
      LOG_WARN("failed to send post", K(ret));
    } else if (OB_FAIL(parse_document_provider->parse_output(allocator, http_response, result_base))) {
      LOG_WARN("failed to parse output", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, result_base, result_str))) {
      LOG_WARN("failed to print json to string", K(ret));
    } else {
      result = result_str;
    }
  }
  return ret;
}



int ObExprAIParseDocument::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const 
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = ObExprAIParseDocument::eval_ai_parse_document;
  return ret;
}

} // namespace sql
} // namespace oceanbase
 