#package_name:utl_i18n
#author: xiaofeng.lby

CREATE OR REPLACE PACKAGE UTL_I18N AS

  FUNCTION STRING_TO_RAW (
      data             IN VARCHAR,
      dst_charset      IN VARCHAR := NULL
  )
  RETURN RAW;

  FUNCTION RAW_TO_CHAR (
      data             IN RAW,
      src_charset      IN VARCHAR := NULL
  )
  RETURN VARCHAR;

END UTL_I18N;
//
