#package_name:utl_i18n
#author: xiaofeng.lby

CREATE OR REPLACE PACKAGE BODY UTL_I18N AS

  FUNCTION STRING_TO_RAW (
      data             IN VARCHAR,
      dst_charset      IN VARCHAR := NULL
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_i18n_string_to_raw);

  FUNCTION RAW_TO_CHAR (
      data             IN RAW,
      src_charset      IN VARCHAR := NULL
  )
  RETURN VARCHAR;
  PRAGMA INTERFACE(c, utl_i18n_raw_to_char);

END UTL_I18N;
//
