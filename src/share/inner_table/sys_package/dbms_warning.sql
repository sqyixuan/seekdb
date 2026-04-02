#package_name:dbms_warning
#author:jingxing.lj

CREATE OR REPLACE PACKAGE dbms_warning AS

  PROCEDURE add_warning_setting_cat(warning_category IN VARCHAR2,
                                    warning_value    IN VARCHAR2,
                                    scope            IN VARCHAR2);

  PROCEDURE add_warning_setting_num(warning_number IN PLS_INTEGER,
                                    warning_value  IN VARCHAR2,
                                    scope          IN VARCHAR2);

  FUNCTION get_warning_setting_cat(warning_category IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION get_warning_setting_num(warning_number IN PLS_INTEGER) RETURN VARCHAR2;

  FUNCTION get_warning_setting_string RETURN VARCHAR2;

  PROCEDURE set_warning_setting_string(VALUE IN VARCHAR2, scope IN VARCHAR2);

  FUNCTION get_category(warning_number IN  PLS_INTEGER) RETURN VARCHAR2;
END dbms_warning;
//
