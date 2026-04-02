#_ -*- coding: utf-8 -*-
#!/usr/bin/env python

param_list = (
  "c_number",
  "c_raw",
  "c_char",
  "c_char_c",
  "c_varchar2",
  "c_varchar2_c",
  "c_nchar",
  "c_nvarchar",
  "c_clob",
  "'1号'",
  "n'1号'",
  "date'2012-12-12'",
  "12.3456",
  "null"
)
charset_list = ( "gbk", "utf8")
length_semantics_list = ("byte", "char")

def print_head(fout):
  text = """
#owner: sean.yyj
#owner group: sql3
#tags: expr
"""
  fout.write(text)

def print_prepare_case(fout, charset):
  text = """
--echo # prepare_environment
let $char_charset = '%s';
--source mysql_test/include/charset/create_tenant_by_charset.inc
--source mysql_test/include/charset/conn_tenant.inc

--echo # create table for test
--result_format 3
let $table_name = t_deduce_length;
create table t_deduce_length(
    c_number number,
    c_raw raw(13),
    c_char char(13 byte),
    c_char_c char(13 char),
    c_varchar2 varchar2(13 byte),
    c_varchar2_c varchar2(13 char),
    c_varchar2_l varchar2(200 byte),
    c_nchar nchar(13),
    c_nvarchar nvarchar2(13),
    c_nvarchar_l nvarchar2(200),
    c_clob clob
);
desc t_deduce_length;

alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
alter session set NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF';
alter session set NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
""" % (charset)
  fout.write(text)

def print_cleanup(fout):
  text = """
--echo # cleanup
--result_format 1
purge recyclebin;
--source mysql_test/include/charset/drop_tenant.inc
"""
  fout.write(text)

def print_expr1(fout, expr_template, length_semantics):
  fout.write("\n--echo ########## start test #########\n")
  fout.write("alter session set NLS_LENGTH_SEMANTICS = '%s';\n" % (length_semantics))
  for param1 in param_list:
    column_expr = expr_template % (param1)
    fout.write("let $column_expr = %s;\n" % (column_expr))
    fout.write("--source mysql_test/test_suite/calc_result_length/include/ctas_and_desc.inc\n")
  fout.write("alter session set NLS_LENGTH_SEMANTICS = '%s';\n" % ("byte"))

def print_expr2(fout, expr_template, length_semantics):
  fout.write("\n--echo ########## start test #########\n")
  fout.write("alter session set NLS_LENGTH_SEMANTICS = '%s';\n" % (length_semantics))
  for param1 in param_list:
    for param2 in param_list:
      column_expr = expr_template % (param1, param2)
      fout.write("let $column_expr = %s;\n" % (column_expr))
      fout.write("--source mysql_test/test_suite/calc_result_length/include/ctas_and_desc.inc\n")
  fout.write("alter session set NLS_LENGTH_SEMANTICS = '%s';\n" % ("byte"))

def generate_test(expr_name, expr_template, param_num):
  for charset in charset_list:
    output_file = "expr_%s_%s_oracle.test" % (expr_name, charset)
    fout = open(output_file, 'w')
    print_head(fout)
    print_prepare_case(fout, charset)
    for length_semantics in length_semantics_list:
      if param_num == 1:
        print_expr1(fout, expr_template, length_semantics)
      elif param_num == 2:
        print_expr2(fout, expr_template, length_semantics)
      else:
        print "unsupported param_num %d for %s" % (param_num, expr_name)
    print_cleanup(fout)
    fout.close()

if __name__ == "__main__":
  generate_test("chr", "chr(%s)", 1)
  generate_test("concat", "concat(%s, %s)", 2)
  generate_test("initcap", "initcap(%s)", 1)
  generate_test("lower", "lower(%s)", 1)
  generate_test("lpad1", "lpad(%s, 23)", 1)
  generate_test("lpad2", "lpad(%s, c_number)", 1)
  generate_test("lpad3", "lpad(%s, 23, %s)", 2)
  generate_test("lpad4", "lpad(%s, c_number, %s)", 2)
  generate_test("ltrim1", "ltrim(%s)", 1)
  generate_test("ltrim2", "ltrim(%s, %s)", 2)
  generate_test("nls_lower", "nls_lower(%s)", 1)
  generate_test("nls_upper", "nls_upper(%s)", 1)
  generate_test("replace", "replace(%s, c_varchar2_l, %s)", 2)
  generate_test("rpad1", "rpad(%s, 23)", 1)
  generate_test("rpad2", "rpad(%s, c_number)", 1)
  generate_test("rpad3", "rpad(%s, 23, %s)", 2)
  generate_test("rpad4", "rpad(%s, c_number, %s)", 2)
  generate_test("rtrim1", "rtrim(%s)", 1)
  generate_test("rtrim2", "rtrim(%s, %s)", 2)
  generate_test("substr1", "substr(%s, 2)", 1)
  generate_test("substr2", "substr(%s, c_number)", 1)
  generate_test("substr3", "substr(%s, 2, 7)", 1)
  generate_test("substr4", "substr(%s, c_number, c_number)", 1)
  generate_test("substrb1", "substrb(%s, 2)", 1)
  generate_test("substrb2", "substrb(%s, c_number)", 1)
  generate_test("substrb3", "substrb(%s, 2, 7)", 1)
  generate_test("substrb4", "substrb(%s, c_number, c_number)", 1)
  generate_test("translate", "translate(%s, c_varchar2_l, c_varchar2)", 1)
  generate_test("translateusing1", "translate(%s using char_cs)", 1)
  generate_test("translateusing2", "translate(%s using nchar_cs)", 1)
  generate_test("upper", "upper(%s)", 1)
  print "done!"
