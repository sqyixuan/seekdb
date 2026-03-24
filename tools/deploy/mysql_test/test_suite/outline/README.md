Attention:由于GV$OB_PLAN_CACHE_PLAN_STAT中statement字段内容的变化，不要再运行generate.py了,等简葵出支持 let query_value =之后再调整修改generate.py
# 该目录主要用于使用mysqltest测试spm的功能,主要包括以下几方面
（1） outline的功能
（2） sql 限流功能
（3） spm baseline 生成以及演进

PART ONE: outline
##outline的功能点, outline是否生效以及准确性；

# 所有测试语句通过查看OB产品文档及参数其他测试case，对不同数据类型、表达式、函数、及各种dml语句进行了一定的覆盖。

# ./t目录下文件说明：
    1. outline\_basic.test－－测试outline哪些类型的sql能够匹配；
         * 测试思路：对一条sql，先执行一次, 并记录结果集，记为A；
                     在该sql上创建一个outline；
                     将该sql执行一遍，验证结果集B是否走了outline指定的hint,这里通过hint让sql走特定的索引，结果集的排序和未创建outline时执行的结果集A排序不一样，则表明，该sql的执行路径是outline指定的路径。
                     如果该sql中有常量，则改变常量，然后再执行一次；通过plan cache状态视图查看是否命中；
                     ⚠ ：添加新case时，需要保证前面没有类似的语句出现。
    2. outline\_check\_right.test -- 测试走outline之后plan 的准确性；
         * 测试思路：对一条sql，先关闭plan cache，将该sql改变常量后执行一次，记录结果，
                     然后开启plan cache，在改sql上创建一个outline，先执行原sql
                     再执行改变常量后的sql，将该次执行结果与之前记录的没有使用plan cache时的结果进行比对，一样说明正确；通过plan cache状态视图查看是否命中；
    3. outline\_no\_hint\_check\_hit.test -- 测试create outline 多样性,包括 delete 语句，insert 语句，update 语句，以及replace语句；
         * 测试思路：先将outline_no_hint.sql中的所有语句执行一遍，
                     然后对outline_no_hint.sql中的每行做如下处理：在该sql上创建一个outline，先执行原sql,查询hit_count和outline_id
                     再执行改变常量后的sql，通过plan cache状态视图查看命中情况；
    4. outline\_no\_hint\_check\_hit.test -- 测试走各种语句outline之后plan 的准确性；
         * 测试思路：先关闭plan cache，将该sql改变常量后执行一次，记录结果，
                     然后开启plan cache，将所有原始sql执行一遍，不记录结果，然后在每条原始sql上创建一个outline，之后将所有的原始sql执行一遍，
                     再将所有改变常量后的sql执行一遍，将该次执行结果与之前记录的没有使用plan cache时的结果进行比对，一样说明正确；

    5. outline.sql 所有测试的sql语句；
       outline_change\_param.sql--- plan\_cache.sql中所有sql语句改变常量值后的sql；
       outline_table_init.sql --- 创建及初始化以上两个文件中所涉及的表及数据;
       outline_init.sql --- 给outline.sql中的每条sql创建对应的outline;
       outline_no_hint.sql --- 不指定hint测试简单create outline语句;
       outline_no_hint_change_param.sql --- outline_no_hint.sql中sql语句改变常量值之后的sql;

    6. generate.py
       * 作用：通过outline.sql, outline_init.sql, outline_change\_param.sql, outline_table\_init.sql这四个文件，执行：./generate.py自动生成outline_basic.test和outline_check_right.test文件。
               通过outline_no_hint.sql, outline_no_hint_change_param.sql, outline_table_init.sql这三个文件，生成outline_no_hint_check_hit.test和outline_no_hint_check_right.test

# 添加case方法：
    * 在outline\_basic.test 和outline\_check\_right.test中添加：
       1 在outline.sql和outline_change_param.sql这两个文件的相同行中加入该sql及变化参数后的sql，且需要在outline_init.sql中添加对应的outline 创建语句,需要保证这个三个文件的行数一致
       2 然后在outline_table_init.sql文件中构造数据，
       3 最后执行./generate.py；即可生成outline_basic.test和outline_check_right.test文件
       ⚠ ：添加新case时，需要保证前面没有类似的语句出现。
    * outline\_no\_hint\_check\_hit.test和outline\_no\_hint\_check\_right.test
       1 在outline_no_hint.sql和outline_no_hint_change_param.sql这两个文件的相同行中加入该sql及变化参数后的sql，需要保证这个三个文件的行数一致
       3 最后执行./generate.py；即可生成outline_not_hint_check_hit.test和outline_no_hint_check_right.test文件
       ⚠ ：添加新case时，需要保证前面没有类似的语句出现。
