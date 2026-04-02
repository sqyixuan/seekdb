# 该目录主要用于使用mysqltest测试plan cache的功能点，匹配及准确性；

# 所有测试语句通过查看OB产品文档及参数其他测试case，对不同数据类型、表达式、函数、及各种dml语句进行了一定的覆盖。

# ./t目录下文件说明：
    1. plan_cache_basic.test－－测试plan cache哪些类型的sql能够匹配；
         * 测试思路：对一条sql，先执行一次；
                     如果该sql中有常量，则改变常量，
                     然后再执行一次；通过plan cache状态视图查看是否命中；
                     ⚠ ：添加新case时，需要保证前面没有类似的语句出现。
    2. plan_cache_check_right.test -- 测试plan cache命中后plan 的准确性；
         * 测试思路：对一条sql，先关闭plan cache，将该sql改变常量后执行一次，记录结果，
                     然后开启plan cache，先执行原sql，不记录结果，
                     再执行改变常量后的sql，将该次执行结果与之前记录的没有使用plan cache时的结果进行比对，一样说明正确；
    3. plan_cache_update.test -- 测试某些属性改变后，plan cache能否及时更新plan；
         * 测试思路：先执行一条sql，然后改变某些需要更新plan的属性，
                     再执行两次该sql，通过plan cache状态视图查看该sql是否为被命中1次（第一次进入plan cache不算命中次数），如果是，则符合预期。


    4. plan_cache.sql 所有测试的sql语句；
       plan_cache_change\_param.sql--- plan\_cache.sql中所有sql语句改变常量值后的sql；
       plan_cache_table_init.sql --- 创建及初始化以上两个文件中所涉及的表及数据;

    5. generate.py
       * 作用：通过plan_cache.sql,plan_cache_change\_param.sql, plan_cache_table\_init.sql这三个文件，执行：python generate.py自动生成plan\_cache_basic.test和plan_cache_check_right.test文件。


# 添加case方法：
    * 在plan_cache\_basic.test 和plan\_cache_check_right.test中添加：
       1 在plan_cache.sql和plan_cache_change_param.sql这两个文件的相同行中加入该sql及变化参数后的sql，
       2 然后在plan_cache_table_init.sql文件中构造数据，
       3 最后执行python generate.py；即可生成plan_cache_basic.test和plan_cache_check_right.test文件
       ⚠ ：添加新case时，需要保证前面没有类似的语句出现。
    * 在plan_cache_update.test中添加case，直接手动添加即可。
