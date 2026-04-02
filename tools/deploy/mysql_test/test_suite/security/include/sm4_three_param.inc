--error 0,1235
select 1 as flag from dual where sm4_encrypt('asdasdasdasd', '12312313123', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj') = sm4_encrypt('asdasdasdasd', '12312313123', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj');
--error 0,1235
select hex(sm4_encrypt('asdasdasdasd', '12312313123', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'))  from dual;
--error 0,1235
select hex(sm4_encrypt('asdas123123dasdasd', '12312313123', '$$&$(*&(%*&(*%&($&*%&(dkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'))  from dual;
--error 0,1235
select hex(sm4_encrypt('asdasdasdasd', '', '1290380129038129038012938'))  from dual;
--error 0,1235
select hex(sm4_encrypt('', 'abc', '121903810293801289301298301982301928'))  from dual;
--error 0,1235
select hex(sm4_encrypt('asdasdasdasd', 'asdasdas', '1230121231230381209380913820912830'))  from dual;
--error 0,1235
select sm4_decrypt(sm4_encrypt('asdasdasdasd', '', '1290380129038129038012938'), '','1290380129038129038012938')  from dual;
--error 0,1235
select sm4_decrypt(sm4_encrypt('', 'abc', '121903810293801289301298301982301928'), 'abc', '121903810293801289301298301982301928')  from dual;
--error 0,1235
select sm4_decrypt(sm4_encrypt('asdasdasdasd', 'asdasdas', '1230121231230381209380913820912830'),'asdasdas', '1230121231230381209380913820912830') from dual;
--error 0,1235
select hex(sm4_encrypt('中文字符加一点', '你好', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'))  from dual;
--error 0,1235
select hex(sm4_encrypt('测试', '是不是', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'))  from dual;
--error 0,1235
select hex(sm4_encrypt('意义', '周三', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'))  from dual;
--error 0,1235
select hex(sm4_encrypt('午后的天气还不错', '莎士比亚', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'))  from dual;
--error 0,1235
select sm4_decrypt(sm4_encrypt('中文字符加一点', '你好', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'), '你好', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj') from dual;
--error 0,1235
select sm4_decrypt(sm4_encrypt('测试', '是不是', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'),'是不是','asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj') from dual;
--error 0,1235
select sm4_decrypt(sm4_encrypt('意义', '周三', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'),'周三','asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj') from dual;
--error 0,1235
select sm4_decrypt(sm4_encrypt('午后的天气还不错', '莎士比亚', 'asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj'),'莎士比亚','asdasdkljasdkljalskdjaklsdjaklsjdaklsdjlaksdj') from dual;
