--error 0,1235
select 1 as flag from dual where sm4_encrypt('abc', '&*!') = sm4_encrypt('abc', '&*!'); 
--error 0,1235
select hex(sm4_encrypt('abcd', 'asd')) from dual; 
--error 0,1235
select hex(sm4_encrypt('asdiajsldkjasldjaljaskldjalskdjalskj', '123123jasldkjlkj1')) from dual; 
--error 0,1235
select hex(sm4_encrypt('lalalla', 'daskldalsdkaldk')) from dual; 
--error 0,1235
select hex(sm4_encrypt(12564765764343423423473,5754343243243243123)) from dual; 
--error 0,1235
select hex(sm4_encrypt('asdasd', '!%@#$')) from dual; 
--error 0,1235
select hex(sm4_encrypt('hahahaa', 'aklsjdlkajsdklajsdkllkjalsjdlakjsl')) from dual; 
--error 0,1235
select hex(sm4_encrypt('aklsdjalkjsdlkajsdklajsdlajsdlkajsdljkasldjkalsdjalskdjalksdjaklsjdlaksjdlaksjdlalskdjalsjkdalsjkdaklsdj', 'asdl;ajksdlaksdlasdlasdkl;asd;aklsd;alskd;aksd;aks;alsdkas;ldka;lksda;kls;')) from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt('abcd', 'asd'), 'asd') from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt('asdiajsldkjasldjaljaskldjalskdjalskj', '123123jasldkjlkj1'), '123123jasldkjlkj1') from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt('lalalla', 'daskldalsdkaldk'),'daskldalsdkaldk') from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt(12564765764343423423473,5754343243243243123), 5754343243243243123) from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt('asdasd', '!%@#$'), '!%@#$') from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt('hahahaa', 'aklsjdlkajsdklajsdkllkjalsjdlakjsl'), 'aklsjdlkajsdklajsdkllkjalsjdlakjsl') from dual; 
--error 0,1235
select hex(sm4_decrypt(sm4_encrypt('aklsdjalkjsdlkajsdklajsdlajsdlkajsdljkasldjkalsdjalskdjalksdjaklsjdlaksjdlaksjdlalskdjalsjkdalsjkdaklsdj', 'asdl;ajksdlaksdl'), 'asdl;ajksdlaksdl')) from dual; 
--error 0,1235
select hex(sm4_decrypt(1, 2)) from dual; 
--error 0,1235
select hex(sm4_decrypt(1.2, 3.6)) from dual; 
--error 0,1235
select hex(sm4_decrypt('abc', 'dbc')) from dual;
--error 0,1235
select hex(sm4_encrypt('断剑重铸之时，英雄归来之日', 'asd')) from dual; 
--error 0,1235
select hex(sm4_encrypt('我用双手成就你的梦想', 'asd')) from dual; 
--error 0,1235
select hex(sm4_encrypt('没有什么好恐惧的，除了恐惧本身', 'asd')) from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt('断剑重铸之时，英雄归来之日', 'asd'), 'asd') from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt('我用双手成就你的梦想', 'asd'), 'asd') from dual; 
--error 0,1235
select sm4_decrypt(sm4_encrypt('没有什么好恐惧的, 除了恐惧本身', 'asd'), 'asd') from dual;
