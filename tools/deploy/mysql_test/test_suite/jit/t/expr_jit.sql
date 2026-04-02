select int_c or '1', int_c + 1, int_c - 10, int_c + bigint_c, int_c = bigint_c, int_c = varchar_c from jit_t1;
select varchar_c or '1', varchar_c and '1', varchar_c + 1, varchar_c = '1', varchar_c in (1,2), varchar_c in ('hello', '1') from jit_t1;
select case when varchar_c then bigint_c + 1 else bigint_c + '1' end from jit_t1;
select  sum(bigint_c + 1)+1 as c1, sum(bigint_c - 1)-1 as c2, sum(bigint_c + 1)-1 as c3 from jit_t1;
select  case when varchar_c = 'hello' then bigint_c + 10 else bigint_c - 10 end as c1 from jit_t1 limit 8;
select  if(bigint_c = 1, bigint_c, bigint_c - 5) as c1 from jit_t1;
select  '1', 2, NULL from jit_t1;
select  tinyint_c is null, tinyint_c = 1, tinyint_c in (1,2) from jit_t1;

SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF((settletype=1 OR settletype=2),realamount,0)) realAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(settleType=2, (settleamount-realamount), 0)) closeAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(settleType=3, settleamount, 0)) refundAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN settleAmount!=0 AND (settletype=1 OR settletype=2) THEN floor(commision * (realamount/settleAmount)) ELSE 0 END) commisionAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='TKP', IF((settletype=1 OR settletype=2),realamount,0), 0)) tkpAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=1, realamount, 0),0), 0)) tkpB2cAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=2, realamount, 0),0), 0)) tkpC2cAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=3, realamount, 0),0), 0)) tkpEtaoAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=4, realamount, 0),0), 0)) tkpTmallHKAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='TANX', IF((settletype=1 OR settletype=2),realamount,0), 0)) tanxAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TANX' AND earningsource=24341404 AND settletype IN(1,2) THEN realamount ELSE 0 END) tanxAmount_b2b FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TANX' AND earningsource=29089295 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_p4psingle FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TANX' AND earningsource=29089295 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_p4psingle_tmall_hk FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TANX' AND earningsource=17534123 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_p4ppicword FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TANX' AND earningsource=17534123 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_p4ppicword_tmall_hk FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TANX' AND earningsource=17825897 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_ecpm FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TANX' AND earningsource=17825897 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_ecpm_tmall_hk FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF((transtype='CPA' OR transtype='CPSA'), IF((settletype=1 OR settletype=2),realamount,0),0)) cpsAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(earningtype=1,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'),realamount,0),0),0)) cpsB2cBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(earningtype=2,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'), realamount,0),0),0)) cpsJhsBonusAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(earningtype=3,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'),realamount,0),0),0)) tripBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='CPSV', IF((settletype=1 OR settletype=2),IF(subType=1, realamount, 0),0),0)) cpsvB2cAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='CPSV', IF((settletype=1 OR settletype=2),IF(subType=2, realamount, 0),0),0)) cpsvC2cAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(earningtype=1,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvB2cBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(earningtype=2,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvJhsBonusAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(earningtype=3,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvTripBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='CJHB' AND settletype IN(1,2) THEN realamount ELSE 0 END) AS cjhbAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=0 THEN realamount ELSE 0 END) AS cjhbOriginal FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=1 THEN realamount ELSE 0 END) AS cjhbB2cBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=2 THEN realamount ELSE 0 END) AS cjhbJhsBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=3 THEN realamount ELSE 0 END) AS cjhbTripBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TKA' AND settletype IN(1,2) THEN realamount ELSE 0 END) AS tkaAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=1 THEN realamount ELSE 0 END) AS tkaB2cBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=2 THEN realamount ELSE 0 END) AS tkaJhsBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=3 THEN realamount ELSE 0 END) AS tkaTripBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=4 THEN realamount ELSE 0 END) AS tkaTmallHkBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(earningtype=1,IF(settletype=3, settleamount, 0),0)) refundBonusAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(earningtype=2,IF(settletype=3, settleamount, 0),0)) refundJhsBonusAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(CASE WHEN transType='TKM' AND settletype IN(1,2) THEN realamount ELSE 0 END) tkmAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='WAPP', IF((settletype=1 OR settletype=2),realamount,0), 0)) wappamount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='MOA', IF((settletype=1 OR settletype=2),realamount,0), 0)) moaAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/(IF(transType='CJZ', IF((settletype=1 OR settletype=2),realamount,0), 0)) cjzAmount FROM dayincome;

SELECT /*+ USE_JIT(FORCE)*/pubId pubId FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(settleamount) settleAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF((settletype=1 OR settletype=2),realamount,0)) realAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(settleType=2, (settleamount-realamount), 0)) closeAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(settleType=3, settleamount, 0)) refundAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN settleAmount!=0 AND (settletype=1 OR settletype=2) THEN floor(commision * (realamount/settleAmount)) ELSE 0 END) commisionAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),realamount,0), 0)) tkpAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=1, realamount, 0),0), 0)) tkpB2cAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=2, realamount, 0),0), 0)) tkpC2cAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=3, realamount, 0),0), 0)) tkpEtaoAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=4, realamount, 0),0), 0)) tkpTmallHKAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='TANX', IF((settletype=1 OR settletype=2),realamount,0), 0)) tanxAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TANX' AND earningsource=24341404 AND settletype IN(1,2) THEN realamount ELSE 0 END) tanxAmount_b2b FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TANX' AND earningsource=29089295 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_p4psingle FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TANX' AND earningsource=29089295 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_p4psingle_tmall_hk FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TANX' AND earningsource=17534123 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_p4ppicword FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TANX' AND earningsource=17534123 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_p4ppicword_tmall_hk FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TANX' AND earningsource=17825897 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_ecpm FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TANX' AND earningsource=17825897 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_ecpm_tmall_hk FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF((transtype='CPA' OR transtype='CPSA'), IF((settletype=1 OR settletype=2),realamount,0),0)) cpsAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(earningtype=1,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'),realamount,0),0),0)) cpsB2cBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(earningtype=2,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'), realamount,0),0),0)) cpsJhsBonusAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(earningtype=3,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'),realamount,0),0),0)) tripBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='CPSV', IF((settletype=1 OR settletype=2),IF(subType=1, realamount, 0),0),0)) cpsvB2cAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='CPSV', IF((settletype=1 OR settletype=2),IF(subType=2, realamount, 0),0),0)) cpsvC2cAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(earningtype=1,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvB2cBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(earningtype=2,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvJhsBonusAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(earningtype=3,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvTripBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) THEN realamount ELSE 0 END) AS cjhbAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=0 THEN realamount ELSE 0 END) AS cjhbOriginal FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=1 THEN realamount ELSE 0 END) AS cjhbB2cBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=2 THEN realamount ELSE 0 END) AS cjhbJhsBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=3 THEN realamount ELSE 0 END) AS cjhbTripBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) THEN realamount ELSE 0 END) AS tkaAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=1 THEN realamount ELSE 0 END) AS tkaB2cBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=2 THEN realamount ELSE 0 END) AS tkaJhsBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=3 THEN realamount ELSE 0 END) AS tkaTripBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=4 THEN realamount ELSE 0 END) AS tkaTmallHkBonus FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(earningtype=1,IF(settletype=3, settleamount, 0),0)) refundBonusAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(earningtype=2,IF(settletype=3, settleamount, 0),0)) refundJhsBonusAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(CASE WHEN transType='TKM' AND settletype IN(1,2) THEN realamount ELSE 0 END) tkmAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='WAPP', IF((settletype=1 OR settletype=2),realamount,0), 0)) wappamount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='MOA', IF((settletype=1 OR settletype=2),realamount,0), 0)) moaAmount FROM dayincome;
SELECT /*+ USE_JIT(FORCE)*/SUM(IF(transType='CJZ', IF((settletype=1 OR settletype=2),realamount,0), 0)) cjzAmount FROM dayincome;

--sorted_result
SELECT /*+ USE_JIT(FORCE)*/pubId pubId, SUM(settleamount) settleAmount, SUM(IF((settletype=1 OR settletype=2),realamount,0)) realAmount, SUM(IF(settleType=2, (settleamount-realamount), 0)) closeAmount, SUM(IF(settleType=3, settleamount, 0)) refundAmount, SUM(CASE WHEN settleAmount!=0 AND (settletype=1 OR settletype=2) THEN floor(commision * (realamount/settleAmount)) ELSE 0 END) commisionAmount, SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),realamount,0), 0)) tkpAmount, SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=1, realamount, 0),0), 0)) tkpB2cAmount, SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=2, realamount, 0),0), 0)) tkpC2cAmount, SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=3, realamount, 0),0), 0)) tkpEtaoAmount, SUM(IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=4, realamount, 0),0), 0)) tkpTmallHKAmount, SUM(IF(transType='TANX', IF((settletype=1 OR settletype=2),realamount,0), 0)) tanxAmount, SUM(CASE WHEN transType='TANX' AND earningsource=24341404 AND settletype IN(1,2) THEN realamount ELSE 0 END) tanxAmount_b2b, SUM(CASE WHEN transType='TANX' AND earningsource=29089295 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_p4psingle, SUM(CASE WHEN transType='TANX' AND earningsource=29089295 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_p4psingle_tmall_hk, SUM(CASE WHEN transType='TANX' AND earningsource=17534123 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_p4ppicword, SUM(CASE WHEN transType='TANX' AND earningsource=17534123 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_p4ppicword_tmall_hk, SUM(CASE WHEN transType='TANX' AND earningsource=17825897 AND settletype IN(1,2) AND (subtype IS NULL OR subtype IN(1,2)) THEN realamount ELSE 0 END) tanxAmount_ecpm, SUM(CASE WHEN transType='TANX' AND earningsource=17825897 AND settletype IN(1,2) AND subtype = 4 THEN realamount ELSE 0 END) tanxAmount_ecpm_tmall_hk, SUM(IF((transtype='CPA' OR transtype='CPSA'), IF((settletype=1 OR settletype=2),realamount,0),0)) cpsAmount, SUM(IF(earningtype=1,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'),realamount,0),0),0)) cpsB2cBonus, SUM(IF(earningtype=2,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'), realamount,0),0),0)) cpsJhsBonusAmount, SUM(IF(earningtype=3,IF((settletype=1 OR settletype=2),IF((transtype='CPA' OR transtype='CPSA'),realamount,0),0),0)) tripBonus, SUM(IF(transType='CPSV', IF((settletype=1 OR settletype=2),IF(subType=1, realamount, 0),0),0)) cpsvB2cAmount, SUM(IF(transType='CPSV', IF((settletype=1 OR settletype=2),IF(subType=2, realamount, 0),0),0)) cpsvC2cAmount, SUM(IF(earningtype=1,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvB2cBonus, SUM(IF(earningtype=2,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvJhsBonusAmount, SUM(IF(earningtype=3,IF((settletype=1 OR settletype=2),IF(transtype='CPSV',realamount,0),0),0)) cpsvTripBonus, SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) THEN realamount ELSE 0 END) AS cjhbAmount, SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=0 THEN realamount ELSE 0 END) AS cjhbOriginal, SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=1 THEN realamount ELSE 0 END) AS cjhbB2cBonus, SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=2 THEN realamount ELSE 0 END) AS cjhbJhsBonus, SUM(CASE WHEN transType='CJHB' AND settletype IN(1,2) AND earningtype=3 THEN realamount ELSE 0 END) AS cjhbTripBonus, SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) THEN realamount ELSE 0 END) AS tkaAmount, SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=1 THEN realamount ELSE 0 END) AS tkaB2cBonus, SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=2 THEN realamount ELSE 0 END) AS tkaJhsBonus, SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=3 THEN realamount ELSE 0 END) AS tkaTripBonus, SUM(CASE WHEN transType='TKA' AND settletype IN(1,2) AND earningtype=4 THEN realamount ELSE 0 END) AS tkaTmallHkBonus, SUM(IF(earningtype=1,IF(settletype=3, settleamount, 0),0)) refundBonusAmount, SUM(IF(earningtype=2,IF(settletype=3, settleamount, 0),0)) refundJhsBonusAmount, SUM(CASE WHEN transType='TKM' AND settletype IN(1,2) THEN realamount ELSE 0 END) tkmAmount, SUM(IF(transType='WAPP', IF((settletype=1 OR settletype=2),realamount,0), 0)) wappamount, SUM(IF(transType='MOA', IF((settletype=1 OR settletype=2),realamount,0), 0)) moaAmount, SUM(IF(transType='CJZ', IF((settletype=1 OR settletype=2),realamount,0), 0)) cjzAmount FROM dayincome GROUP BY pubid;

# filter expr using jit compilation
# is nulll only supports tinyint
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE settletype is null or subType is null;
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE settletype is null and subType is null;

# in, not in, only support bigint
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid IN (40);
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid IN (11, 34);
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid IN (11, 34) or pubid IN (40, 11);

SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid NOT IN (11);
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid NOT IN (11, 34);
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid NOT IN (11, 34) or pubid IN (40, 11);

# equal expr, supports bigint and varchar
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid = 40 or transType = 'WAPP';
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid = 11 or transType = 'CPSV';

# add and minus, only bigint is supported
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE settleamount + 1 = 3;
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE settleamount - 1  = 1;
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE 1 + 2 - 3 + 4 = 4;
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE 1 + 3 - 3 + 4 = 5;

# cmp operator, only bigint is supported
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid < 34 or settleamount > 1;
SELECT /*+ USE_JIT(FORCE)*/(settleamount) settleAmount FROM dayincome WHERE pubid <= 34 or settleamount + 1 >= 2;

# compilicate sqls
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE CASE WHEN transType='CJHB' THEN 1 ELSE 0 END;
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF((settletype=1 OR settletype=2),1,0);
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF(settleType=2, (settleamount-realamount), 0);
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF(transType='TKP', IF((settletype=1 OR settletype=2),realamount,0), 0);
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=1, realamount, 0),0), 0);
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=2, realamount, 0),0), 0);
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=3, realamount, 0),0), 0);
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF(transType='TKP', IF((settletype=1 OR settletype=2),IF(subType=4, realamount, 0),0), 0);
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF(transType='TANX', IF((settletype=1 OR settletype=2),realamount,0), 0);
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome WHERE IF((transtype='CPA' OR transtype='CPSA'), IF((settletype=1 OR settletype=2),realamount,0),0);

SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome GROUP BY pubid HAVING SUM(IF(transType='WAPP', IF((settletype=1 OR settletype=2),realamount,0), 0)) >= 0;
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome GROUP BY pubid HAVING SUM(IF(transType='MOA', IF((settletype=1 OR settletype=2),realamount,0), 0)) >= 0;
SELECT /*+ USE_JIT(FORCE)*/* FROM dayincome GROUP BY pubid HAVING SUM(IF(transType='CJZ', IF((settletype=1 OR settletype=2),realamount,0), 0)) >= 0;
