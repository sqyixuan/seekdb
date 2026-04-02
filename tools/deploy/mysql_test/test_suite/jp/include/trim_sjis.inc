#
# Test TRIM() function with Japanese characters sjis encoding
#

--character_set sjis
SET NAMES sjis;
SET character_set_database = sjis;

--disable_warnings
DROP TABLE IF EXISTS `ÇsÇP`;
DROP TABLE IF EXISTS `ÇsÇQ`;
DROP TABLE IF EXISTS `ÇsÇR`;
--enable_warnings
eval CREATE TABLE `ÇsÇP` (`ÇbÇP` CHAR(12), INDEX(`ÇbÇP`)) DEFAULT CHARSET = sjis;
eval CREATE TABLE `ÇsÇQ` (`ÇbÇP` CHAR(12), INDEX(`ÇbÇP`)) DEFAULT CHARSET = sjis;
eval CREATE TABLE `ÇsÇR` (`ÇbÇP` CHAR(12), INDEX(`ÇbÇP`)) DEFAULT CHARSET = sjis;

# Nothing to trim
INSERT INTO `ÇsÇP` VALUES ('±≤≥¥µ');
# Test data for TRIM(TRAILING [remstr] FROM str)
INSERT INTO `ÇsÇP` VALUES ('±≤≥¥µ∂');
INSERT INTO `ÇsÇP` VALUES ('±≤≥¥µ∂∂');
INSERT INTO `ÇsÇP` VALUES ('±≤≥¥µ∂∂∂');
# Test data for TRIM(LEADING [remstr] FROM str)
INSERT INTO `ÇsÇP` VALUES ('∂±≤≥¥µ');
INSERT INTO `ÇsÇP` VALUES ('∂∂±≤≥¥µ');
INSERT INTO `ÇsÇP` VALUES ('∂∂∂±≤≥¥µ');
# Test data for TRIM(BOTH [remstr] FROM str)
INSERT INTO `ÇsÇP` VALUES ('∂∂∂±≤≥¥µ∂∂∂');
# Test data for TRIM without [remstr] (remove spaces) 
INSERT INTO `ÇsÇP` VALUES ('   ±≤≥¥µ   ');
INSERT INTO `ÇsÇQ` VALUES ('Ç†Ç¢Ç§Ç¶Ç®');
INSERT INTO `ÇsÇQ` VALUES ('Ç†Ç¢Ç§Ç¶Ç®Ç©');
INSERT INTO `ÇsÇQ` VALUES ('Ç†Ç¢Ç§Ç¶Ç®Ç©Ç©');
INSERT INTO `ÇsÇQ` VALUES ('Ç†Ç¢Ç§Ç¶Ç®Ç©Ç©Ç©');
INSERT INTO `ÇsÇQ` VALUES ('Ç©Ç†Ç¢Ç§Ç¶Ç®');
INSERT INTO `ÇsÇQ` VALUES ('Ç©Ç©Ç†Ç¢Ç§Ç¶Ç®');
INSERT INTO `ÇsÇQ` VALUES ('Ç©Ç©Ç©Ç†Ç¢Ç§Ç¶Ç®');
INSERT INTO `ÇsÇQ` VALUES ('Ç©Ç©Ç©Ç†Ç¢Ç§Ç¶Ç®Ç©Ç©Ç©');
INSERT INTO `ÇsÇQ` VALUES ('   Ç†Ç¢Ç§Ç¶Ç®   ');
INSERT INTO `ÇsÇR` VALUES ('É\è\ï\ó\ç\');
INSERT INTO `ÇsÇR` VALUES ('É\è\ï\ó\ç\ñ\');
INSERT INTO `ÇsÇR` VALUES ('É\è\ï\ó\ç\ñ\ñ\');
INSERT INTO `ÇsÇR` VALUES ('É\è\ï\ó\ç\ñ\ñ\ñ\');
INSERT INTO `ÇsÇR` VALUES ('ñ\É\è\ï\ó\ç\');
INSERT INTO `ÇsÇR` VALUES ('ñ\ñ\É\è\ï\ó\ç\');
INSERT INTO `ÇsÇR` VALUES ('ñ\ñ\ñ\É\è\ï\ó\ç\');
INSERT INTO `ÇsÇR` VALUES ('ñ\ñ\ñ\É\è\ï\ó\ç\ñ\ñ\ñ\');
INSERT INTO `ÇsÇR` VALUES ('   É\è\ï\ó\ç\   ');

SELECT `ÇbÇP`,TRIM(TRAILING '∂' FROM `ÇbÇP`) FROM `ÇsÇP`;
SELECT `ÇbÇP`,TRIM(LEADING '∂' FROM `ÇbÇP`) FROM `ÇsÇP`;
SELECT `ÇbÇP`,TRIM(BOTH '∂' FROM `ÇbÇP`) FROM `ÇsÇP`;
SELECT `ÇbÇP`,TRIM(`ÇbÇP`) FROM `ÇsÇP`;
SELECT `ÇbÇP`,TRIM(TRAILING 'Ç©' FROM `ÇbÇP`) FROM `ÇsÇQ`;
SELECT `ÇbÇP`,TRIM(LEADING 'Ç©' FROM `ÇbÇP`) FROM `ÇsÇQ`;
SELECT `ÇbÇP`,TRIM(BOTH 'Ç©' FROM `ÇbÇP`) FROM `ÇsÇQ`;
SELECT `ÇbÇP`,TRIM(`ÇbÇP`) FROM `ÇsÇQ`;
SELECT `ÇbÇP`,TRIM(TRAILING 'ñ\' FROM `ÇbÇP`) FROM `ÇsÇR`;
SELECT `ÇbÇP`,TRIM(LEADING 'ñ\' FROM `ÇbÇP`) FROM `ÇsÇR`;
SELECT `ÇbÇP`,TRIM(BOTH 'ñ\' FROM `ÇbÇP`) FROM `ÇsÇR`;
SELECT `ÇbÇP`,TRIM(`ÇbÇP`) FROM `ÇsÇR`;

DROP TABLE `ÇsÇP`;
DROP TABLE `ÇsÇQ`;
DROP TABLE `ÇsÇR`;
