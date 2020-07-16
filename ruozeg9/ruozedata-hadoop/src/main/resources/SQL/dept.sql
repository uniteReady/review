/*
Navicat MySQL Data Transfer

Source Server         : jkd开发环境
Source Server Version : 50640
Source Host           : 192.168.101.217:3306
Source Database       : test

Target Server Type    : MYSQL
Target Server Version : 50640
File Encoding         : 65001

Date: 2020-07-16 11:07:50
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for dept
-- ----------------------------
DROP TABLE IF EXISTS `dept`;
CREATE TABLE `dept` (
  `DEPTNO` int(11) DEFAULT NULL COMMENT '部门编号',
  `DNAME` varchar(100) DEFAULT NULL COMMENT '部门名称',
  `LOC` varchar(100) DEFAULT NULL COMMENT '部门所在位置'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='部门表';

-- ----------------------------
-- Records of dept
-- ----------------------------
INSERT INTO `dept` VALUES ('10', 'ACCOUNTING', 'NEW YORK');
INSERT INTO `dept` VALUES ('20', 'RESEARCH', 'DALLAS');
INSERT INTO `dept` VALUES ('30', 'SALES', 'CHICHGO');
INSERT INTO `dept` VALUES ('40', 'OPERATIONS', 'BOSTON');
INSERT INTO `dept` VALUES ('9999', 'QA', 'SZ');
