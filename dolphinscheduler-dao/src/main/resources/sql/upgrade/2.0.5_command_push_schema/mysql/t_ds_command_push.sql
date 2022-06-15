/*
Navicat MySQL Data Transfer

Source Server         : 10.10.80.72
Source Server Version : 50723
Source Host           : 10.10.80.72:3306
Source Database       : dolschmy

Target Server Type    : MYSQL
Target Server Version : 50723
File Encoding         : 65001

Date: 2022-05-26 08:58:21
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for t_ds_command_push
-- ----------------------------
DROP TABLE IF EXISTS `t_ds_command_push`;
CREATE TABLE `t_ds_command_push` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
  `command_type` tinyint(4) DEFAULT NULL COMMENT 'Command type: 0 start workflow, 1 start execution from current node, 2 resume fault-tolerant workflow, 3 resume pause process, 4 start execution from failed node, 5 complement, 6 schedule, 7 rerun, 8 pause, 9 stop, 10 resume waiting thread',
  `process_definition_code` bigint(20) NOT NULL COMMENT 'process definition code',
  `process_definition_version` int(11) DEFAULT '0' COMMENT 'process definition version',
  `process_instance_id` int(11) DEFAULT '0' COMMENT 'process instance id',
  `command_param` text COMMENT 'json command parameters',
  `task_depend_type` tinyint(4) DEFAULT NULL COMMENT 'Node dependency type: 0 current node, 1 forward, 2 backward',
  `failure_strategy` tinyint(4) DEFAULT '0' COMMENT 'Failed policy: 0 end, 1 continue',
  `warning_type` tinyint(4) DEFAULT '0' COMMENT 'Alarm type: 0 is not sent, 1 process is sent successfully, 2 process is sent failed, 3 process is sent successfully and all failures are sent',
  `warning_group_id` int(11) DEFAULT NULL COMMENT 'warning group',
  `schedule_time` datetime DEFAULT NULL COMMENT 'schedule time',
  `start_time` datetime DEFAULT NULL COMMENT 'start time',
  `executor_id` int(11) DEFAULT NULL COMMENT 'executor id',
  `update_time` datetime DEFAULT NULL COMMENT 'update time',
  `process_instance_priority` int(11) DEFAULT NULL COMMENT 'process instance priority: 0 Highest,1 High,2 Medium,3 Low,4 Lowest',
  `worker_group` varchar(64) DEFAULT NULL COMMENT 'worker group',
  `environment_code` bigint(20) DEFAULT '-1' COMMENT 'environment code',
  `dry_run` tinyint(4) DEFAULT '0' COMMENT 'dry run flag：0 normal, 1 dry run',
  `dep_data_names` varchar(256) NOT NULL COMMENT 'dependent name such as: ,a,b,c,d',
  `dep_data_time_replaced_name` varchar(64) NOT NULL COMMENT 'replaced with dependent time such as: g_date_time',
  `online_flag` tinyint(4) DEFAULT '0' COMMENT 'staus：0 offline, 1 online',
  PRIMARY KEY (`id`),
  KEY `push_priority_id_index` (`process_instance_priority`,`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE TABLE `t_ds_command_push_waiting` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
  `process_definition_code` bigint(20) NOT NULL COMMENT 'process definition code',
  `command_push_id` int(11) DEFAULT '0' COMMENT 'command_push id',
  `dep_data_name` varchar(64) DEFAULT NULL COMMENT 'msg info data name',
  `dep_data_count` int(11) DEFAULT NULL COMMENT 'msg data : data count',
  `dep_data_period` int(11) DEFAULT NULL COMMENT 'msg data : period',
  `dep_data_time` datetime DEFAULT NULL COMMENT 'msg data time',
  `dep_data_param` text COMMENT 'json command parameters',
  `insert_time` datetime DEFAULT NULL COMMENT 'start time',
  `update_time` datetime DEFAULT NULL COMMENT 'update time',
  `receive_flag` tinyint(4) DEFAULT '0' COMMENT 'staus：0 unreceived, 1 received',
  `handled_flag` tinyint(4) DEFAULT '0' COMMENT 'staus：0 unhandled, 1 handled',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;