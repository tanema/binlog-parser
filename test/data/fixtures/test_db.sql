CREATE DATABASE `test_db` DEFAULT CHARACTER SET utf8;
USE `test_db`;
CREATE TABLE `buildings` ( `building_no` int(11) NOT NULL AUTO_INCREMENT, `building_name` varchar(255) NOT NULL, `address` varchar(355) NOT NULL, PRIMARY KEY (`building_no`)) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;
CREATE TABLE `departments` ( `dept_no` char(4) NOT NULL, `dept_name` varchar(40) NOT NULL, PRIMARY KEY (`dept_no`), UNIQUE KEY `dept_name` (`dept_name`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `filler` ( `id` int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=MEMORY DEFAULT CHARSET=utf8;
CREATE TABLE `language` ( `language_id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT, `name` char(20) NOT NULL, `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, `some_field` varchar(255) DEFAULT NULL, PRIMARY KEY (`language_id`)) ENGINE=InnoDB AUTO_INCREMENT=72 DEFAULT CHARSET=utf8;
CREATE TABLE `lookup` ( `id` int(11) NOT NULL, `value` int(11) NOT NULL, `shorttxt` text NOT NULL, `longtxt` text NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `rooms` ( `room_no` int(11) NOT NULL AUTO_INCREMENT, `room_name` varchar(255) NOT NULL, `building_no` int(11) NOT NULL, PRIMARY KEY (`room_no`), KEY `building_no` (`building_no`), CONSTRAINT `rooms_ibfk_1` FOREIGN KEY (`building_no`) REFERENCES `buildings` (`building_no`) ON DELETE CASCADE) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;
