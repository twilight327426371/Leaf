SET @saved_cs_client = @@character_set_client;
SET character_set_client = utf8;

DROP DATABASE IF EXISTS `leaf`;
CREATE DATABASE `leaf` ;
USE `leaf`;

CREATE TABLE `leaderboards` (
	`lid` MEDIUMINT(8) unsigned NOT NULL AUTO_INCREMENT,
	`name` VARCHAR(124) NOT NULL,


	PRIMARY KEY (`lid`),
  	KEY `name` (`name`)
) ENGINE=InnoDB CHARSET=utf8;


CREATE TABLE `entries` (
	`eid` INT(11) unsigned NOT NULL,
	`lid` MEDIUMINT(8) unsigned NOT NULL,
	`score` INT(11) unsigned NOT NULL,

	PRIMARY KEY (`lid`, `eid`),
	KEY `user_entry` (`lid`, `score`)

) ENGINE=InnoDB CHARSET=utf8;


CREATE TABLE `score_buckets` (
    
    `lid` MEDIUMINT(8) unsigned NOT NULL,
    `score` INT(11) unsigned NOT NULL,
    `size` INT(11) unsigned NOT NULL,

    KEY `leaderboard_score` (`lid`, `score`)

) ENGINE=InnoDB CHARSET=utf8;

SET @@character_set_client = @saved_cs_client;