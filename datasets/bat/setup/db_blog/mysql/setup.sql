CREATE TABLE `tbl_attachments`(
  `attachment_id` int NOT NULL,
  `post_id`
    int
  DEFAULT NULL,
  `user_id` int
  DEFAULT NULL,
  `file_name` varchar(255)
  DEFAULT NULL,
  `file_path` varchar(255)
  DEFAULT NULL,
  `file_size` int
  DEFAULT NULL,
  `file_type` varchar(50)
  DEFAULT NULL,
  `upload_date` datetime
  DEFAULT NULL,
  `description` text,
  `is_active` tinyint(1)
  DEFAULT NULL,
  `visibility_status` varchar(20)
  DEFAULT NULL,
  `download_count` int
  DEFAULT NULL,
  `file_extension` varchar(10)
  DEFAULT NULL,
  `uploaded_by_ip` varchar(15)
  DEFAULT NULL,
  `last_modified` datetime
  DEFAULT NULL,
  `expiration_date` datetime
  DEFAULT NULL,
  `tags_file` json
  DEFAULT NULL,
  `category_file` json
  DEFAULT NULL,
  `access_permissions` json
  DEFAULT NULL,
  `storage_location` varchar(255)
  DEFAULT NULL,
  `metadata` json
  DEFAULT NULL,
  `is_featured` tinyint(1)
  DEFAULT NULL,
  `parent_attachment_id` int
  DEFAULT NULL,
  PRIMARY KEY(`attachment_id`),
  KEY `fk_post_id`(`post_id`),
  KEY `fk_user_id`(`user_id`),
  KEY `fk_parent_attachment_id`(`parent_attachment_id`),
  CONSTRAINT `fk_parent_attachment_id`
  FOREIGN KEY(`parent_attachment_id`)
  REFERENCES `tbl_attachments`(`attachment_id`),
  CONSTRAINT `fk_post_id`
  FOREIGN KEY(`post_id`)
  REFERENCES `tbl_posts`(`post_id`),
  CONSTRAINT `fk_user_id`
  FOREIGN KEY(`user_id`)
  REFERENCES `tbl_users`(`user_id`));

CREATE TABLE `tbl_categories`(
  `category_id` int NOT NULL,
  `created_by`
    int
  DEFAULT NULL,
  `updated_by` int
  DEFAULT NULL,
  `name` varchar(255)
  DEFAULT NULL,
  `description` text,
  `created_at` timestamp NULL
  DEFAULT NULL,
  `updated_at` timestamp NULL
  DEFAULT NULL,
  `parent_category` int
  DEFAULT NULL,
  `slug` varchar(255)
  DEFAULT NULL,
  `image` varchar(255)
  DEFAULT NULL,
  `count` int
  DEFAULT NULL,
  `visibility` tinyint(1)
  DEFAULT NULL,
  `is_edited` tinyint(1)
  DEFAULT NULL,
  PRIMARY KEY(`category_id`),
  KEY `created_by`(`created_by`),
  KEY `updated_by`(`updated_by`),
  CONSTRAINT `tbl_categories_ibfk_1`
  FOREIGN KEY(`created_by`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_categories_ibfk_2`
  FOREIGN KEY(`updated_by`)
  REFERENCES `tbl_users`(`user_id`));

CREATE TABLE `tbl_categories_hierarchy`(
  `category_hierarchy_id` int NOT NULL,
  `category_id`
    int
  DEFAULT NULL,
  `created_by` int
  DEFAULT NULL,
  `updated_by` int
  DEFAULT NULL,
  `name` varchar(255)
  DEFAULT NULL,
  `description` text,
  `created_at` timestamp NULL
  DEFAULT NULL,
  `updated_at` timestamp NULL
  DEFAULT NULL,
  `status` varchar(50)
  DEFAULT NULL,
  `visibility` varchar(50)
  DEFAULT NULL,
  `orden_priority` int
  DEFAULT NULL,
  `icon` varchar(255)
  DEFAULT NULL,
  `color` varchar(50)
  DEFAULT NULL,
  `metadata` text,
  `parent_category_name` varchar(255)
  DEFAULT NULL,
  PRIMARY KEY(`category_hierarchy_id`),
  KEY `category_id`(`category_id`),
  KEY `created_by`(`created_by`),
  KEY `updated_by`(`updated_by`),
  CONSTRAINT `tbl_categories_hierarchy_ibfk_1`
  FOREIGN KEY(`category_id`)
  REFERENCES `tbl_categories`(`category_id`),
  CONSTRAINT `tbl_categories_hierarchy_ibfk_2`
  FOREIGN KEY(`created_by`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_categories_hierarchy_ibfk_3`
  FOREIGN KEY(`updated_by`)
  REFERENCES `tbl_users`(`user_id`));

CREATE TABLE `tbl_comments`(
  `comment_id` int NOT NULL,
  `content` text,
  `user_id`
    int
  DEFAULT NULL,
  `post_id` int
  DEFAULT NULL,
  `parent_comment_id` int
  DEFAULT NULL,
  `likes_count` int
  DEFAULT NULL,
  `created_at` timestamp NULL
  DEFAULT NULL,
  `updated_at` timestamp NULL
  DEFAULT NULL,
  `is_approved` tinyint(1)
  DEFAULT NULL,
  `dislikes_count` int
  DEFAULT NULL,
  `report_count` int
  DEFAULT NULL,
  `is_deleted` tinyint(1)
  DEFAULT NULL,
  `user_ip` varchar(255)
  DEFAULT NULL,
  `user_agent` varchar(255)
  DEFAULT NULL,
  `is_flagged` tinyint(1)
  DEFAULT NULL,
  `reply_count` int
  DEFAULT NULL,
  `mentions` json
  DEFAULT NULL,
  `attachments` json
  DEFAULT NULL,
  `metadata` json
  DEFAULT NULL,
  `is_edited` tinyint(1)
  DEFAULT NULL,
  PRIMARY KEY(`comment_id`),
  KEY `user_id`(`user_id`),
  KEY `post_id`(`post_id`),
  CONSTRAINT `tbl_comments_ibfk_1`
  FOREIGN KEY(`user_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_comments_ibfk_2`
  FOREIGN KEY(`post_id`)
  REFERENCES `tbl_posts`(`post_id`));

CREATE TABLE `tbl_country`(
  `country_name` varchar(255) NOT NULL,
  `country_id`
    int
  DEFAULT NULL,
  `continent` varchar(255)
  DEFAULT NULL,
  `region` varchar(255)
  DEFAULT NULL,
  `X` float
  DEFAULT NULL,
  `Y` float
  DEFAULT NULL,
  PRIMARY KEY(`country_name`),
  KEY `country_id`(`country_id`),
  CONSTRAINT `tbl_country_ibfk_1`
  FOREIGN KEY(`country_id`)
  REFERENCES `tbl_users`(`user_id`));

CREATE TABLE `tbl_followers`(
  `follower_id` int NOT NULL,
  `following_id`
    int
  DEFAULT NULL,
  `created_at` timestamp NULL
  DEFAULT NULL,
  `updated_at` timestamp NULL
  DEFAULT NULL,
  `follower_data` json
  DEFAULT NULL,
  PRIMARY KEY(`follower_id`));

CREATE TABLE `tbl_labels`(
  `label_id` int NOT NULL,
  `creator_id`
    int
  DEFAULT NULL,
  `updated_by` int
  DEFAULT NULL,
  `name` varchar(255)
  DEFAULT NULL,
  `description` text,
  `created_at` timestamp NULL
  DEFAULT NULL,
  `updated_at` timestamp NULL
  DEFAULT NULL,
  `post_count` int
  DEFAULT NULL,
  `visibility_status` varchar(50)
  DEFAULT NULL,
  `is_active` tinyint(1)
  DEFAULT NULL,
  `usage_frequency` int
  DEFAULT NULL,
  `parent_label_id` int
  DEFAULT NULL,
  PRIMARY KEY(`label_id`),
  KEY `creator_id`(`creator_id`),
  KEY `updated_by`(`updated_by`),
  KEY `parent_label_id`(`parent_label_id`),
  CONSTRAINT `tbl_labels_ibfk_1`
  FOREIGN KEY(`creator_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_labels_ibfk_2`
  FOREIGN KEY(`updated_by`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_labels_ibfk_3`
  FOREIGN KEY(`parent_label_id`)
  REFERENCES `tbl_labels`(`label_id`));

CREATE TABLE `tbl_media`(
  `media_id` int NOT NULL,
  `post_id`
    int
  DEFAULT NULL,
  `type` varchar(255)
  DEFAULT NULL,
  `url` varchar(255)
  DEFAULT NULL,
  `description` text,
  `upload_date` timestamp NULL
  DEFAULT NULL,
  `updated_at` timestamp NULL
  DEFAULT NULL,
  `filename` varchar(255)
  DEFAULT NULL,
  `file_size` int
  DEFAULT NULL,
  `caption` text,
  `uploader_id` int
  DEFAULT NULL,
  `visibility` varchar(50)
  DEFAULT NULL,
  `access_rights` json
  DEFAULT NULL,
  `file_path` varchar(255)
  DEFAULT NULL,
  `metadata` json
  DEFAULT NULL,
  `hash` varchar(255)
  DEFAULT NULL,
  `is_encrypted` tinyint(1)
  DEFAULT NULL,
  `encryption_key` varchar(255)
  DEFAULT NULL,
  `encryption_algorithm` varchar(50)
  DEFAULT NULL,
  `status_id` int
  DEFAULT NULL,
  PRIMARY KEY(`media_id`));

CREATE TABLE `tbl_notifications`(
  `notification_id` int NOT NULL,
  `user_id`
    int
  DEFAULT NULL,
  `notification_type` varchar(255)
  DEFAULT NULL,
  `sender_id` int
  DEFAULT NULL,
  `post_id` int
  DEFAULT NULL,
  `comment_id` int
  DEFAULT NULL,
  `notification_message` text,
  `notification_date` datetime
  DEFAULT NULL,
  `is_read` tinyint(1)
  DEFAULT NULL,
  `is_archived` tinyint(1)
  DEFAULT NULL,
  `is_deleted` tinyint(1)
  DEFAULT NULL,
  `link_to_notification` varchar(255)
  DEFAULT NULL,
  `notification_priority` int
  DEFAULT NULL,
  `additional_data` json
  DEFAULT NULL,
  `notification_subject` varchar(255)
  DEFAULT NULL,
  `notification_status` varchar(50)
  DEFAULT NULL,
  `notification_action` varchar(50)
  DEFAULT NULL,
  `related_user_id` int
  DEFAULT NULL,
  `source_application` varchar(255)
  DEFAULT NULL,
  `notification_category` varchar(255)
  DEFAULT NULL,
  `expiration_date` datetime
  DEFAULT NULL,
  `delivery_method` varchar(50)
  DEFAULT NULL,
  `notification_channel` varchar(50)
  DEFAULT NULL,
  `notification_language` varchar(50)
  DEFAULT NULL,
  `recipient_group` varchar(255)
  DEFAULT NULL,
  `notification_tags` json
  DEFAULT NULL,
  `notification_expiry_time` datetime
  DEFAULT NULL,
  `notification_context` json
  DEFAULT NULL,
  `notification_origin` varchar(255)
  DEFAULT NULL,
  PRIMARY KEY(`notification_id`),
  KEY `user_id`(`user_id`),
  KEY `sender_id`(`sender_id`),
  KEY `post_id`(`post_id`),
  CONSTRAINT `tbl_notifications_ibfk_1`
  FOREIGN KEY(`user_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_notifications_ibfk_2`
  FOREIGN KEY(`sender_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_notifications_ibfk_3`
  FOREIGN KEY(`post_id`)
  REFERENCES `tbl_posts`(`post_id`));

CREATE TABLE `tbl_posts`(
  `post_id` int NOT NULL,
  `user_id`
    int
  DEFAULT NULL,
  `title` varchar(255)
  DEFAULT NULL,
  `content` text,
  `published_at` datetime
  DEFAULT NULL,
  `is_published` tinyint(1)
  DEFAULT NULL,
  `views_count` int
  DEFAULT NULL,
  `likes_count` int
  DEFAULT NULL,
  `shares_count` int
  DEFAULT NULL,
  `created_at` datetime
  DEFAULT NULL,
  `updated_at` datetime
  DEFAULT NULL,
  `slug` varchar(255)
  DEFAULT NULL,
  `excerpt` text,
  `status_id` int
  DEFAULT NULL,
  `category_id` int
  DEFAULT NULL,
  `dislikes_count` int
  DEFAULT NULL,
  `comments_count` int
  DEFAULT NULL,
  `is_featured` tinyint(1)
  DEFAULT NULL,
  `author_name` varchar(255)
  DEFAULT NULL,
  `image_url` varchar(255)
  DEFAULT NULL,
  `tags` json
  DEFAULT NULL,
  `reading_time` int
  DEFAULT NULL,
  `external_link` varchar(255)
  DEFAULT NULL,
  `metadata` json
  DEFAULT NULL,
  PRIMARY KEY(`post_id`),
  KEY `user_id`(`user_id`),
  CONSTRAINT `tbl_posts_ibfk_1`
  FOREIGN KEY(`user_id`)
  REFERENCES `tbl_users`(`user_id`));

CREATE TABLE `tbl_posts_tags`(
  `post_id` int NOT NULL,
  `tag_id` int NOT NULL,
  `comments`
    json
  DEFAULT NULL,
  `label_status` varchar(50)
  DEFAULT NULL,
  `label_at` timestamp NULL
  DEFAULT NULL,
  PRIMARY KEY(`post_id`, `tag_id`),
  KEY `tag_id`(`tag_id`),
  CONSTRAINT `tbl_posts_tags_ibfk_1`
  FOREIGN KEY(`tag_id`)
  REFERENCES `tbl_tags`(`tag_id`),
  CONSTRAINT `tbl_posts_tags_ibfk_2`
  FOREIGN KEY(`post_id`)
  REFERENCES `tbl_posts`(`post_id`));

CREATE TABLE `tbl_private_messages`(
  `message_id` int NOT NULL,
  `sender_id`
    int
  DEFAULT NULL,
  `receiver_id` int
  DEFAULT NULL,
  `subject` varchar(255)
  DEFAULT NULL,
  `body` text,
  `sending_time` timestamp NULL
  DEFAULT NULL,
  `is_read` tinyint(1)
  DEFAULT NULL,
  `is_archived` tinyint(1)
  DEFAULT NULL,
  `is_deleted_by_sender` tinyint(1)
  DEFAULT NULL,
  `is_deleted_by_receiver` tinyint(1)
  DEFAULT NULL,
  `is_flagged` tinyint(1)
  DEFAULT NULL,
  `priority` int
  DEFAULT NULL,
  `attachment_id` int
  DEFAULT NULL,
  `conversation_id` int
  DEFAULT NULL,
  `status` varchar(50)
  DEFAULT NULL,
  `parent_message_id` int
  DEFAULT NULL,
  `read_timestamp` timestamp NULL
  DEFAULT NULL,
  `message_data` json
  DEFAULT NULL,
  PRIMARY KEY(`message_id`),
  KEY `sender_id`(`sender_id`),
  KEY `receiver_id`(`receiver_id`),
  KEY `attachment_id`(`attachment_id`),
  KEY `parent_message_id`(`parent_message_id`),
  CONSTRAINT `tbl_private_messages_ibfk_1`
  FOREIGN KEY(`sender_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_private_messages_ibfk_2`
  FOREIGN KEY(`receiver_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_private_messages_ibfk_3`
  FOREIGN KEY(`attachment_id`)
  REFERENCES `tbl_attachments`(`attachment_id`),
  CONSTRAINT `tbl_private_messages_ibfk_4`
  FOREIGN KEY(`parent_message_id`)
  REFERENCES `tbl_private_messages`(`message_id`));

CREATE TABLE `tbl_reactions`(
  `reaction_id` int NOT NULL,
  `comment_id`
    int
  DEFAULT NULL,
  `user_id` int
  DEFAULT NULL,
  `post_id` int
  DEFAULT NULL,
  `reaction_type` varchar(255)
  DEFAULT NULL,
  `reaction_date` timestamp NULL
  DEFAULT NULL,
  `reaction_score` int
  DEFAULT NULL,
  `reaction_text` text,
  `is_active` tinyint(1)
  DEFAULT NULL,
  `reaction_count` int
  DEFAULT NULL,
  `source_ip` varchar(255)
  DEFAULT NULL,
  `device_info` json
  DEFAULT NULL,
  PRIMARY KEY(`reaction_id`),
  KEY `comment_id`(`comment_id`),
  CONSTRAINT `tbl_reactions_ibfk_1`
  FOREIGN KEY(`comment_id`)
  REFERENCES `tbl_comments`(`comment_id`));

CREATE TABLE `tbl_reports`(
  `report_id` int NOT NULL,
  `user_id`
    int
  DEFAULT NULL,
  `post_id` int
  DEFAULT NULL,
  `report_reason` varchar(255)
  DEFAULT NULL,
  `report_timestamp` timestamp NULL
  DEFAULT NULL,
  `is_resolved` tinyint(1)
  DEFAULT NULL,
  `resolved_by` int
  DEFAULT NULL,
  `resolution_timestamp` timestamp NULL
  DEFAULT NULL,
  `report_comments` text,
  `severity_level` int
  DEFAULT NULL,
  `report_type` varchar(50)
  DEFAULT NULL,
  `report_entity_type` varchar(50)
  DEFAULT NULL,
  `report_entity_id` int
  DEFAULT NULL,
  `action_taken` text,
  `action_timestamp` timestamp NULL
  DEFAULT NULL,
  `report_ip_address` varchar(15)
  DEFAULT NULL,
  `reported_user_id` int
  DEFAULT NULL,
  `reported_comment_id` int
  DEFAULT NULL,
  `reported_user_reputation` int
  DEFAULT NULL,
  `report_category` varchar(50)
  DEFAULT NULL,
  `reported_context_text` text,
  `attachments` json
  DEFAULT NULL,
  `flagged_user` tinyint(1)
  DEFAULT NULL,
  `report_source` varchar(50)
  DEFAULT NULL,
  `report_feedback` text,
  `report_verification_status` varchar(50)
  DEFAULT NULL,
  `report_mechanism` varchar(50)
  DEFAULT NULL,
  `report_priority` int
  DEFAULT NULL,
  `assigned_to` int
  DEFAULT NULL,
  `report_history` json
  DEFAULT NULL,
  PRIMARY KEY(`report_id`),
  KEY `user_id`(`user_id`),
  KEY `post_id`(`post_id`),
  KEY `resolved_by`(`resolved_by`),
  KEY `reported_user_id`(`reported_user_id`),
  KEY `reported_comment_id`(`reported_comment_id`),
  CONSTRAINT `tbl_reports_ibfk_1`
  FOREIGN KEY(`user_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_reports_ibfk_2`
  FOREIGN KEY(`post_id`)
  REFERENCES `tbl_posts`(`post_id`),
  CONSTRAINT `tbl_reports_ibfk_3`
  FOREIGN KEY(`resolved_by`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_reports_ibfk_4`
  FOREIGN KEY(`reported_user_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_reports_ibfk_5`
  FOREIGN KEY(`reported_comment_id`)
  REFERENCES `tbl_comments`(`comment_id`));

CREATE TABLE `tbl_status`(
  `status_id` int NOT NULL,
  `Name` varchar(255) DEFAULT NULL, PRIMARY KEY(`status_id`));

CREATE TABLE `tbl_tags`(
  `tag_id` int NOT NULL,
  `creator_id`
    int
  DEFAULT NULL,
  `category_id` int
  DEFAULT NULL,
  `name` varchar(255)
  DEFAULT NULL,
  `description` text,
  `created_at` timestamp NULL
  DEFAULT NULL,
  `updated_at` timestamp NULL
  DEFAULT NULL,
  `slug` varchar(255)
  DEFAULT NULL,
  `count` int
  DEFAULT NULL,
  `visibility` tinyint(1)
  DEFAULT NULL,
  `active` tinyint(1)
  DEFAULT NULL,
  `parent_tag` int
  DEFAULT NULL,
  `status_id` int
  DEFAULT NULL,
  `access_level` int
  DEFAULT NULL,
  `is_approved` tinyint(1)
  DEFAULT NULL,
  `metadata` json
  DEFAULT NULL,
  `is_edited` tinyint(1)
  DEFAULT NULL,
  PRIMARY KEY(`tag_id`),
  KEY `creator_id`(`creator_id`),
  KEY `category_id`(`category_id`),
  CONSTRAINT `tbl_tags_ibfk_1`
  FOREIGN KEY(`creator_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_tags_ibfk_2`
  FOREIGN KEY(`category_id`)
  REFERENCES `tbl_categories`(`category_id`));

CREATE TABLE `tbl_tags_hierarchy`(
  `tag_hierarchy_id` int NOT NULL,
  `tag_id`
    int
  DEFAULT NULL,
  `relationship_type` varchar(255)
  DEFAULT NULL,
  `weight` decimal(10, 2)
  DEFAULT NULL,
  `created_at` timestamp NULL
  DEFAULT NULL,
  `updated_at` timestamp NULL
  DEFAULT NULL,
  `active` tinyint(1)
  DEFAULT NULL,
  `description` text,
  `creator_id` int
  DEFAULT NULL,
  `modifier_id` int
  DEFAULT NULL,
  `is_deleted` tinyint(1)
  DEFAULT NULL,
  `deleted_at` timestamp NULL
  DEFAULT NULL,
  `revision_history` json
  DEFAULT NULL,
  `comments` json
  DEFAULT NULL,
  PRIMARY KEY(`tag_hierarchy_id`));

CREATE TABLE `tbl_user_activity`(
  `activity_id` int NOT NULL,
  `user_id`
    int
  DEFAULT NULL,
  `activity_type` varchar(255)
  DEFAULT NULL,
  `activity_timestamp` timestamp NULL
  DEFAULT NULL,
  `post_id` int
  DEFAULT NULL,
  `comment_id` int
  DEFAULT NULL,
  `activity_description` text,
  `activity_source` varchar(255)
  DEFAULT NULL,
  `activity_location` varchar(255)
  DEFAULT NULL,
  `activity_duration` int
  DEFAULT NULL,
  `activity_device` varchar(255)
  DEFAULT NULL,
  `activity_status` varchar(50)
  DEFAULT NULL,
  `activity_impact` varchar(50)
  DEFAULT NULL,
  `activity_privacy_level` varchar(50)
  DEFAULT NULL,
  `activity_related_users` json
  DEFAULT NULL,
  `activiy_extra_data` json
  DEFAULT NULL,
  `ip_address` varchar(15)
  DEFAULT NULL,
  `user_agent` varchar(255)
  DEFAULT NULL,
  `url` varchar(255)
  DEFAULT NULL,
  `session_id` varchar(255)
  DEFAULT NULL,
  `browser` varchar(255)
  DEFAULT NULL,
  `operating_system` varchar(255)
  DEFAULT NULL,
  `referrer` varchar(255)
  DEFAULT NULL,
  `event_id` int
  DEFAULT NULL,
  `error_message` text,
  `location_coordinates` json
  DEFAULT NULL,
  `session_duration` int
  DEFAULT NULL,
  PRIMARY KEY(`activity_id`),
  KEY `user_id`(`user_id`),
  KEY `post_id`(`post_id`),
  KEY `comment_id`(`comment_id`),
  CONSTRAINT `tbl_user_activity_ibfk_1`
  FOREIGN KEY(`user_id`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_user_activity_ibfk_2`
  FOREIGN KEY(`post_id`)
  REFERENCES `tbl_posts`(`post_id`),
  CONSTRAINT `tbl_user_activity_ibfk_3`
  FOREIGN KEY(`comment_id`)
  REFERENCES `tbl_comments`(`comment_id`));

CREATE TABLE `tbl_user_roles`(
  `role_id` int NOT NULL,
  `created_by`
    int
  DEFAULT NULL,
  `updated_by` int
  DEFAULT NULL,
  `description` varchar(255)
  DEFAULT NULL,
  `created_at` timestamp NULL
  DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL
  DEFAULT CURRENT_TIMESTAMP,
  `permissions` json
  DEFAULT NULL,
  `role_name` varchar(50)
  DEFAULT NULL,
  `is_active` tinyint(1)
  DEFAULT NULL,
  `restrictions` json
  DEFAULT NULL,
  `expiry_date` date
  DEFAULT NULL,
  `created_by_ip` varchar(50)
  DEFAULT NULL,
  `updated_by_ip` varchar(50)
  DEFAULT NULL,
  `created_by_device` varchar(100)
  DEFAULT NULL,
  `updated_by_device` varchar(100)
  DEFAULT NULL,
  `active_sessions` int
  DEFAULT NULL,
  `last_sessions` json
  DEFAULT NULL,
  PRIMARY KEY(`role_id`),
  KEY `created_by`(`created_by`),
  KEY `updated_by`(`updated_by`),
  CONSTRAINT `tbl_user_roles_ibfk_1`
  FOREIGN KEY(`created_by`)
  REFERENCES `tbl_users`(`user_id`),
  CONSTRAINT `tbl_user_roles_ibfk_2`
  FOREIGN KEY(`updated_by`)
  REFERENCES `tbl_users`(`user_id`));

CREATE TABLE `tbl_users`(
  `user_id` int NOT NULL,
  `first_name`
    varchar(255)
  DEFAULT NULL,
  `last_name` varchar(255)
  DEFAULT NULL,
  `username` varchar(255)
  DEFAULT NULL,
  `password_hash` varchar(255)
  DEFAULT NULL,
  `social_media_links` json
  DEFAULT NULL,
  `contact_info` json
  DEFAULT NULL,
  `avatar_url` varchar(255)
  DEFAULT NULL,
  `bio` text,
  `website` varchar(255)
  DEFAULT NULL,
  `created_at` timestamp NULL
  DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL
  DEFAULT CURRENT_TIMESTAMP,
  `notification_preferences` varchar(255)
  DEFAULT NULL,
  `role_id` int
  DEFAULT NULL,
  `last_login` timestamp NULL
  DEFAULT NULL,
  `active` tinyint(1)
  DEFAULT NULL,
  PRIMARY KEY(`user_id`));
