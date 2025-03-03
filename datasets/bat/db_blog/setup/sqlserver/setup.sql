CREATE TABLE dbo.tbl_attachments (
  attachment_id INT NOT NULL PRIMARY KEY,
  post_id INT NULL,
  user_id INT NULL,
  file_name NVARCHAR(255) NULL,
  file_path NVARCHAR(255) NULL,
  file_size INT NULL,
  file_type NVARCHAR(50) NULL,
  upload_date DATETIME NULL,
  description TEXT NULL,
  is_active BIT NULL,
  visibility_status NVARCHAR(20) NULL,
  download_count INT NULL,
  file_extension NVARCHAR(10) NULL,
  uploaded_by_ip NVARCHAR(15) NULL,
  last_modified DATETIME NULL,
  expiration_date DATETIME NULL,
  tags_file NVARCHAR(MAX) NULL,  
  category_file NVARCHAR(MAX) NULL,
  access_permissions NVARCHAR(MAX) NULL,
  storage_location NVARCHAR(255) NULL,
  metadata NVARCHAR(MAX) NULL,
  is_featured BIT NULL,
  parent_attachment_id INT NULL
);

CREATE TABLE dbo.tbl_categories (
  category_id INT NOT NULL PRIMARY KEY,
  created_by INT NULL,
  updated_by INT NULL,
  name NVARCHAR(255) NULL,
  description TEXT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  parent_category INT NULL,
  slug NVARCHAR(255) NULL,
  image NVARCHAR(255) NULL,
  count INT NULL,
  visibility BIT NULL,
  is_edited BIT NULL
);

CREATE TABLE dbo.tbl_categories_hierarchy (
  category_hierarchy_id INT NOT NULL PRIMARY KEY,
  category_id INT NULL,
  created_by INT NULL,
  updated_by INT NULL,
  name NVARCHAR(255) NULL,
  description TEXT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  status NVARCHAR(50) NULL,
  visibility NVARCHAR(50) NULL,
  orden_priority INT NULL,
  icon NVARCHAR(255) NULL,
  color NVARCHAR(50) NULL,
  metadata TEXT NULL,
  parent_category_name NVARCHAR(255) NULL
);

CREATE TABLE dbo.tbl_comments (
  comment_id INT NOT NULL PRIMARY KEY,
  content TEXT NULL,
  user_id INT NULL,
  post_id INT NULL,
  parent_comment_id INT NULL,
  likes_count INT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  is_approved BIT NULL,
  dislikes_count INT NULL,
  report_count INT NULL,
  is_deleted BIT NULL,
  user_ip NVARCHAR(255) NULL,
  user_agent NVARCHAR(255) NULL,
  is_flagged BIT NULL,
  reply_count INT NULL,
  mentions NVARCHAR(MAX) NULL,
  attachments NVARCHAR(MAX) NULL,
  metadata NVARCHAR(MAX) NULL,
  is_edited BIT NULL
);

CREATE TABLE dbo.tbl_country (
  country_name NVARCHAR(255) NOT NULL PRIMARY KEY,
  country_id INT NULL,
  continent NVARCHAR(255) NULL,
  region NVARCHAR(255) NULL,
  X FLOAT NULL,
  Y FLOAT NULL
);

CREATE TABLE dbo.tbl_followers (
  follower_id INT NOT NULL PRIMARY KEY,
  following_id INT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  follower_data NVARCHAR(MAX) NULL
);

CREATE TABLE dbo.tbl_labels (
  label_id INT NOT NULL PRIMARY KEY,
  creator_id INT NULL,
  updated_by INT NULL,
  name NVARCHAR(255) NULL,
  description NVARCHAR(MAX) NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  post_count INT NULL,
  visibility_status NVARCHAR(50) NULL,
  is_active BIT NULL,
  usage_frequency INT NULL,
  parent_label_id INT NULL
);

CREATE TABLE dbo.tbl_media (
  media_id INT NOT NULL PRIMARY KEY,
  post_id INT NULL,
  type NVARCHAR(255) NULL,
  url NVARCHAR(255) NULL,
  description NVARCHAR(MAX) NULL,
  upload_date DATETIME NULL,
  updated_at DATETIME NULL,
  filename NVARCHAR(255) NULL,
  file_size INT NULL,
  caption NVARCHAR(MAX) NULL,
  uploader_id INT NULL,
  visibility NVARCHAR(50) NULL,
  access_rights NVARCHAR(MAX) NULL,
  file_path NVARCHAR(255) NULL,
  metadata NVARCHAR(MAX) NULL,
  hash NVARCHAR(255) NULL,
  is_encrypted BIT NULL,
  encryption_key NVARCHAR(255) NULL,
  encryption_algorithm NVARCHAR(50) NULL,
  status_id INT NULL
);

CREATE TABLE dbo.tbl_notifications (
  notification_id INT NOT NULL PRIMARY KEY,
  user_id INT NULL,
  notification_type NVARCHAR(255) NULL,
  sender_id INT NULL,
  post_id INT NULL,
  comment_id INT NULL,
  notification_message NVARCHAR(MAX) NULL,
  notification_date DATETIME NULL,
  is_read BIT NULL,
  is_archived BIT NULL,
  is_deleted BIT NULL,
  link_to_notification NVARCHAR(255) NULL,
  notification_priority INT NULL,
  additional_data NVARCHAR(MAX) NULL,
  notification_subject NVARCHAR(255) NULL,
  notification_status NVARCHAR(50) NULL,
  notification_action NVARCHAR(50) NULL,
  related_user_id INT NULL,
  source_application NVARCHAR(255) NULL,
  notification_category NVARCHAR(255) NULL,
  expiration_date DATETIME NULL,
  delivery_method NVARCHAR(50) NULL,
  notification_channel NVARCHAR(50) NULL,
  notification_language NVARCHAR(50) NULL,
  recipient_group NVARCHAR(255) NULL,
  notification_tags NVARCHAR(MAX) NULL,
  notification_expiry_time DATETIME NULL,
  notification_context NVARCHAR(MAX) NULL,
  notification_origin NVARCHAR(255) NULL
);


CREATE TABLE dbo.tbl_posts (
  post_id INT NOT NULL PRIMARY KEY,
  user_id INT NULL,
  title NVARCHAR(255) NULL,
  content NVARCHAR(MAX) NULL,
  published_at DATETIME NULL,
  is_published BIT NULL,
  views_count INT NULL,
  likes_count INT NULL,
  shares_count INT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  slug NVARCHAR(255) NULL,
  excerpt NVARCHAR(MAX) NULL,
  status_id INT NULL,
  category_id INT NULL,
  dislikes_count INT NULL,
  comments_count INT NULL,
  is_featured BIT NULL,
  author_name NVARCHAR(255) NULL,
  image_url NVARCHAR(255) NULL,
  tags NVARCHAR(MAX) NULL,
  reading_time INT NULL,
  external_link NVARCHAR(255) NULL,
  metadata NVARCHAR(MAX) NULL
);

CREATE TABLE dbo.tbl_posts_tags (
  post_id INT NOT NULL,
  tag_id INT NOT NULL,
  comments NVARCHAR(MAX) NULL,
  label_status NVARCHAR(50) NULL,
  label_at DATETIME NULL

  PRIMARY KEY (post_id, tag_id),
);

CREATE TABLE dbo.tbl_private_messages (
  message_id INT NOT NULL PRIMARY KEY,
  sender_id INT NULL,
  receiver_id INT NULL,
  subject NVARCHAR(255) NULL,
  body NVARCHAR(MAX) NULL,
  sending_time DATETIME NULL,
  is_read BIT NULL,
  is_archived BIT NULL,
  is_deleted_by_sender BIT NULL,
  is_deleted_by_receiver BIT NULL,
  is_flagged BIT NULL,
  priority INT NULL,
  attachment_id INT NULL,
  conversation_id INT NULL,
  status NVARCHAR(50) NULL,
  parent_message_id INT NULL,
  read_timestamp DATETIME NULL,
  message_data NVARCHAR(MAX) NULL
);

CREATE TABLE dbo.tbl_reactions (
  reaction_id INT NOT NULL PRIMARY KEY,
  comment_id INT NULL,
  user_id INT NULL,
  post_id INT NULL,
  reaction_type NVARCHAR(255) NULL,
  reaction_date DATETIME NULL,
  reaction_score INT NULL,
  reaction_text NVARCHAR(MAX) NULL,
  is_active BIT NULL,
  reaction_count INT NULL,
  source_ip NVARCHAR(255) NULL,
  device_info NVARCHAR(MAX) NULL
);

CREATE TABLE dbo.tbl_reports (
  report_id INT NOT NULL PRIMARY KEY,
  user_id INT NULL,
  post_id INT NULL,
  report_reason NVARCHAR(255) NULL,
  report_timestamp DATETIME NULL,
  is_resolved BIT NULL,
  resolved_by INT NULL,
  resolution_timestamp DATETIME NULL,
  report_comments NVARCHAR(MAX) NULL,
  severity_level INT NULL,
  report_type NVARCHAR(50) NULL,
  report_entity_type NVARCHAR(50) NULL,
  report_entity_id INT NULL,
  action_taken NVARCHAR(MAX) NULL,
  action_timestamp DATETIME NULL,
  report_ip_address NVARCHAR(15) NULL,
  reported_user_id INT NULL,
  reported_comment_id INT NULL,
  reported_user_reputation INT NULL,
  report_category NVARCHAR(50) NULL,
  reported_context_text NVARCHAR(MAX) NULL,
  attachments NVARCHAR(MAX) NULL,
  flagged_user BIT NULL,
  report_source NVARCHAR(50) NULL,
  report_feedback NVARCHAR(MAX) NULL,
  report_verification_status NVARCHAR(50) NULL,
  report_mechanism NVARCHAR(50) NULL,
  report_priority INT NULL,
  assigned_to INT NULL,
  report_history NVARCHAR(MAX) NULL
);


CREATE TABLE dbo.tbl_status (
  status_id INT NOT NULL PRIMARY KEY,
  Name NVARCHAR(255) NULL
);

CREATE TABLE dbo.tbl_tags (
  tag_id INT NOT NULL PRIMARY KEY,
  creator_id INT NULL,
  category_id INT NULL,
  name NVARCHAR(255) NULL,
  description NVARCHAR(MAX) NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  slug NVARCHAR(255) NULL,
  count INT NULL,
  visibility BIT NULL,
  active BIT NULL,
  parent_tag INT NULL,
  status_id INT NULL,
  access_level INT NULL,
  is_approved BIT NULL,
  metadata NVARCHAR(MAX) NULL,
  is_edited BIT NULL
);

CREATE TABLE dbo.tbl_tags_hierarchy (
  tag_hierarchy_id INT NOT NULL PRIMARY KEY,
  tag_id INT NULL,
  relationship_type NVARCHAR(255) NULL,
  weight DECIMAL(10, 2) NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  active BIT NULL,
  description NVARCHAR(MAX) NULL,
  creator_id INT NULL,
  modifier_id INT NULL,
  is_deleted BIT NULL,
  deleted_at DATETIME NULL,
  revision_history NVARCHAR(MAX) NULL,
  comments NVARCHAR(MAX) NULL
);

CREATE TABLE dbo.tbl_user_activity (
  activity_id INT NOT NULL PRIMARY KEY,
  user_id INT NULL,
  activity_type NVARCHAR(255) NULL,
  activity_timestamp DATETIME NULL,
  post_id INT NULL,
  comment_id INT NULL,
  activity_description NVARCHAR(MAX) NULL,
  activity_source NVARCHAR(255) NULL,
  activity_location NVARCHAR(255) NULL,
  activity_duration INT NULL,
  activity_device NVARCHAR(255) NULL,
  activity_status NVARCHAR(50) NULL,
  activity_impact NVARCHAR(50) NULL,
  activity_privacy_level NVARCHAR(50) NULL,
  activity_related_users NVARCHAR(MAX) NULL,
  activiy_extra_data NVARCHAR(MAX) NULL,
  ip_address NVARCHAR(15) NULL,
  user_agent NVARCHAR(255) NULL,
  url NVARCHAR(255) NULL,
  session_id NVARCHAR(255) NULL,
  browser NVARCHAR(255) NULL,
  operating_system NVARCHAR(255) NULL,
  referrer NVARCHAR(255) NULL,
  event_id INT NULL,
  error_message NVARCHAR(MAX) NULL,
  location_coordinates NVARCHAR(MAX) NULL,
  session_duration INT NULL
);

CREATE TABLE dbo.tbl_user_roles (
  role_id INT NOT NULL PRIMARY KEY,
  created_by INT NULL,
  updated_by INT NULL,
  description NVARCHAR(255) NULL,
  created_at DATETIME DEFAULT GETDATE(),
  updated_at DATETIME DEFAULT GETDATE(),
  permissions NVARCHAR(MAX) NULL,
  role_name NVARCHAR(50) NULL,
  is_active BIT NULL,
  restrictions NVARCHAR(MAX) NULL,
  expiry_date DATE NULL,
  created_by_ip NVARCHAR(50) NULL,
  updated_by_ip NVARCHAR(50) NULL,
  created_by_device NVARCHAR(100) NULL,
  updated_by_device NVARCHAR(100) NULL,
  active_sessions INT NULL,
  last_sessions NVARCHAR(MAX) NULL
);

CREATE TABLE dbo.tbl_users (
  user_id INT NOT NULL PRIMARY KEY,
  first_name NVARCHAR(255) NULL,
  last_name NVARCHAR(255) NULL,
  username NVARCHAR(255) NULL,
  password_hash NVARCHAR(255) NULL,
  social_media_links NVARCHAR(MAX) NULL,
  contact_info NVARCHAR(MAX) NULL,
  avatar_url NVARCHAR(255) NULL,
  bio NVARCHAR(MAX) NULL,
  website NVARCHAR(255) NULL,
  created_at DATETIME DEFAULT GETDATE(),
  updated_at DATETIME DEFAULT GETDATE(),
  notification_preferences NVARCHAR(255) NULL,
  role_id INT NULL,
  last_login DATETIME NULL,
  active BIT NULL
);

