ALTER TABLE dbo.tbl_categories_hierarchy
ADD CONSTRAINT fk_category_id FOREIGN KEY(category_id) REFERENCES dbo.tbl_categories(category_id);

ALTER TABLE dbo.tbl_categories
ADD CONSTRAINT fk_created_by_categories FOREIGN KEY(created_by) REFERENCES dbo.tbl_users(user_id);

ALTER TABLE dbo.tbl_categories_hierarchy
ADD CONSTRAINT fk_created_by_categories_hierarchy FOREIGN KEY(created_by) REFERENCES dbo.tbl_users(user_id);

ALTER TABLE dbo.tbl_user_roles
ADD CONSTRAINT fk_created_by_user_roles FOREIGN KEY(created_by) REFERENCES dbo.tbl_users(user_id);

ALTER TABLE dbo.tbl_attachments
ADD CONSTRAINT fk_parent_attachment_id FOREIGN KEY(parent_attachment_id) REFERENCES dbo.tbl_attachments(attachment_id);

ALTER TABLE dbo.tbl_attachments
ADD CONSTRAINT fk_post_id_attachments FOREIGN KEY(post_id) REFERENCES dbo.tbl_posts(post_id);

ALTER TABLE dbo.tbl_comments
ADD CONSTRAINT fk_post_id_comments FOREIGN KEY(post_id) REFERENCES dbo.tbl_posts(post_id);

ALTER TABLE dbo.tbl_notifications
ADD CONSTRAINT fk_post_id_notifications FOREIGN KEY (post_id) REFERENCES dbo.tbl_posts(post_id);

ALTER TABLE dbo.tbl_posts_tags
ADD CONSTRAINT fk_post_id_posts_tags FOREIGN KEY (post_id) REFERENCES dbo.tbl_posts (post_id);

ALTER TABLE dbo.tbl_notifications
ADD CONSTRAINT fk_sender_id FOREIGN KEY (sender_id) REFERENCES dbo.tbl_users (user_id);

ALTER TABLE dbo.tbl_posts_tags
ADD CONSTRAINT fk_tag_id FOREIGN KEY (tag_id) REFERENCES dbo.tbl_tags (tag_id);

ALTER TABLE dbo.tbl_categories
ADD CONSTRAINT fk_updated_by_categories FOREIGN KEY (updated_by) REFERENCES dbo.tbl_users (user_id);

ALTER TABLE dbo.tbl_categories_hierarchy
ADD CONSTRAINT fk_updated_by_categories_hierarchy FOREIGN KEY (updated_by) REFERENCES dbo.tbl_users (user_id);

ALTER TABLE dbo.tbl_user_roles
ADD CONSTRAINT fk_updated_by_user_roles FOREIGN KEY (updated_by) REFERENCES dbo.tbl_users (user_id);

ALTER TABLE dbo.tbl_attachments
ADD CONSTRAINT fk_user_id_attachments FOREIGN KEY (user_id) REFERENCES dbo.tbl_users (user_id);

ALTER TABLE dbo.tbl_comments
ADD CONSTRAINT fk_user_id_comments FOREIGN KEY (user_id) REFERENCES dbo.tbl_users (user_id);

ALTER TABLE dbo.tbl_notifications
ADD CONSTRAINT fk_user_id_notifications FOREIGN KEY (user_id) REFERENCES dbo.tbl_users (user_id);

ALTER TABLE dbo.tbl_posts
ADD CONSTRAINT fk_user_id_posts FOREIGN KEY (user_id) REFERENCES dbo.tbl_users (user_id);

ALTER TABLE dbo.tbl_users
ADD CONSTRAINT fk_user_role_id FOREIGN KEY (role_id) REFERENCES dbo.tbl_user_roles (role_id);







