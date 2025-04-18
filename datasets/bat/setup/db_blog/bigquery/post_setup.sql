ALTER TABLE {{dataset}}.tbl_categories_hierarchy
ADD FOREIGN KEY(category_id) REFERENCES {{dataset}}.tbl_categories(category_id) NOT ENFORCED;

ALTER TABLE {{dataset}}.tbl_categories
ADD FOREIGN KEY(created_by) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE {{dataset}}.tbl_categories_hierarchy
ADD FOREIGN KEY(created_by) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE {{dataset}}.tbl_user_roles
ADD FOREIGN KEY(created_by) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_attachments
ADD FOREIGN KEY(post_id) REFERENCES {{dataset}}.tbl_posts(post_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_comments
ADD FOREIGN KEY(post_id) REFERENCES {{dataset}}.tbl_posts(post_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_notifications
ADD FOREIGN KEY(post_id) REFERENCES {{dataset}}.tbl_posts(post_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_posts_tags
ADD FOREIGN KEY(post_id) REFERENCES {{dataset}}.tbl_posts(post_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_notifications
ADD FOREIGN KEY(sender_id) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_posts_tags
ADD FOREIGN KEY(tag_id) REFERENCES {{dataset}}.tbl_tags(tag_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_categories
ADD FOREIGN KEY(updated_by) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_categories_hierarchy
ADD FOREIGN KEY(updated_by) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_user_roles
ADD FOREIGN KEY(updated_by) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_attachments
ADD FOREIGN KEY(user_id) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_comments
ADD FOREIGN KEY(user_id) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_notifications
ADD FOREIGN KEY(user_id) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_posts
ADD FOREIGN KEY(user_id) REFERENCES {{dataset}}.tbl_users(user_id) NOT ENFORCED;

ALTER TABLE  {{dataset}}.tbl_users
ADD FOREIGN KEY(role_id) REFERENCES {{dataset}}.tbl_user_roles(role_id) NOT ENFORCED;
