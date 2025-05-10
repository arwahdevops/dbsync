-- Drop tables if they exist (order matters due to foreign keys)
DROP TABLE IF EXISTS `comments`;
DROP TABLE IF EXISTS `post_categories`;
DROP TABLE IF EXISTS `posts`;
DROP TABLE IF EXISTS `categories`;
DROP TABLE IF EXISTS `users`;

-- Tabel users
CREATE TABLE `users` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `username` VARCHAR(50) NOT NULL,
    `email` VARCHAR(100) NOT NULL,
    `full_name` VARCHAR(100) DEFAULT NULL,
    `bio` TEXT,
    `age` TINYINT UNSIGNED DEFAULT NULL, -- Akan dipetakan ke SMALLINT di PG
    `salary` DECIMAL(10, 2) DEFAULT NULL,
    `is_active` BOOLEAN DEFAULT TRUE, -- Di MySQL ini akan jadi TINYINT(1)
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_username` (`username`),
    UNIQUE KEY `uq_email` (`email`),
    INDEX `idx_age` (`age`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabel categories
CREATE TABLE `categories` (
    `category_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(50) NOT NULL,
    `description` TEXT,
    PRIMARY KEY (`category_id`),
    UNIQUE KEY `uq_category_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabel posts
CREATE TABLE `posts` (
    `post_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `author_id` INT UNSIGNED NOT NULL,
    `title` VARCHAR(255) NOT NULL,
    `content` LONGTEXT,
    `slug` VARCHAR(255) NOT NULL,
    `views` INT UNSIGNED DEFAULT 0,
    `published_at` DATETIME DEFAULT NULL,
    PRIMARY KEY (`post_id`),
    UNIQUE KEY `uq_post_slug` (`slug`),
    INDEX `idx_author_id` (`author_id`),
    INDEX `idx_published_at` (`published_at`),
    CONSTRAINT `fk_post_author` FOREIGN KEY (`author_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabel comments
CREATE TABLE `comments` (
    `comment_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `post_id` BIGINT UNSIGNED NOT NULL,
    `user_id` INT UNSIGNED NOT NULL,
    `parent_comment_id` BIGINT UNSIGNED DEFAULT NULL,
    `content` TEXT NOT NULL,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`comment_id`),
    INDEX `idx_comment_post_id` (`post_id`),
    INDEX `idx_comment_user_id` (`user_id`),
    CONSTRAINT `fk_comment_post` FOREIGN KEY (`post_id`) REFERENCES `posts` (`post_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_comment_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_comment_parent` FOREIGN KEY (`parent_comment_id`) REFERENCES `comments` (`comment_id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabel pivot post_categories (Many-to-Many)
CREATE TABLE `post_categories` (
    `post_id` BIGINT UNSIGNED NOT NULL,
    `category_id` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`post_id`, `category_id`),
    CONSTRAINT `fk_pc_post` FOREIGN KEY (`post_id`) REFERENCES `posts` (`post_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_pc_category` FOREIGN KEY (`category_id`) REFERENCES `categories` (`category_id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- Data untuk tabel users
INSERT INTO `users` (`id`, `username`, `email`, `full_name`, `bio`, `age`, `salary`, `is_active`, `created_at`, `updated_at`) VALUES
(1, 'john_doe', 'john.doe@example.com', 'John Doe', 'Bio for John Doe', 30, 60000.00, TRUE, '2024-01-15 10:00:00', '2024-01-15 10:00:00'),
(2, 'jane_smith', 'jane.smith@example.com', 'Jane Smith', NULL, 28, 75000.50, TRUE, '2024-01-16 11:30:00', '2024-01-16 11:30:00'),
(3, 'bob_johnson', 'bob.johnson@example.com', 'Bob Johnson', 'A friendly guy.', 35, 50000.75, FALSE, '2024-01-17 14:15:00', '2024-01-18 09:00:00');

-- Data untuk tabel categories
INSERT INTO `categories` (`category_id`, `name`, `description`) VALUES
(1, 'Technology', 'Articles related to technology and software.'),
(2, 'Science', 'Discoveries and research in science.'),
(3, 'Travel', 'Guides and stories about travel destinations.');

-- Data untuk tabel posts
INSERT INTO `posts` (`post_id`, `author_id`, `title`, `content`, `slug`, `views`, `published_at`) VALUES
(1, 1, 'First Post by John', 'Content of the first post.', 'first-post-john', 100, '2024-01-20 08:00:00'),
(2, 2, 'Jane Shares Insights', 'Some insightful content here.', 'jane-shares-insights', 250, '2024-01-21 10:00:00'),
(3, 1, 'Johns Second Article', 'More thoughts from John.', 'johns-second-article', 150, '2024-01-22 14:30:00'),
(4, 2, 'A Valid Post by Jane', 'This post now has a valid author.', 'a-valid-post-by-jane', 10, '2024-01-23 16:00:00');

-- Data untuk tabel comments
INSERT INTO `comments` (`comment_id`, `post_id`, `user_id`, `parent_comment_id`, `content`, `created_at`) VALUES
(1, 1, 2, NULL, 'Great first post, John!', '2024-01-20 09:00:00'),
(2, 1, 3, 1, 'I agree with Jane!', '2024-01-20 09:30:00'),
(3, 2, 1, NULL, 'Thanks for sharing, Jane.', '2024-01-21 11:00:00');

-- Data untuk tabel post_categories
INSERT INTO `post_categories` (`post_id`, `category_id`) VALUES
(1, 1),
(1, 2),
(2, 1),
(3, 3),
(4, 2);
