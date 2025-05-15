-- Drop tables in reverse order of dependency to avoid FK errors during cleanup (if run manually)
DROP TABLE IF EXISTS `product_variants`;
DROP TABLE IF EXISTS `products`;
DROP TABLE IF EXISTS `comments`;
DROP TABLE IF EXISTS `post_categories`;
DROP TABLE IF EXISTS `posts`;
DROP TABLE IF EXISTS `categories`;
DROP TABLE IF EXISTS `users`;

-- users table
CREATE TABLE `users` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `username` VARCHAR(50) NOT NULL UNIQUE,
    `email` VARCHAR(100) NOT NULL UNIQUE,
    `full_name` VARCHAR(100) NULL,
    `bio` TEXT,
    `age` TINYINT UNSIGNED NULL, -- More appropriate than plain TINYINT for age
    `salary` DECIMAL(10, 2) DEFAULT 0.00,
    `is_active` TINYINT(1) DEFAULT 1, -- Common way to represent boolean in MySQL
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `json_data` JSON NULL,
    `binary_data` BLOB NULL,
    `user_status` ENUM('active', 'pending', 'banned') DEFAULT 'active',
    `registration_date` DATE NULL,
    `last_login_time` TIME NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- categories table
CREATE TABLE `categories` (
    `category_id` INT AUTO_INCREMENT PRIMARY KEY,
    `name` VARCHAR(50) NOT NULL UNIQUE,
    `description` TEXT,
    `parent_cat_id` INT NULL,
    CONSTRAINT `fk_category_parent` FOREIGN KEY (`parent_cat_id`) REFERENCES `categories` (`category_id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- posts table
CREATE TABLE `posts` (
    `post_id` INT AUTO_INCREMENT PRIMARY KEY,
    `author_id` INT NOT NULL,
    `title` VARCHAR(200) NOT NULL,
    `content` LONGTEXT, -- Changed from TEXT to LONGTEXT for larger content
    `slug` VARCHAR(100) NOT NULL UNIQUE,
    `published_at` DATETIME NULL,
    `views` INT DEFAULT 0,
    `average_rating` FLOAT DEFAULT 0.0,
    CONSTRAINT `fk_post_author` FOREIGN KEY (`author_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- comments table
CREATE TABLE `comments` (
    `comment_id` INT AUTO_INCREMENT PRIMARY KEY,
    `post_id` INT NOT NULL,
    `user_id` INT NOT NULL,
    `parent_comment_id` INT NULL,
    `comment_text` TEXT NOT NULL,
    `commented_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT `fk_comment_post` FOREIGN KEY (`post_id`) REFERENCES `posts` (`post_id`) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_comment_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT, -- Changed to RESTRICT to test this action
    CONSTRAINT `fk_comment_parent` FOREIGN KEY (`parent_comment_id`) REFERENCES `comments` (`comment_id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- post_categories (junction table)
CREATE TABLE `post_categories` (
    `post_id` INT NOT NULL,
    `category_id` INT NOT NULL,
    PRIMARY KEY (`post_id`, `category_id`),
    CONSTRAINT `fk_pc_post` FOREIGN KEY (`post_id`) REFERENCES `posts` (`post_id`) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_pc_category` FOREIGN KEY (`category_id`) REFERENCES `categories` (`category_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- products table
CREATE TABLE `products` (
    `product_id` INT AUTO_INCREMENT PRIMARY KEY,
    `product_name` VARCHAR(255) NOT NULL,
    `description` TEXT,
    `price` DECIMAL(10, 2) NOT NULL,
    `created_date` DATE DEFAULT (CURDATE()) -- MySQL 8+ syntax for default expression
    -- For older MySQL, you might need a trigger or set it programmatically if default CURDATE() is not supported directly on DATE
    -- Or make it DATETIME DEFAULT CURRENT_TIMESTAMP and extract DATE part later
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- product_variants table
CREATE TABLE `product_variants` (
    `variant_id` INT AUTO_INCREMENT PRIMARY KEY,
    `product_id` INT NOT NULL,
    `sku` VARCHAR(100) NOT NULL UNIQUE,
    `attributes` JSON, -- e.g., {"color": "red", "size": "M"}
    `stock_qty` INT NOT NULL DEFAULT 0,
    CONSTRAINT `fk_variant_product` FOREIGN KEY (`product_id`) REFERENCES `products` (`product_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indexes (contoh)
CREATE INDEX `idx_posts_published_at` ON `posts` (`published_at` DESC);
CREATE INDEX `idx_users_age` ON `users` (`age`);


-- Sample Data (ensuring referential integrity)

INSERT INTO `users` (`username`, `email`, `full_name`, `bio`, `age`, `salary`, `is_active`, `user_status`, `registration_date`, `last_login_time`, `json_data`, `binary_data`) VALUES
('johndoe', 'john.doe@example.com', 'John Doe', 'A software developer.', 30, 75000.50, 1, 'active', '2023-01-15', '10:30:00', '{"theme": "dark", "notifications": true}', UNHEX(HEX('Sample binary data for John'))),
('janedoe', 'jane.doe@example.com', 'Jane Doe', NULL, 28, 82000.00, 1, 'active', '2022-11-20', '11:00:00', '{"beta_user": false}', NULL),
('bobsmith', 'bob.smith@example.com', 'Bob Smith', 'Loves hiking.', 35, 60000.75, 0, 'pending', '2023-05-10', '09:15:45', NULL, UNHEX(HEX('Another binary blob'))),
('testuser_inactive', 'inactive@example.com', 'Inactive User', 'Bio here', 40, 50000.00, 0, 'banned', '2021-01-01', '08:00:00', NULL, NULL); -- User for FK testing (e.g., if a comment had this user_id and user was deleted)

INSERT INTO `categories` (`name`, `description`, `parent_cat_id`) VALUES
('Technology', 'All about tech.', NULL),
('Programming', 'Coding and development.', 1),
('Databases', 'Database systems and management.', 2);

INSERT INTO `posts` (`author_id`, `title`, `content`, `slug`, `published_at`, `views`, `average_rating`) VALUES
(1, 'First Post about Go', 'Content of the first post about Go programming.', 'first-post-go', '2023-06-01 10:00:00', 150, 4.5),
(2, 'Database Normalization Explained', 'Detailed explanation of database normalization forms.', 'db-normalization', '2023-06-15 14:30:00', 250, 4.8),
(1, 'Advanced Go Concurrency', 'Exploring advanced concurrency patterns in Go.', 'advanced-go-concurrency', NULL, 50, 4.2), -- Unpublished
(3, 'Introduction to SQL', 'A beginner guide to SQL.', 'intro-sql', '2023-07-01 09:00:00', 300, 4.0);

INSERT INTO `comments` (`post_id`, `user_id`, `parent_comment_id`, `comment_text`) VALUES
(1, 2, NULL, 'Great post on Go!'),
(1, 3, 1, 'Thanks Jane, I agree.'),
(2, 1, NULL, 'Very clear explanation of normalization.');
-- Add a comment that might cause an FK issue if user 4 is deleted without proper ON DELETE handling on the FK
-- INSERT INTO `comments` (`post_id`, `user_id`, `comment_text`) VALUES (3, 4, 'Test comment from inactive user');
-- For now, let's keep data consistent. If you want to test FK violations, add data like above.

INSERT INTO `post_categories` (`post_id`, `category_id`) VALUES
(1, 2), -- First Post (Go) -> Programming
(2, 3), -- DB Normalization -> Databases
(3, 2), -- Advanced Go -> Programming
(4, 3), -- Intro SQL -> Databases
(1, 1); -- First Post (Go) -> Technology (multi-category)

INSERT INTO `products` (`product_name`, `description`, `price`, `created_date`) VALUES
('Laptop Pro X', 'High-performance laptop for professionals', 1299.99, '2024-01-10'),
('Wireless Mouse Ergo', 'Ergonomic wireless mouse', 49.50, '2024-02-15');

INSERT INTO `product_variants` (`product_id`, `sku`, `attributes`, `stock_qty`) VALUES
(1, 'LPX-16GB-512SSD', '{"ram": "16GB", "storage": "512GB SSD", "color": "Silver"}', 50),
(1, 'LPX-32GB-1TBSSD', '{"ram": "32GB", "storage": "1TB SSD", "color": "Space Gray"}', 25),
(2, 'WME-BLK', '{"color": "Black"}', 200);
