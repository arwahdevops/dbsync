-- Tabel Pengguna
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    full_name VARCHAR(100),
    bio TEXT,
    age TINYINT UNSIGNED NULL,
    salary DECIMAL(10, 2) DEFAULT 50000.00,
    is_active BOOLEAN DEFAULT TRUE, -- MySQL akan mengubah BOOLEAN menjadi TINYINT(1)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Tabel Postingan
CREATE TABLE posts (
    post_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    author_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    content MEDIUMTEXT,
    slug VARCHAR(200) NOT NULL UNIQUE,
    views INT DEFAULT 0,
    published_at DATETIME NULL,
    CONSTRAINT fk_post_author FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE -- Jika user dihapus, postingannya juga
);

-- Tabel Komentar
CREATE TABLE comments (
    comment_id INT AUTO_INCREMENT PRIMARY KEY,
    post_id BIGINT NOT NULL,
    commenter_id INT NULL, -- Komentar bisa anonim
    comment_text VARCHAR(1000) NOT NULL,
    approved BOOLEAN DEFAULT FALSE,
    created_on DATE,
    parent_comment_id INT NULL,
    CONSTRAINT fk_comment_post FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
    CONSTRAINT fk_comment_commenter FOREIGN KEY (commenter_id) REFERENCES users(id) ON DELETE SET NULL, -- Jika user dihapus, komentarnya jadi anonim
    CONSTRAINT fk_comment_parent FOREIGN KEY (parent_comment_id) REFERENCES comments(comment_id) ON DELETE SET NULL -- Self-referencing FK
);

-- Tabel Kategori
CREATE TABLE categories (
    category_id SMALLINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255)
);

-- Tabel Pivot untuk Postingan dan Kategori (Many-to-Many)
CREATE TABLE post_categories (
    pc_id INT AUTO_INCREMENT PRIMARY KEY,
    post_id BIGINT NOT NULL,
    category_id SMALLINT NOT NULL,
    CONSTRAINT fk_pc_post FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
    CONSTRAINT fk_pc_category FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE CASCADE,
    UNIQUE KEY uk_post_category (post_id, category_id) -- Pastikan kombinasi post dan kategori unik
);


-- Data Sampel --

INSERT INTO users (username, email, full_name, bio, age, salary, is_active) VALUES
('john_doe', 'john.doe@example.com', 'John Doe', 'Software developer and avid blogger.', 30, 75000.50, TRUE),
('jane_smith', 'jane.smith@example.com', 'Jane Smith', 'Tech enthusiast and content creator.', 28, 68000.00, TRUE),
('guest_user', 'guest@example.com', 'Guest User', NULL, NULL, 20000.00, FALSE);

INSERT INTO posts (author_id, title, content, slug, views, published_at) VALUES
(1, 'My First Blog Post', 'This is the content of my first post!', 'my-first-blog-post', 150, '2023-01-10 10:00:00'),
(1, 'Advanced Go Programming', 'Exploring advanced concepts in Go.', 'advanced-go-programming', 275, '2023-02-15 14:30:00'),
(2, 'Introduction to Databases', 'A beginner-friendly guide to databases.', 'intro-to-databases', 120, '2023-03-01 09:00:00'),
(1, 'Unpublished Draft', 'This post is not yet published.', 'unpublished-draft', 0, NULL);

INSERT INTO comments (post_id, commenter_id, comment_text, approved, created_on, parent_comment_id) VALUES
(1, 2, 'Great first post, John!', TRUE, '2023-01-10', NULL),
(1, 1, 'Thanks, Jane!', TRUE, '2023-01-11', 1), -- Balasan untuk komentar pertama
(2, NULL, 'Interesting topic, looking forward to more.', FALSE, '2023-02-16', NULL); -- Komentar anonim, belum disetujui

INSERT INTO categories (name, description) VALUES
('Technology', 'Articles related to technology and software.'),
('Programming', 'Tutorials and discussions about programming languages.'),
('Databases', 'Everything about database systems.');

INSERT INTO post_categories (post_id, category_id) VALUES
(1, 1), -- My First Blog Post -> Technology
(2, 1), -- Advanced Go Programming -> Technology
(2, 2), -- Advanced Go Programming -> Programming
(3, 3); -- Introduction to Databases -> Databases
