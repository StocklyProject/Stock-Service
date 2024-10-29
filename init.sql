CREATE DATABASE IF NOT EXISTS stockly;
USE stockly;

CREATE TABLE IF NOT EXISTS stock (
    id INT AUTO_INCREMENT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted TINYINT DEFAULT 0,
    symbol VARCHAR(100),
    high INT,
    low INT,
    volume INT,
    date DATETIME,
    open INT,
    close INT,
    rate DOUBLE,
    rate_price INT
);