-- Drop Database If Exists (Ensures a Fresh Start)
DROP DATABASE IF EXISTS mydatabase;

-- Create New Database
CREATE DATABASE mydatabase;

-- Connect to the New Database
\c mydatabase;

-- Drop Tables If They Exist
DROP TABLE IF EXISTS tb_transaction;
DROP TABLE IF EXISTS tb_address;
DROP TABLE IF EXISTS tb_client;

-- Create `tb_client`
CREATE TABLE tb_client (
    id SERIAL PRIMARY KEY,
    runtime_id VARCHAR(50) UNIQUE NOT NULL,
    names VARCHAR(255) NOT NULL,
    mail VARCHAR(255) UNIQUE NOT NULL,
    account_created_at TIMESTAMP NOT NULL
);

-- Create `tb_address`
CREATE TABLE tb_address (
    id SERIAL PRIMARY KEY,
    runtime_id VARCHAR(50) UNIQUE NOT NULL,
    street VARCHAR(255),
    city VARCHAR(100),
    post_code TEXT,
    country VARCHAR(100),
    FOREIGN KEY (runtime_id) REFERENCES tb_client(runtime_id) ON DELETE CASCADE
);

-- Create `tb_transaction`
CREATE TABLE tb_transaction (
    id SERIAL PRIMARY KEY,
    runtime_id VARCHAR(50) NOT NULL,
    client_from VARCHAR(50) NULL,
    client_to VARCHAR(50) NULL,
    amount VARCHAR(10) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    transaction_type VARCHAR(50) NULL,
    FOREIGN KEY (runtime_id) REFERENCES tb_client(runtime_id) ON DELETE CASCADE
);

