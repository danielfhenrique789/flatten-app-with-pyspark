-- Step 1: Drop Database If Exists (Ensures a Fresh Start)
DROP DATABASE IF EXISTS mydatabase;

-- Step 2: Create New Database
CREATE DATABASE mydatabase;

-- Step 3: Connect to the New Database
\c mydatabase;

-- Step 4: Drop Tables If They Exist
DROP TABLE IF EXISTS tb_transaction;
DROP TABLE IF EXISTS tb_address;
DROP TABLE IF EXISTS tb_client;

-- Step 5: Create `tb_client`
CREATE TABLE tb_client (
    id SERIAL PRIMARY KEY,
    runtime_id VARCHAR(50) UNIQUE NOT NULL,
    names VARCHAR(255) NOT NULL,
    mail VARCHAR(255) UNIQUE NOT NULL,
    account_created_at TIMESTAMP NOT NULL
);

-- Step 6: Create `tb_address`
CREATE TABLE tb_address (
    id SERIAL PRIMARY KEY,
    runtime_id VARCHAR(50) UNIQUE NOT NULL,
    street VARCHAR(255),
    city VARCHAR(100),
    post_code TEXT,
    country VARCHAR(100),
    FOREIGN KEY (runtime_id) REFERENCES tb_client(runtime_id) ON DELETE CASCADE
);

-- Step 7: Create `tb_transaction`
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

