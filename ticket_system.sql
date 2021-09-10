CREATE DATABASE ticket_system;

USE ticket_system;

CREATE TABLE sales (
	ticket_id INT,
    trans_date DATE,
    event_id INT,
    event_name VARCHAR(50),
    event_date DATE,
    event_type VARCHAR(10),
    event_city VARCHAR(20),
    customer_id INT,
    price DECIMAL,
    num_tickets INT
);