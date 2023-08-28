-- to be run in Snowflake worksheets to create the tables for accepting data
-- tables are not created automatically by Airflow to keep control over the incoming data
-- and to throw an error if the data does not match the expected schema

create or replace table showcase.raw.device (
	id int,
	type smallint,
	store_id int
);

create or replace table showcase.raw.store (
	id int,
	name varchar(255),
	address varchar(255),
	city varchar(255),
	country varchar(255),
	created_at timestamp_ntz(9),
	typology varchar(255),
	customer_id int
);

create or replace table showcase.raw.transaction (
	id int,
	device_id int,
	product_name varchar(255),
	product_sku varchar(255),
	category_name varchar(255),
	amount int,
	status varchar(255),
	card_number varchar(255),
	cvv int,
	created_at timestamp_ntz(9),
	happened_at timestamp_ntz(9)
);