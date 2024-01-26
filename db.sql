-- Connect to the default database (e.g., postgres) to create new database.
CREATE DATABASE weather_app;

-- Now, connect to newly created database and create the table.
\c weather_app

-- Create new user
CREATE USER weather_app_user WITH ENCRYPTED PASSWORD 'weather_app_password';

-- Grant privileges to the new user
GRANT ALL PRIVILEGES ON DATABASE weather_app TO weather_app_user;

-- Create the 'weatherdata' table
CREATE TABLE weatherdata (
    cityname VARCHAR(255) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    air_pressure REAL,
    humidity INTEGER CHECK (humidity >= 0 AND humidity <= 100), -- percentage
    temperature REAL,
    PRIMARY KEY (cityname, timestamp)
);

-- Grant privileges to the new user
GRANT ALL PRIVILEGES ON TABLE weatherdata TO weather_app_user;
