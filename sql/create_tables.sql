-- Create the fuel_exports table to store the ETL data
CREATE TABLE IF NOT EXISTS fuel_exports (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    station_id INTEGER NOT NULL,
    dock_bay SMALLINT,
    dock_level VARCHAR(10),
    ship_name VARCHAR(100),
    franchise VARCHAR(50),
    captain_name VARCHAR(100),
    species VARCHAR(50),
    fuel_type VARCHAR(50),
    fuel_units REAL,
    price_per_unit DECIMAL(8,2),
    total_cost DECIMAL(12,2),
    services TEXT, -- Comma-separated list of services
    is_emergency BOOLEAN,
    visited_at TIMESTAMP WITH TIME ZONE,
    arrival_date DATE,
    coords_x DOUBLE PRECISION,
    coords_y DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create the processed_files table to track which files have been processed
CREATE TABLE IF NOT EXISTS processed_files (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) UNIQUE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_fuel_exports_transaction_id ON fuel_exports(transaction_id);
CREATE INDEX IF NOT EXISTS idx_fuel_exports_station_id ON fuel_exports(station_id);
CREATE INDEX IF NOT EXISTS idx_fuel_exports_visited_at ON fuel_exports(visited_at);
CREATE INDEX IF NOT EXISTS idx_fuel_exports_fuel_type ON fuel_exports(fuel_type);
CREATE INDEX IF NOT EXISTS idx_fuel_exports_franchise ON fuel_exports(franchise);

CREATE INDEX IF NOT EXISTS idx_processed_files_filename ON processed_files(filename);
CREATE INDEX IF NOT EXISTS idx_processed_files_processed_at ON processed_files(processed_at);