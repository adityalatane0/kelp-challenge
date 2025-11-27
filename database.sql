-- Table creation script
CREATE TABLE IF NOT EXISTS public.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL, -- name = firstName + lastName
    age INT NOT NULL,
    address JSONB,
    additional_info JSONB
);
