-- Add unique constraint to broker name
ALTER TABLE broker ADD CONSTRAINT broker_name_unique UNIQUE (name); 