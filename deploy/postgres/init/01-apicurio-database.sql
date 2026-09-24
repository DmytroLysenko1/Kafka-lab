-- Runs only when the postgres volume is created for the first time. The registry keeps its
-- schemas in a database of its own so that a `TRUNCATE payments, outbox` in an integration
-- test cannot take the schema history with it.
CREATE DATABASE apicurio;
