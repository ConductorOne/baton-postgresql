CREATE TABLE test_table
(
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_table_log
(
    log_id        SERIAL PRIMARY KEY,
    test_table_id INTEGER     NOT NULL,
    change_type   VARCHAR(10) NOT NULL,
    changed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (test_table_id) REFERENCES test_table (id)
);

-- Insert some sample data
INSERT INTO test_table (name)
VALUES ('Test Item 1'),
       ('Test Item 2'),
       ('Test Item 3');

-- Create an index for testing
CREATE INDEX idx_test_table_name ON test_table (name);

-- Create a view for testing
CREATE VIEW test_table_view AS
SELECT id, name, created_at
FROM test_table;

-- Create a function for testing
CREATE OR REPLACE FUNCTION get_test_item_count()
    RETURNS INTEGER AS
$$
BEGIN
    RETURN (SELECT COUNT(*) FROM test_table);
END;
$$ LANGUAGE plpgsql;

-- Create a trigger function for testing
CREATE OR REPLACE FUNCTION log_test_table_changes()
    RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO test_table_log (test_table_id, change_type, changed_at)
    VALUES (NEW.id, TG_OP, CURRENT_TIMESTAMP);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a sequence for testing
CREATE SEQUENCE test_table_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Create procedures for testing
CREATE OR REPLACE PROCEDURE add_test_item(item_name VARCHAR)
    LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO test_table (name) VALUES (item_name);
END;
$$;

-- create roles and users

CREATE ROLE test_role WITH LOGIN PASSWORD 'test_password';
CREATE USER test_user WITH PASSWORD 'test_password';
GRANT test_role TO test_user;

