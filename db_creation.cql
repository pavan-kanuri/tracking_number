CREATE KEYSPACE tracking WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE tracking.tracking_numbers (
    id UUID PRIMARY KEY,
    tracking_number text
);