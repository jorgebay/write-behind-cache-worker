CREATE TABLE sample_table (
    id BIGINT PRIMARY KEY,
    partition_key BIGINT NOT NULL
);

INSERT INTO sample_table (id, partition_key) VALUES (3, 2000);
INSERT INTO sample_table (id, partition_key) VALUES (1, 1000);
INSERT INTO sample_table (id, partition_key) VALUES (2, 1000);


CREATE TABLE uuid_table (
    id UUID PRIMARY KEY,
    partition_key UUID NOT NULL
);

-- UUID v7 on the ids for ordering
INSERT INTO uuid_table (id, partition_key) VALUES ('01926cc6-6430-7359-8ba1-02f348b55d36', '8afb5e31-d8a6-4d92-b964-6ad8cc296050');
INSERT INTO uuid_table (id, partition_key) VALUES ('01926cc4-cece-72d3-b801-abcb74b68556', '65e6690c-80a6-4c76-95c7-2bbb686e4074');
INSERT INTO uuid_table (id, partition_key) VALUES ('01926cc4-0783-76d8-ab15-d8ac61b6372e', '65e6690c-80a6-4c76-95c7-2bbb686e4074');
