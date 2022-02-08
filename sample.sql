-- Adding primary_key, non_null, uniqueness ddl statements 

-- Processing table sample_table_1

ALTER TABLE test_schema.sample_table_1 ADD PRIMARY KEY (arg_1);
ALTER TABLE test_schema.sample_table_1 ALTER COLUMN arg_1 SET NOT NULL;
ALTER TABLE test_schema.sample_table_1 ALTER COLUMN arg_2 SET NOT NULL;
ALTER TABLE test_schema.sample_table_1 ALTER COLUMN arg_3 SET NOT NULL;
ALTER TABLE test_schema.sample_table_1 ADD CONSTRAINT unique_test_schema_sample_table_1_arg_1 UNIQUE (arg_1);


-- Processing table sample_table_2

ALTER TABLE test_schema.sample_table_2 ADD PRIMARY KEY (arg_1);
ALTER TABLE test_schema.sample_table_2 ALTER COLUMN arg_1 SET NOT NULL;
ALTER TABLE test_schema.sample_table_2 ADD CONSTRAINT unique_test_schema_sample_table_2_arg_1 UNIQUE (arg_1);


-- Processing table sample_table_3

ALTER TABLE test_schema.sample_table_3 ADD PRIMARY KEY (arg_1,arg_2);


-- Adding foreign key ddl statements 

-- Processing table sample_table_1

ALTER TABLE test_schema.sample_table_1 ADD CONSTRAINT fk_test_schema_sample_table_1_arg_3_sample_table_2_arg_1 FOREIGN KEY arg_3 REFERENCES test_schema.sample_table_2 (arg_1);


