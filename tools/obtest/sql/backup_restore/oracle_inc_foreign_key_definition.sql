
ALTER TABLE base_table_foreign_key_2 DROP CONSTRAINT base_fk2_fk_c4_c3;

CREATE TABLE inc_table_foreign_key_1 (
     pk    NUMBER primary key,
     c1    INT,
     c2    NUMBER,
     c3    VARCHAR2(100),
     c4    CHAR(100)
);
CREATE UNIQUE INDEX inc_fk1_idx_c1_c2 ON inc_table_foreign_key_1(c1, c2);
CREATE UNIQUE INDEX inc_fk1_idx_c3_c4 ON inc_table_foreign_key_1(c3, c4);

CREATE TABLE inc_table_foreign_key_2 (
     c1    INT,
     c2    NUMBER,
     c3    VARCHAR2(100),
     c4    CHAR(100),
     c5    INT,
     c6    NUMBER,
     CONSTRAINT inc_fk1_pk_c1_c2 PRIMARY KEY (c1, c2),
     CONSTRAINT inc_fk1_fk_c4_c3 FOREIGN KEY (c4, c3) REFERENCES inc_table_foreign_key_1 (c4, c3),
     CONSTRAINT inc_fk1_fk_c5_c6 FOREIGN KEY (c5, c6) REFERENCES inc_table_foreign_key_2 (c1, c2) ON DELETE CASCADE
);

