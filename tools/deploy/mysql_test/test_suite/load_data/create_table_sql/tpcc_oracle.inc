
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE customer;
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE district;
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE history;
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE item;
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE new_order;
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE order_line;
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE orders;
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE stock;
--source mysql_test/include/ignore_drop_table_err.inc
DROP TABLE warehouse;

CREATE TABLE customer (
  c_id number(11) NOT NULL default '0',
  c_d_id number(11) NOT NULL default '0',
  c_w_id number(11) NOT NULL default '0',
  c_first varchar2(16 char) default NULL,
  c_middle char(2) default NULL,
  c_last varchar2(16 char) default NULL,
  c_street_1 varchar2(20 char) default NULL,
  c_street_2 varchar2(20 char) default NULL,
  c_city varchar2(20 char) default NULL,
  c_state char(2) default NULL,
  c_zip varchar2(9 char) default NULL,
  c_phone varchar2(16 char) default NULL,
  c_since timestamp NOT NULL,
  c_credit char(2) default NULL,
  c_credit_lim number(24,12) default NULL,
  c_discount number(4,4) default NULL,
  c_balance number(24,12) default NULL,
  c_ytd_payment number(24,12) default NULL,
  c_payment_cnt number(14) default NULL,
  c_delivery_cnt number(14) default NULL,
  c_data varchar2(500 char),
  PRIMARY KEY  (c_w_id,c_d_id,c_id)
);

CREATE TABLE district (
  d_id number(11) NOT NULL default '0',
  d_w_id number(11) NOT NULL default '0',
  d_name varchar2(10 char) default NULL,
  d_street_1 varchar2(20 char) default NULL,
  d_street_2 varchar2(20 char) default NULL,
  d_city varchar2(20 char) default NULL,
  d_state char(2) default NULL,
  d_zip varchar2(9 char) default NULL,
  d_tax number(4,4) default NULL,
  d_ytd number(24,12) default NULL,
  d_next_o_id number(11) default NULL,
  PRIMARY KEY (d_w_id,d_id)
);

CREATE TABLE history (
  -- h_id number(20) NOT NULL,
  h_c_id number(11) default NULL,
  h_c_d_id number(11) default NULL,
  h_c_w_id number(11) default NULL,
  h_d_id number(11) default NULL,
  h_w_id number(11) NOT NULL default '0',
  h_date timestamp NOT NULL,
  h_amount number(6,2) default NULL,
  h_data varchar2(24 char) default NULL
  -- PRIMARY KEY (h_id, h_w_id)
);

CREATE TABLE item (
  i_id number(11) NOT NULL default '0',
  i_im_id number(11) default NULL,
  i_name varchar2(24 char) default NULL,
  i_price number(6,2) default NULL,
  i_data varchar2(50 char) default NULL,
  PRIMARY KEY (i_id)
);

CREATE TABLE new_order (
  no_o_id number(11) NOT NULL default '0',
  no_d_id number(11) NOT NULL default '0',
  no_w_id number(11) NOT NULL default '0',
  PRIMARY KEY  (no_w_id,no_d_id,no_o_id)
);

CREATE TABLE order_line (
  ol_o_id number(11) NOT NULL default '0',
  ol_d_id number(11) NOT NULL default '0',
  ol_w_id number(11) NOT NULL default '0',
  ol_number number(11) NOT NULL default '0',
  ol_i_id number(11) default NULL,
  ol_supply_w_id number(11) default NULL,
  ol_delivery_d timestamp NOT NULL,
  ol_quantity number(2) default NULL,
  ol_amount number(6,2) default NULL,
  ol_dist_info varchar2(24 char) default NULL,
  PRIMARY KEY  (ol_w_id,ol_d_id,ol_o_id,ol_number)
);

CREATE TABLE orders (
  o_id number(11) NOT NULL default '0',
  o_d_id number(11) NOT NULL default '0',
  o_w_id number(11) NOT NULL default '0',
  o_c_id number(11) default NULL,
  o_entry_d timestamp NOT NULL,
  o_carrier_id number(11) default NULL,
  o_ol_cnt number(11) default NULL,
  o_all_local number(1) default NULL,
  PRIMARY KEY (o_w_id,o_d_id,o_id)
);

CREATE TABLE stock (
  s_i_id number(11) NOT NULL default '0',
  s_w_id number(11) NOT NULL default '0',
  s_quantity number(4) NOT NULL default '0',
  s_dist_01 varchar2(24 char) default NULL,
  s_dist_02 varchar2(24 char) default NULL,
  s_dist_03 varchar2(24 char) default NULL,
  s_dist_04 varchar2(24 char) default NULL,
  s_dist_05 varchar2(24 char) default NULL,
  s_dist_06 varchar2(24 char) default NULL,
  s_dist_07 varchar2(24 char) default NULL,
  s_dist_08 varchar2(24 char) default NULL,
  s_dist_09 varchar2(24 char) default NULL,
  s_dist_10 varchar2(24 char) default NULL,
  s_ytd number(16,8) default NULL,
  s_order_cnt number(14) default NULL,
  s_remote_cnt number(14) default NULL,
  s_data varchar2(50 char) default NULL,
  PRIMARY KEY (s_w_id,s_i_id)
);

CREATE TABLE warehouse (
  w_id number(11) NOT NULL default '0',
  w_name varchar2(10 char) default NULL,
  w_street_1 varchar2(20 char) default NULL,
  w_street_2 varchar2(20 char) default NULL,
  w_city varchar2(20 char) default NULL,
  w_state char(2) default NULL,
  w_zip varchar2(9 char) default NULL,
  w_tax number(4,4) default NULL,
  w_ytd number(24,12) default NULL,
  PRIMARY KEY (w_id)
);

