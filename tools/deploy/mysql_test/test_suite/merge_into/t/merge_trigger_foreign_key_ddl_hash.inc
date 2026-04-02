
create sequence seq
increment by 1
start with 1
nomaxvalue
nocycle;
create table category (
  c_cat varchar2(10) primary key
);

create table products (
  p_name varchar2(10) unique,
  p_cat varchar2(10),
  p_id integer primary key,
  p_cat_len int generated always as (length(p_cat)),
  constraint cst_fk1 foreign key (p_cat) references category (c_cat) on delete CASCADE,
  constraint cst_ck1 check (p_name != 'NP+CC+BU')
)
partition by hash(p_id) partitions 3;

create table newproducts (
  np_name varchar2(10),
  np_cat varchar2(10),
  np_id integer
);

create table orders (
  o_name varchar2(10),
  constraint cst_fk2 foreign key (o_name) references products (p_name) on delete CASCADE
);

create table trg_log (
  log_seq int,
  log_id  int,
  log_tp  varchar2(10),
  log_ev  varchar2(10),
  log_old varchar2(10),
  log_new varchar2(10)
);

