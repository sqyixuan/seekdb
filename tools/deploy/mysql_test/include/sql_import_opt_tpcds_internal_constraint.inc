--disable_query_log
--disable_result_log

if ($TENANT == 'mysql') {
DROP DATABASE IF EXISTS `OPT_TPCDS_CHECK`;
CREATE DATABASE `OPT_TPCDS_CHECK`;
create table OPT_TPCDS_CHECK.date_dim
(
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                          ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       ,
    primary key (d_date_sk)
);

create table OPT_TPCDS_CHECK.call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             number(5,2)                  ,
    cc_tax_percentage         number(5,2)                  ,
    primary key (cc_call_center_sk)                        ,
    constraint cc_d1 foreign key(cc_closed_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cc_d2 foreign key(cc_open_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.catalog_page
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  ,
    primary key (cp_catalog_page_sk)                        ,
    constraint cp_d1 foreign key(cp_end_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cp_d2 foreign key(cp_start_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.item
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           number(7,2)                  ,
    i_wholesale_cost          number(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      ,
    primary key (i_item_sk)
);

create table OPT_TPCDS_CHECK.reason
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     ,
    primary key (r_reason_sk)
);

create table OPT_TPCDS_CHECK.customer_address
(
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             number(5,2)                  ,
    ca_location_type          char(20)                      ,
    primary key (ca_address_sk)
);

create table OPT_TPCDS_CHECK.income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       ,
    primary key (ib_income_band_sk)
);

create table OPT_TPCDS_CHECK.customer_demographics
(
    cd_demo_sk                integer               not null,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       ,
    primary key (cd_demo_sk)
);

create table OPT_TPCDS_CHECK.household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       ,
    primary key (hd_demo_sk)                                ,
    constraint hd_ib foreign key(hd_income_band_sk) references OPT_TPCDS_CHECK.income_band (ib_income_band_sk)
);

create table OPT_TPCDS_CHECK.customer
(
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      ,
    primary key (c_customer_sk)                             ,
    constraint c_a foreign key  (c_current_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint c_cd foreign key  (c_current_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint c_hd foreign key  (c_current_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint c_fsd foreign key  (c_first_sales_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint c_fsd2 foreign key  (c_first_shipto_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.time_dim
(
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      ,
    primary key (t_time_sk)
);

create table OPT_TPCDS_CHECK.ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      ,
    primary key (sm_ship_mode_sk)
);

create table OPT_TPCDS_CHECK.warehouse
(
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              number(5,2)                  ,
    primary key (w_warehouse_sk)
);

create table OPT_TPCDS_CHECK.promotion
(
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    number(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       ,
    primary key (p_promo_sk)                                ,
    constraint p_end_date foreign key(p_end_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint p_i foreign key(p_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint p_start_date foreign key(p_start_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer                       ,
    cs_order_number           integer               not null,
    cs_quantity               integer                       ,
    cs_wholesale_cost         number(7,2)                  ,
    cs_list_price             number(7,2)                  ,
    cs_sales_price            number(7,2)                  ,
    cs_ext_discount_amt       number(7,2)                  ,
    cs_ext_sales_price        number(7,2)                  ,
    cs_ext_wholesale_cost     number(7,2)                  ,
    cs_ext_list_price         number(7,2)                  ,
    cs_ext_tax                number(7,2)                  ,
    cs_coupon_amt             number(7,2)                  ,
    cs_ext_ship_cost          number(7,2)                  ,
    cs_net_paid               number(7,2)                  ,
    cs_net_paid_inc_tax       number(7,2)                  ,
    cs_net_paid_inc_ship      number(7,2)                  ,
    cs_net_paid_inc_ship_tax  number(7,2)                  ,
    cs_net_profit             number(7,2)                  ,
    primary key (cs_item_sk, cs_order_number)              ,
    constraint cs_b_a foreign key  (cs_bill_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint cs_b_cd foreign key  (cs_bill_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint cs_b_c foreign key  (cs_bill_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint cs_b_hd foreign key  (cs_bill_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint cs_cc foreign key  (cs_call_center_sk) references OPT_TPCDS_CHECK.call_center (cc_call_center_sk),
    constraint cs_cp foreign key  (cs_catalog_page_sk) references OPT_TPCDS_CHECK.catalog_page (cp_catalog_page_sk),
    constraint cs_i foreign key  (cs_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint cs_p foreign key  (cs_promo_sk) references OPT_TPCDS_CHECK.promotion (p_promo_sk),
    constraint cs_s_a foreign key  (cs_ship_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint cs_s_cd foreign key  (cs_ship_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint cs_s_c foreign key  (cs_ship_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint cs_d1 foreign key  (cs_ship_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cs_s_hd foreign key  (cs_ship_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint cs_sm foreign key  (cs_ship_mode_sk) references OPT_TPCDS_CHECK.ship_mode (sm_ship_mode_sk),
    constraint cs_d2 foreign key  (cs_sold_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cs_t foreign key  (cs_sold_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint cs_w foreign key  (cs_warehouse_sk) references OPT_TPCDS_CHECK.warehouse (w_warehouse_sk)
);

create table OPT_TPCDS_CHECK.catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           integer               not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          number(7,2)                  ,
    cr_return_tax             number(7,2)                  ,
    cr_return_amt_inc_tax     number(7,2)                  ,
    cr_fee                    number(7,2)                  ,
    cr_return_ship_cost       number(7,2)                  ,
    cr_refunded_cash          number(7,2)                  ,
    cr_reversed_charge        number(7,2)                  ,
    cr_store_credit           number(7,2)                  ,
    cr_net_loss               number(7,2)                  ,
    primary key (cr_item_sk, cr_order_number)              ,
    constraint cr_cc foreign key  (cr_call_center_sk) references OPT_TPCDS_CHECK.call_center (cc_call_center_sk),
    constraint cr_cp foreign key  (cr_catalog_page_sk) references OPT_TPCDS_CHECK.catalog_page (cp_catalog_page_sk),
    constraint cr_i foreign key  (cr_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint cr_r foreign key  (cr_reason_sk) references OPT_TPCDS_CHECK.reason (r_reason_sk),
    constraint cr_a1 foreign key  (cr_refunded_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint cr_cd1 foreign key  (cr_refunded_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint cr_c1 foreign key  (cr_refunded_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint cr_hd1 foreign key  (cr_refunded_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint cr_d1 foreign key  (cr_returned_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cr_t foreign key  (cr_returned_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint cr_a2 foreign key  (cr_returning_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint cr_cd2 foreign key  (cr_returning_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint cr_c2 foreign key  (cr_returning_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint cr_hd2 foreign key  (cr_returning_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint cr_sm foreign key  (cr_ship_mode_sk) references OPT_TPCDS_CHECK.ship_mode (sm_ship_mode_sk),
    constraint cr_w2 foreign key  (cr_warehouse_sk) references OPT_TPCDS_CHECK.warehouse (w_warehouse_sk)
);

create table OPT_TPCDS_CHECK.inventory
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       ,
    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk),
    constraint inv_d foreign key(inv_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint inv_i foreign key(inv_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint inv_w foreign key(inv_warehouse_sk) references OPT_TPCDS_CHECK.warehouse (w_warehouse_sk)
);

create table OPT_TPCDS_CHECK.store
(
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_rec_start_date          date                          ,
    s_rec_end_date            date                          ,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              number(5,2)                  ,
    s_tax_precentage          number(5,2)                  ,
    primary key (s_store_sk)                               ,
    constraint s_close_date foreign key(s_closed_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.store_returns
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          integer               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             number(7,2)                  ,
    sr_return_tax             number(7,2)                  ,
    sr_return_amt_inc_tax     number(7,2)                  ,
    sr_fee                    number(7,2)                  ,
    sr_return_ship_cost       number(7,2)                  ,
    sr_refunded_cash          number(7,2)                  ,
    sr_reversed_charge        number(7,2)                  ,
    sr_store_credit           number(7,2)                  ,
    sr_net_loss               number(7,2)                  ,
    primary key (sr_item_sk, sr_ticket_number)             ,
    constraint sr_a foreign key  (sr_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint sr_cd foreign key  (sr_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint sr_c foreign key  (sr_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint sr_hd foreign key  (sr_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint sr_i foreign key  (sr_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint sr_r foreign key  (sr_reason_sk) references OPT_TPCDS_CHECK.reason (r_reason_sk),
    constraint sr_ret_d foreign key  (sr_returned_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint sr_t foreign key  (sr_return_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint sr_s foreign key  (sr_store_sk) references OPT_TPCDS_CHECK.store (s_store_sk)
);

create table OPT_TPCDS_CHECK.store_sales
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          integer               not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         number(7,2)                  ,
    ss_list_price             number(7,2)                  ,
    ss_sales_price            number(7,2)                  ,
    ss_ext_discount_amt       number(7,2)                  ,
    ss_ext_sales_price        number(7,2)                  ,
    ss_ext_wholesale_cost     number(7,2)                  ,
    ss_ext_list_price         number(7,2)                  ,
    ss_ext_tax                number(7,2)                  ,
    ss_coupon_amt             number(7,2)                  ,
    ss_net_paid               number(7,2)                  ,
    ss_net_paid_inc_tax       number(7,2)                  ,
    ss_net_profit             number(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)             ,
    constraint ss_a foreign key  (ss_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint ss_cd foreign key  (ss_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint ss_c foreign key  (ss_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint ss_hd foreign key  (ss_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint ss_i foreign key  (ss_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint ss_p foreign key  (ss_promo_sk) references OPT_TPCDS_CHECK.promotion (p_promo_sk),
    constraint ss_d foreign key  (ss_sold_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint ss_t foreign key  (ss_sold_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint ss_s foreign key  (ss_store_sk) references OPT_TPCDS_CHECK.store (s_store_sk)
);

create table OPT_TPCDS_CHECK.web_page
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       ,
    primary key (wp_web_page_sk)                            ,
    constraint wp_ad foreign key  (wp_access_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint wp_cd foreign key  (wp_creation_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.dbgen_version
(
    dv_version                varchar(16)                   ,
    dv_create_date            date                          ,
    dv_create_time            varchar(20)                   ,
    dv_cmdline_args           varchar(200)
);

create table OPT_TPCDS_CHECK.web_site
(
    web_site_sk               integer               not null,
    web_site_id               char(16)              not null,
    web_rec_start_date        date                          ,
    web_rec_end_date          date                          ,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            number(5,2)                  ,
    web_tax_percentage        number(5,2)                  ,
    primary key (web_site_sk)                               ,
    constraint web_d1 foreign key  (web_close_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint web_d2 foreign key  (web_open_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.web_returns
(
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           integer               not null,
    wr_return_quantity        integer                       ,
    wr_return_amt             number(7,2)                  ,
    wr_return_tax             number(7,2)                  ,
    wr_return_amt_inc_tax     number(7,2)                  ,
    wr_fee                    number(7,2)                  ,
    wr_return_ship_cost       number(7,2)                  ,
    wr_refunded_cash          number(7,2)                  ,
    wr_reversed_charge        number(7,2)                  ,
    wr_account_credit         number(7,2)                  ,
    wr_net_loss               number(7,2)                  ,
    primary key (wr_item_sk, wr_order_number)              ,
    constraint wr_i foreign key  (wr_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint wr_r foreign key  (wr_reason_sk) references OPT_TPCDS_CHECK.reason (r_reason_sk),
    constraint wr_ref_a foreign key  (wr_refunded_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint wr_ref_cd foreign key  (wr_refunded_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint wr_ref_c foreign key  (wr_refunded_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint wr_ref_hd foreign key  (wr_refunded_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint wr_ret_d foreign key  (wr_returned_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint wr_ret_t foreign key  (wr_returned_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint wr_ret_a foreign key  (wr_returning_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint wr_ret_cd foreign key  (wr_returning_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint wr_ret_c foreign key  (wr_returning_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint wr_ret_hd foreign key  (wr_returning_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint wr_wp foreign key  (wr_web_page_sk) references OPT_TPCDS_CHECK.web_page (wp_web_page_sk)
);

create table OPT_TPCDS_CHECK.web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           integer               not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         number(7,2)                  ,
    ws_list_price             number(7,2)                  ,
    ws_sales_price            number(7,2)                  ,
    ws_ext_discount_amt       number(7,2)                  ,
    ws_ext_sales_price        number(7,2)                  ,
    ws_ext_wholesale_cost     number(7,2)                  ,
    ws_ext_list_price         number(7,2)                  ,
    ws_ext_tax                number(7,2)                  ,
    ws_coupon_amt             number(7,2)                  ,
    ws_ext_ship_cost          number(7,2)                  ,
    ws_net_paid               number(7,2)                  ,
    ws_net_paid_inc_tax       number(7,2)                  ,
    ws_net_paid_inc_ship      number(7,2)                  ,
    ws_net_paid_inc_ship_tax  number(7,2)                  ,
    ws_net_profit             number(7,2)                  ,
    primary key (ws_item_sk, ws_order_number)              ,
    constraint ws_b_a foreign key  (ws_bill_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint ws_b_cd foreign key  (ws_bill_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint ws_b_c foreign key  (ws_bill_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint ws_b_hd foreign key  (ws_bill_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint ws_i foreign key  (ws_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint ws_p foreign key  (ws_promo_sk) references OPT_TPCDS_CHECK.promotion (p_promo_sk),
    constraint ws_s_a foreign key  (ws_ship_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint ws_s_cd foreign key  (ws_ship_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint ws_s_c foreign key  (ws_ship_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint ws_s_d foreign key  (ws_ship_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint ws_s_hd foreign key  (ws_ship_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint ws_sm foreign key  (ws_ship_mode_sk) references OPT_TPCDS_CHECK.ship_mode (sm_ship_mode_sk),
    constraint ws_d2 foreign key  (ws_sold_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint ws_t foreign key  (ws_sold_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint ws_w2 foreign key  (ws_warehouse_sk) references OPT_TPCDS_CHECK.warehouse (w_warehouse_sk),
    constraint ws_wp foreign key  (ws_web_page_sk) references OPT_TPCDS_CHECK.web_page (wp_web_page_sk),
    constraint ws_ws foreign key  (ws_web_site_sk) references OPT_TPCDS_CHECK.web_site (web_site_sk)
);
}

if ($TENANT == 'oracle') {
--error 0,600
DROP USER OPT_TPCDS_CHECK cascade;
CREATE USER OPT_TPCDS_CHECK identified by 123;
GRANT ALL ON *.* TO OPT_TPCDS_CHECK;
create table OPT_TPCDS_CHECK.date_dim
(
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                          ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       ,
    primary key (d_date_sk)
);

create table OPT_TPCDS_CHECK.call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             number(5,2)                  ,
    cc_tax_percentage         number(5,2)                  ,
    primary key (cc_call_center_sk)                        ,
    constraint cc_d1 foreign key(cc_closed_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cc_d2 foreign key(cc_open_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.catalog_page
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  ,
    primary key (cp_catalog_page_sk)                        ,
    constraint cp_d1 foreign key(cp_end_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cp_d2 foreign key(cp_start_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.item
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           number(7,2)                  ,
    i_wholesale_cost          number(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      ,
    primary key (i_item_sk)
);

create table OPT_TPCDS_CHECK.reason
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     ,
    primary key (r_reason_sk)
);

create table OPT_TPCDS_CHECK.customer_address
(
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             number(5,2)                  ,
    ca_location_type          char(20)                      ,
    primary key (ca_address_sk)
);

create table OPT_TPCDS_CHECK.customer_demographics
(
    cd_demo_sk                integer               not null,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       ,
    primary key (cd_demo_sk)
);

create table OPT_TPCDS_CHECK.income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       ,
    primary key (ib_income_band_sk)
);

create table OPT_TPCDS_CHECK.household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       ,
    primary key (hd_demo_sk)                                ,
    constraint hd_ib foreign key(hd_income_band_sk) references OPT_TPCDS_CHECK.income_band (ib_income_band_sk)
);

create table OPT_TPCDS_CHECK.customer
(
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      ,
    primary key (c_customer_sk)                             ,
    constraint c_a foreign key  (c_current_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint c_cd foreign key  (c_current_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint c_hd foreign key  (c_current_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint c_fsd foreign key  (c_first_sales_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint c_fsd2 foreign key  (c_first_shipto_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.time_dim
(
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      ,
    primary key (t_time_sk)
);

create table OPT_TPCDS_CHECK.ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      ,
    primary key (sm_ship_mode_sk)
);

create table OPT_TPCDS_CHECK.warehouse
(
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              number(5,2)                  ,
    primary key (w_warehouse_sk)
);

create table OPT_TPCDS_CHECK.promotion
(
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    number(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       ,
    primary key (p_promo_sk)                                ,
    constraint p_end_date foreign key(p_end_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint p_i foreign key(p_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint p_start_date foreign key(p_start_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer                       ,
    cs_order_number           integer               not null,
    cs_quantity               integer                       ,
    cs_wholesale_cost         number(7,2)                  ,
    cs_list_price             number(7,2)                  ,
    cs_sales_price            number(7,2)                  ,
    cs_ext_discount_amt       number(7,2)                  ,
    cs_ext_sales_price        number(7,2)                  ,
    cs_ext_wholesale_cost     number(7,2)                  ,
    cs_ext_list_price         number(7,2)                  ,
    cs_ext_tax                number(7,2)                  ,
    cs_coupon_amt             number(7,2)                  ,
    cs_ext_ship_cost          number(7,2)                  ,
    cs_net_paid               number(7,2)                  ,
    cs_net_paid_inc_tax       number(7,2)                  ,
    cs_net_paid_inc_ship      number(7,2)                  ,
    cs_net_paid_inc_ship_tax  number(7,2)                  ,
    cs_net_profit             number(7,2)                  ,
    primary key (cs_item_sk, cs_order_number)              ,
    constraint cs_b_a foreign key  (cs_bill_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint cs_b_cd foreign key  (cs_bill_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint cs_b_c foreign key  (cs_bill_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint cs_b_hd foreign key  (cs_bill_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint cs_cc foreign key  (cs_call_center_sk) references OPT_TPCDS_CHECK.call_center (cc_call_center_sk),
    constraint cs_cp foreign key  (cs_catalog_page_sk) references OPT_TPCDS_CHECK.catalog_page (cp_catalog_page_sk),
    constraint cs_i foreign key  (cs_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint cs_p foreign key  (cs_promo_sk) references OPT_TPCDS_CHECK.promotion (p_promo_sk),
    constraint cs_s_a foreign key  (cs_ship_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint cs_s_cd foreign key  (cs_ship_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint cs_s_c foreign key  (cs_ship_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint cs_d1 foreign key  (cs_ship_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cs_s_hd foreign key  (cs_ship_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint cs_sm foreign key  (cs_ship_mode_sk) references OPT_TPCDS_CHECK.ship_mode (sm_ship_mode_sk),
    constraint cs_d2 foreign key  (cs_sold_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cs_t foreign key  (cs_sold_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint cs_w foreign key  (cs_warehouse_sk) references OPT_TPCDS_CHECK.warehouse (w_warehouse_sk)
);

create table OPT_TPCDS_CHECK.catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           integer               not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          number(7,2)                  ,
    cr_return_tax             number(7,2)                  ,
    cr_return_amt_inc_tax     number(7,2)                  ,
    cr_fee                    number(7,2)                  ,
    cr_return_ship_cost       number(7,2)                  ,
    cr_refunded_cash          number(7,2)                  ,
    cr_reversed_charge        number(7,2)                  ,
    cr_store_credit           number(7,2)                  ,
    cr_net_loss               number(7,2)                  ,
    primary key (cr_item_sk, cr_order_number)              ,
    constraint cr_cc foreign key  (cr_call_center_sk) references OPT_TPCDS_CHECK.call_center (cc_call_center_sk),
    constraint cr_cp foreign key  (cr_catalog_page_sk) references OPT_TPCDS_CHECK.catalog_page (cp_catalog_page_sk),
    constraint cr_i foreign key  (cr_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint cr_r foreign key  (cr_reason_sk) references OPT_TPCDS_CHECK.reason (r_reason_sk),
    constraint cr_a1 foreign key  (cr_refunded_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint cr_cd1 foreign key  (cr_refunded_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint cr_c1 foreign key  (cr_refunded_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint cr_hd1 foreign key  (cr_refunded_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint cr_d1 foreign key  (cr_returned_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint cr_t foreign key  (cr_returned_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint cr_a2 foreign key  (cr_returning_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint cr_cd2 foreign key  (cr_returning_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint cr_c2 foreign key  (cr_returning_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint cr_hd2 foreign key  (cr_returning_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint cr_sm foreign key  (cr_ship_mode_sk) references OPT_TPCDS_CHECK.ship_mode (sm_ship_mode_sk),
    constraint cr_w2 foreign key  (cr_warehouse_sk) references OPT_TPCDS_CHECK.warehouse (w_warehouse_sk)
);

create table OPT_TPCDS_CHECK.inventory
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       ,
    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk),
    constraint inv_d foreign key(inv_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint inv_i foreign key(inv_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint inv_w foreign key(inv_warehouse_sk) references OPT_TPCDS_CHECK.warehouse (w_warehouse_sk)
);

create table OPT_TPCDS_CHECK.store
(
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_rec_start_date          date                          ,
    s_rec_end_date            date                          ,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              number(5,2)                  ,
    s_tax_precentage          number(5,2)                  ,
    primary key (s_store_sk)                               ,
    constraint s_close_date foreign key(s_closed_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.store_returns
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          integer               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             number(7,2)                  ,
    sr_return_tax             number(7,2)                  ,
    sr_return_amt_inc_tax     number(7,2)                  ,
    sr_fee                    number(7,2)                  ,
    sr_return_ship_cost       number(7,2)                  ,
    sr_refunded_cash          number(7,2)                  ,
    sr_reversed_charge        number(7,2)                  ,
    sr_store_credit           number(7,2)                  ,
    sr_net_loss               number(7,2)                  ,
    primary key (sr_item_sk, sr_ticket_number)             ,
    constraint sr_a foreign key  (sr_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint sr_cd foreign key  (sr_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint sr_c foreign key  (sr_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint sr_hd foreign key  (sr_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint sr_i foreign key  (sr_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint sr_r foreign key  (sr_reason_sk) references OPT_TPCDS_CHECK.reason (r_reason_sk),
    constraint sr_ret_d foreign key  (sr_returned_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint sr_t foreign key  (sr_return_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint sr_s foreign key  (sr_store_sk) references OPT_TPCDS_CHECK.store (s_store_sk)
);

create table OPT_TPCDS_CHECK.store_sales
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          integer               not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         number(7,2)                  ,
    ss_list_price             number(7,2)                  ,
    ss_sales_price            number(7,2)                  ,
    ss_ext_discount_amt       number(7,2)                  ,
    ss_ext_sales_price        number(7,2)                  ,
    ss_ext_wholesale_cost     number(7,2)                  ,
    ss_ext_list_price         number(7,2)                  ,
    ss_ext_tax                number(7,2)                  ,
    ss_coupon_amt             number(7,2)                  ,
    ss_net_paid               number(7,2)                  ,
    ss_net_paid_inc_tax       number(7,2)                  ,
    ss_net_profit             number(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)             ,
    constraint ss_a foreign key  (ss_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint ss_cd foreign key  (ss_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint ss_c foreign key  (ss_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint ss_hd foreign key  (ss_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint ss_i foreign key  (ss_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint ss_p foreign key  (ss_promo_sk) references OPT_TPCDS_CHECK.promotion (p_promo_sk),
    constraint ss_d foreign key  (ss_sold_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint ss_t foreign key  (ss_sold_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint ss_s foreign key  (ss_store_sk) references OPT_TPCDS_CHECK.store (s_store_sk)
);

create table OPT_TPCDS_CHECK.web_page
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       ,
    primary key (wp_web_page_sk)                            ,
    constraint wp_ad foreign key  (wp_access_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint wp_cd foreign key  (wp_creation_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.dbgen_version
(
    dv_version                varchar(16)                   ,
    dv_create_date            date                          ,
    dv_create_time            varchar(20)                   ,
    dv_cmdline_args           varchar(200)
);

create table OPT_TPCDS_CHECK.web_site
(
    web_site_sk               integer               not null,
    web_site_id               char(16)              not null,
    web_rec_start_date        date                          ,
    web_rec_end_date          date                          ,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            number(5,2)                  ,
    web_tax_percentage        number(5,2)                  ,
    primary key (web_site_sk)                               ,
    constraint web_d1 foreign key  (web_close_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint web_d2 foreign key  (web_open_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk)
);

create table OPT_TPCDS_CHECK.web_returns
(
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           integer               not null,
    wr_return_quantity        integer                       ,
    wr_return_amt             number(7,2)                  ,
    wr_return_tax             number(7,2)                  ,
    wr_return_amt_inc_tax     number(7,2)                  ,
    wr_fee                    number(7,2)                  ,
    wr_return_ship_cost       number(7,2)                  ,
    wr_refunded_cash          number(7,2)                  ,
    wr_reversed_charge        number(7,2)                  ,
    wr_account_credit         number(7,2)                  ,
    wr_net_loss               number(7,2)                  ,
    primary key (wr_item_sk, wr_order_number)              ,
    constraint wr_i foreign key  (wr_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint wr_r foreign key  (wr_reason_sk) references OPT_TPCDS_CHECK.reason (r_reason_sk),
    constraint wr_ref_a foreign key  (wr_refunded_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint wr_ref_cd foreign key  (wr_refunded_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint wr_ref_c foreign key  (wr_refunded_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint wr_ref_hd foreign key  (wr_refunded_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint wr_ret_d foreign key  (wr_returned_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint wr_ret_t foreign key  (wr_returned_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint wr_ret_a foreign key  (wr_returning_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint wr_ret_cd foreign key  (wr_returning_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint wr_ret_c foreign key  (wr_returning_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint wr_ret_hd foreign key  (wr_returning_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint wr_wp foreign key  (wr_web_page_sk) references OPT_TPCDS_CHECK.web_page (wp_web_page_sk)
);

create table OPT_TPCDS_CHECK.web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           integer               not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         number(7,2)                  ,
    ws_list_price             number(7,2)                  ,
    ws_sales_price            number(7,2)                  ,
    ws_ext_discount_amt       number(7,2)                  ,
    ws_ext_sales_price        number(7,2)                  ,
    ws_ext_wholesale_cost     number(7,2)                  ,
    ws_ext_list_price         number(7,2)                  ,
    ws_ext_tax                number(7,2)                  ,
    ws_coupon_amt             number(7,2)                  ,
    ws_ext_ship_cost          number(7,2)                  ,
    ws_net_paid               number(7,2)                  ,
    ws_net_paid_inc_tax       number(7,2)                  ,
    ws_net_paid_inc_ship      number(7,2)                  ,
    ws_net_paid_inc_ship_tax  number(7,2)                  ,
    ws_net_profit             number(7,2)                  ,
    primary key (ws_item_sk, ws_order_number)              ,
    constraint ws_b_a foreign key  (ws_bill_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint ws_b_cd foreign key  (ws_bill_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint ws_b_c foreign key  (ws_bill_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint ws_b_hd foreign key  (ws_bill_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint ws_i foreign key  (ws_item_sk) references OPT_TPCDS_CHECK.item (i_item_sk),
    constraint ws_p foreign key  (ws_promo_sk) references OPT_TPCDS_CHECK.promotion (p_promo_sk),
    constraint ws_s_a foreign key  (ws_ship_addr_sk) references OPT_TPCDS_CHECK.customer_address (ca_address_sk),
    constraint ws_s_cd foreign key  (ws_ship_cdemo_sk) references OPT_TPCDS_CHECK.customer_demographics (cd_demo_sk),
    constraint ws_s_c foreign key  (ws_ship_customer_sk) references OPT_TPCDS_CHECK.customer (c_customer_sk),
    constraint ws_s_d foreign key  (ws_ship_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint ws_s_hd foreign key  (ws_ship_hdemo_sk) references OPT_TPCDS_CHECK.household_demographics (hd_demo_sk),
    constraint ws_sm foreign key  (ws_ship_mode_sk) references OPT_TPCDS_CHECK.ship_mode (sm_ship_mode_sk),
    constraint ws_d2 foreign key  (ws_sold_date_sk) references OPT_TPCDS_CHECK.date_dim (d_date_sk),
    constraint ws_t foreign key  (ws_sold_time_sk) references OPT_TPCDS_CHECK.time_dim (t_time_sk),
    constraint ws_w2 foreign key  (ws_warehouse_sk) references OPT_TPCDS_CHECK.warehouse (w_warehouse_sk),
    constraint ws_wp foreign key  (ws_web_page_sk) references OPT_TPCDS_CHECK.web_page (wp_web_page_sk),
    constraint ws_ws foreign key  (ws_web_site_sk) references OPT_TPCDS_CHECK.web_site (web_site_sk)
);
}

--enable_query_log
--enable_result_log
