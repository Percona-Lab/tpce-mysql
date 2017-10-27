
DROP TABLE account_permission CASCADE CONSTRAINTS;
CREATE TABLE account_permission (
  ap_ca_id NUMBER(11) NOT NULL,
  ap_acl CHAR(4) NOT NULL,
  ap_tax_id VARCHAR2(20) NOT NULL,
  ap_l_name VARCHAR2(25) NOT NULL,
  ap_f_name VARCHAR2(20) NOT NULL
);

DROP TABLE customer CASCADE CONSTRAINTS;
CREATE TABLE customer (
  c_id NUMBER(11) NOT NULL,
  c_tax_id VARCHAR2(20) NOT NULL,
  c_st_id CHAR(4) NOT NULL,
  c_l_name VARCHAR2(25) NOT NULL,
  c_f_name VARCHAR2(20) NOT NULL,
  c_m_name CHAR(1),
  c_gndr CHAR(1),
  c_tier NUMBER(1) NOT NULL,
  c_dob DATE NOT NULL,
  c_ad_id NUMBER(11) NOT NULL,
  c_ctry_1 VARCHAR2(3),
  c_area_1 VARCHAR2(3),
  c_local_1 VARCHAR2(10),
  c_ext_1 VARCHAR2(5),
  c_ctry_2 VARCHAR2(3),
  c_area_2 VARCHAR2(3),
  c_local_2 VARCHAR2(10),
  c_ext_2 VARCHAR2(5),
  c_ctry_3 VARCHAR2(3),
  c_area_3 VARCHAR2(3),
  c_local_3 VARCHAR2(10),
  c_ext_3 VARCHAR2(5),
  c_email_1 VARCHAR2(50),
  c_email_2 VARCHAR2(50)
);

DROP TABLE customer_account CASCADE CONSTRAINTS;
CREATE TABLE customer_account (
  ca_id NUMBER(11) NOT NULL,
  ca_b_id NUMBER(11) NOT NULL,
  ca_c_id NUMBER(11) NOT NULL,
  ca_name VARCHAR2(50),
  ca_tax_st NUMBER(1) NOT NULL,
  ca_bal DECIMAL(12,2) NOT NULL
);

DROP TABLE customer_taxrate CASCADE CONSTRAINTS;
CREATE TABLE customer_taxrate (
  cx_tx_id CHAR(4) NOT NULL,
  cx_c_id NUMBER(11) NOT NULL
);

DROP TABLE holding CASCADE CONSTRAINTS;
CREATE TABLE holding (
  h_t_id NUMBER(15) NOT NULL,
  h_ca_id NUMBER(11) NOT NULL,
  h_s_symb CHAR(15) NOT NULL,
  h_dts TIMESTAMP NOT NULL,
  h_price DECIMAL(8,2) NOT NULL CHECK (h_price > 0),
  h_qty NUMBER(6) NOT NULL
);

DROP TABLE holding_history CASCADE CONSTRAINTS;
CREATE TABLE holding_history (
  hh_h_t_id NUMBER(15) NOT NULL,
  hh_t_id NUMBER(15) NOT NULL,
  hh_before_qty NUMBER(6) NOT NULL,
  hh_after_qty NUMBER(6) NOT NULL
);

DROP TABLE holding_summary CASCADE CONSTRAINTS;
CREATE TABLE holding_summary (
  hs_ca_id NUMBER(11) NOT NULL,
  hs_s_symb CHAR(15) NOT NULL,
  hs_qty NUMBER(6) NOT NULL
);

DROP TABLE watch_item CASCADE CONSTRAINTS;
CREATE TABLE watch_item (
  wi_wl_id NUMBER(11) NOT NULL,
  wi_s_symb CHAR(15) NOT NULL
);

DROP TABLE watch_list CASCADE CONSTRAINTS;
CREATE TABLE watch_list (
  wl_id NUMBER(11) NOT NULL,
  wl_c_id NUMBER(11) NOT NULL
);




DROP TABLE broker CASCADE CONSTRAINTS;
CREATE TABLE broker (
  b_id NUMBER(11) NOT NULL,
  b_st_id CHAR(4) NOT NULL,
  b_name VARCHAR2(49) NOT NULL,
  b_num_trades NUMBER(9) NOT NULL,
  b_comm_total DECIMAL(12,2) NOT NULL
);

DROP TABLE cash_transaction CASCADE CONSTRAINTS;
CREATE TABLE cash_transaction (
  ct_t_id NUMBER(15) NOT NULL,
  ct_dts TIMESTAMP NOT NULL,
  ct_amt DECIMAL(10,2) NOT NULL,
  ct_name VARCHAR2(100)
);

DROP TABLE charge CASCADE CONSTRAINTS;
CREATE TABLE charge (
  ch_tt_id CHAR(3) NOT NULL,
  ch_c_tier NUMBER(1) NOT NULL,
  ch_chrg DECIMAL(10,2) NOT NULL CHECK (ch_chrg > 0)
);

DROP TABLE commission_rate CASCADE CONSTRAINTS;
CREATE TABLE commission_rate (
  cr_c_tier NUMBER(1) NOT NULL,
  cr_tt_id CHAR(3) NOT NULL,
  cr_ex_id CHAR(6) NOT NULL,
  cr_from_qty NUMBER(6) NOT NULL CHECK (cr_from_qty >= 0),
  cr_to_qty NUMBER(6) NOT NULL,
  cr_rate DECIMAL(5,2) NOT NULL CHECK (cr_rate >= 0),
  CONSTRAINT ck_cr CHECK (cr_to_qty > cr_from_qty)
);

DROP TABLE settlement CASCADE CONSTRAINTS;
CREATE TABLE settlement (
  se_t_id NUMBER(15) NOT NULL,
  se_cash_type VARCHAR2(40) NOT NULL,
  se_cash_due_date DATE NOT NULL,
  se_amt DECIMAL(10,2) NOT NULL
);

DROP TABLE trade CASCADE CONSTRAINTS;
CREATE TABLE trade (
  t_id NUMBER(15) NOT NULL,
  t_dts TIMESTAMP NOT NULL,
  t_st_id CHAR(4) NOT NULL,
  t_tt_id CHAR(3) NOT NULL,
  t_is_cash NUMBER(1) NOT NULL,
  t_s_symb CHAR(15) NOT NULL,
  t_qty NUMBER(6) NOT NULL CHECK (t_qty > 0),
  t_bid_price DECIMAL(8,2) NOT NULL CHECK (t_bid_price > 0),
  t_ca_id NUMBER(11) NOT NULL,
  t_exec_name VARCHAR2(49) NOT NULL,
  t_trade_price DECIMAL(8,2),
  t_chrg DECIMAL(10,2) NOT NULL CHECK (t_chrg >= 0),
  t_comm DECIMAL(10,2) NOT NULL CHECK (t_comm >= 0),
  t_tax DECIMAL(10,2) NOT NULL CHECK (t_tax >= 0),
  t_lifo NUMBER(1) NOT NULL
);

DROP TABLE trade_history CASCADE CONSTRAINTS;
CREATE TABLE trade_history (
  th_t_id NUMBER(15) NOT NULL,
  th_dts TIMESTAMP NOT NULL,
  th_st_id CHAR(4) NOT NULL
);

DROP TABLE trade_request CASCADE CONSTRAINTS;
CREATE TABLE trade_request (
  tr_t_id NUMBER(15) NOT NULL,
  tr_tt_id CHAR(3) NOT NULL,
  tr_s_symb CHAR(15) NOT NULL,
  tr_qty NUMBER(6) NOT NULL CHECK (tr_qty > 0),
  tr_bid_price DECIMAL(8,2) NOT NULL CHECK (tr_bid_price > 0),
  tr_b_id NUMBER(11) NOT NULL
);

DROP TABLE trade_type CASCADE CONSTRAINTS;
CREATE TABLE trade_type (
  tt_id CHAR(3) NOT NULL,
  tt_name VARCHAR2(12) NOT NULL,
  tt_is_sell NUMBER(1) NOT NULL,
  tt_is_mrkt NUMBER(1) NOT NULL
);





DROP TABLE company CASCADE CONSTRAINTS;
CREATE TABLE company (
  co_id NUMBER(11) NOT NULL,
  co_st_id CHAR(4) NOT NULL,
  co_name VARCHAR2(60) NOT NULL,
  co_in_id CHAR(2) NOT NULL,
  co_sp_rate CHAR(4) NOT NULL,
  co_ceo VARCHAR2(46) NOT NULL,
  co_ad_id NUMBER(11) NOT NULL,
  co_desc VARCHAR2(150) NOT NULL,
  co_open_date DATE NOT NULL
);

DROP TABLE company_competitor CASCADE CONSTRAINTS;
CREATE TABLE company_competitor (
  cp_co_id NUMBER(11) NOT NULL,
  cp_comp_co_id NUMBER(11) NOT NULL,
  cp_in_id CHAR(2) NOT NULL
);

DROP TABLE daily_market CASCADE CONSTRAINTS;
CREATE TABLE daily_market (
  dm_date DATE NOT NULL,
  dm_s_symb CHAR(15) NOT NULL,
  dm_close DECIMAL(8,2) NOT NULL,
  dm_high DECIMAL(8,2) NOT NULL,
  dm_low DECIMAL(8,2) NOT NULL,
  dm_vol NUMBER(12) NOT NULL
);

DROP TABLE exchange CASCADE CONSTRAINTS;
CREATE TABLE exchange (
  ex_id CHAR(6) NOT NULL,
  ex_name VARCHAR2(100) NOT NULL,
  ex_num_symb NUMBER(6) NOT NULL,
  ex_open NUMBER(4) NOT NULL,
  ex_close NUMBER(4) NOT NULL,
  ex_desc VARCHAR2(150),
  ex_ad_id NUMBER(11) NOT NULL
);

DROP TABLE financial CASCADE CONSTRAINTS;
CREATE TABLE financial (
  fi_co_id NUMBER(11) NOT NULL,
  fi_year NUMBER(4) NOT NULL,
  fi_qtr NUMBER(1) NOT NULL,
  fi_qtr_start_date DATE NOT NULL,
  fi_revenue DECIMAL(15,2) NOT NULL,
  fi_net_earn DECIMAL(15,2) NOT NULL,
  fi_basic_eps DECIMAL(10,2) NOT NULL,
  fi_dilut_eps DECIMAL(10,2) NOT NULL,
  fi_margin DECIMAL(10,2) NOT NULL,
  fi_inventory DECIMAL(15,2) NOT NULL,
  fi_assets DECIMAL(15,2) NOT NULL,
  fi_liability DECIMAL(15,2) NOT NULL,
  fi_out_basic NUMBER(12) NOT NULL,
  fi_out_dilut NUMBER(12) NOT NULL
);

DROP TABLE industry CASCADE CONSTRAINTS;
CREATE TABLE industry (
  in_id CHAR(2) NOT NULL,
  in_name VARCHAR2(50) NOT NULL,
  in_sc_id CHAR(2) NOT NULL
);

DROP TABLE last_trade CASCADE CONSTRAINTS;
CREATE TABLE last_trade (
  lt_s_symb CHAR(15) NOT NULL,
  lt_dts TIMESTAMP NOT NULL,
  lt_price DECIMAL(8,2) NOT NULL,
  lt_open_price DECIMAL(8,2) NOT NULL,
  lt_vol NUMBER(12) NOT NULL
);





DROP TABLE news_item CASCADE CONSTRAINTS;
CREATE TABLE news_item (
  ni_id NUMBER(11) NOT NULL,
  ni_headline VARCHAR2(80) NOT NULL,
  ni_summary VARCHAR2(255) NOT NULL,
  ni_item BLOB NOT NULL,
  ni_dts TIMESTAMP NOT NULL,
  ni_source VARCHAR2(30) NOT NULL,
  ni_author VARCHAR2(30)
);

DROP TABLE news_xref CASCADE CONSTRAINTS;
CREATE TABLE news_xref (
  nx_ni_id NUMBER(11) NOT NULL,
  nx_co_id NUMBER(11) NOT NULL
);

DROP TABLE sector CASCADE CONSTRAINTS;
CREATE TABLE sector (
  sc_id CHAR(2) NOT NULL,
  sc_name VARCHAR2(30) NOT NULL
);

DROP TABLE security CASCADE CONSTRAINTS;
CREATE TABLE security (
  s_symb CHAR(15) NOT NULL,
  s_issue CHAR(6) NOT NULL,
  s_st_id CHAR(4) NOT NULL,
  s_name VARCHAR2(70) NOT NULL,
  s_ex_id CHAR(6) NOT NULL,
  s_co_id NUMBER(11) NOT NULL,
  s_num_out NUMBER(11) NOT NULL,
  s_start_date DATE NOT NULL,
  s_exch_date DATE NOT NULL,
  s_pe DECIMAL(10,2) NOT NULL,
  s_52wk_high DECIMAL(8,2) NOT NULL,
  s_52wk_high_date DATE NOT NULL,
  s_52wk_low DECIMAL(8,2) NOT NULL,
  s_52wk_low_date DATE NOT NULL,
  s_dividend DECIMAL(10,2) NOT NULL,
  s_yield DECIMAL(5,2) NOT NULL
);





DROP TABLE address CASCADE CONSTRAINTS;
CREATE TABLE address (
  ad_id NUMBER(11) NOT NULL,
  ad_line1 VARCHAR2(80),
  ad_line2 VARCHAR2(80),
  ad_zc_code CHAR(12) NOT NULL,
  ad_ctry VARCHAR2(80)
);

DROP TABLE status_type CASCADE CONSTRAINTS;
CREATE TABLE status_type (
  st_id CHAR(4) NOT NULL,
  st_name CHAR(10) NOT NULL
);

DROP TABLE taxrate CASCADE CONSTRAINTS;
CREATE TABLE taxrate (
  tx_id CHAR(4) NOT NULL,
  tx_name VARCHAR2(50) NOT NULL,
  tx_rate DECIMAL(6,5) NOT NULL CHECK (tx_rate >= 0)
);

DROP TABLE zip_code CASCADE CONSTRAINTS;
CREATE TABLE zip_code (
  zc_code CHAR(12) NOT NULL,
  zc_town VARCHAR2(80) NOT NULL,
  zc_div VARCHAR2(80) NOT NULL
);


