SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;



DROP TABLE IF EXISTS account_permission;
CREATE TABLE account_permission (
  ap_ca_id BIGINT(12) NOT NULL,
  ap_acl CHAR(4) NOT NULL,
  ap_tax_id VARCHAR(20) NOT NULL,
  ap_l_name VARCHAR(25) NOT NULL,
  ap_f_name VARCHAR(20) NOT NULL,
  PRIMARY KEY (ap_ca_id, ap_tax_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
  c_id BIGINT(12) NOT NULL,
  c_tax_id VARCHAR(20) NOT NULL,
  c_st_id CHAR(4) NOT NULL,
  c_l_name VARCHAR(25) NOT NULL,
  c_f_name VARCHAR(20) NOT NULL,
  c_m_name CHAR(1),
  c_gndr CHAR(1),
  c_tier TINYINT(1) NOT NULL,
  c_dob DATE NOT NULL,
  c_ad_id BIGINT(12) NOT NULL,
  c_ctry_1 VARCHAR(3),
  c_area_1 VARCHAR(3),
  c_local_1 VARCHAR(10),
  c_ext_1 VARCHAR(5),
  c_ctry_2 VARCHAR(3),
  c_area_2 VARCHAR(3),
  c_local_2 VARCHAR(10),
  c_ext_2 VARCHAR(5),
  c_ctry_3 VARCHAR(3),
  c_area_3 VARCHAR(3),
  c_local_3 VARCHAR(10),
  c_ext_3 VARCHAR(5),
  c_email_1 VARCHAR(50),
  c_email_2 VARCHAR(50),
  PRIMARY KEY (c_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS customer_account;
CREATE TABLE customer_account (
  ca_id BIGINT(12) NOT NULL,
  ca_b_id BIGINT(12) NOT NULL,
  ca_c_id BIGINT(12) NOT NULL,
  ca_name VARCHAR(50),
  ca_tax_st TINYINT(1) NOT NULL,
  ca_bal DECIMAL(12,2) NOT NULL,
  PRIMARY KEY (ca_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS customer_taxrate;
CREATE TABLE customer_taxrate (
  cx_tx_id CHAR(4) NOT NULL,
  cx_c_id BIGINT(12) NOT NULL,
  PRIMARY KEY (cx_c_id, cx_tx_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS holding;
CREATE TABLE holding (
  h_t_id BIGINT(16) NOT NULL,
  h_ca_id BIGINT(12) NOT NULL,
  h_s_symb CHAR(15) NOT NULL,
  h_dts DATETIME NOT NULL,
  h_price DECIMAL(8,2) NOT NULL CHECK (h_price > 0),
  h_qty MEDIUMINT(7) NOT NULL,
  PRIMARY KEY (h_t_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS holding_history;
CREATE TABLE holding_history (
  hh_h_t_id BIGINT(16) NOT NULL,
  hh_t_id BIGINT(16) NOT NULL,
  hh_before_qty MEDIUMINT(7) NOT NULL,
  hh_after_qty MEDIUMINT(7) NOT NULL,
  PRIMARY KEY (hh_t_id, hh_h_t_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS holding_summary;
CREATE TABLE holding_summary (
  hs_ca_id BIGINT(12) NOT NULL,
  hs_s_symb CHAR(15) NOT NULL,
  hs_qty MEDIUMINT(7) NOT NULL,
  PRIMARY KEY (hs_ca_id, hs_s_symb)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS watch_item;
CREATE TABLE watch_item (
  wi_wl_id BIGINT(12) NOT NULL,
  wi_s_symb CHAR(15) NOT NULL,
  PRIMARY KEY (wi_wl_id, wi_s_symb)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS watch_list;
CREATE TABLE watch_list (
  wl_id BIGINT(12) NOT NULL,
  wl_c_id BIGINT(12) NOT NULL,
  PRIMARY KEY (wl_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;




DROP TABLE IF EXISTS broker;
CREATE TABLE broker (
  b_id BIGINT(12) NOT NULL,
  b_st_id CHAR(4) NOT NULL,
  b_name VARCHAR(49) NOT NULL,
  b_num_trades INT(10) NOT NULL,
  b_comm_total DECIMAL(12,2) NOT NULL,
  PRIMARY KEY (b_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS cash_transaction;
CREATE TABLE cash_transaction (
  ct_t_id BIGINT(16) NOT NULL,
  ct_dts DATETIME NOT NULL,
  ct_amt DECIMAL(10,2) NOT NULL,
  ct_name VARCHAR(100),
  PRIMARY KEY (ct_t_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS charge;
CREATE TABLE charge (
  ch_tt_id CHAR(3) NOT NULL,
  ch_c_tier TINYINT(1) NOT NULL,
  ch_chrg DECIMAL(10,2) NOT NULL CHECK (ch_chrg > 0),
  PRIMARY KEY (ch_tt_id, ch_c_tier)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS commission_rate;
CREATE TABLE commission_rate (
  cr_c_tier TINYINT(1) NOT NULL,
  cr_tt_id CHAR(3) NOT NULL,
  cr_ex_id CHAR(6) NOT NULL,
  cr_from_qty MEDIUMINT(7) NOT NULL CHECK (cr_from_qty >= 0),
  cr_to_qty MEDIUMINT(7) NOT NULL CHECK (cr_to_qty > cr_from_qty),
  cr_rate DECIMAL(5,2) NOT NULL CHECK (cr_rate >= 0),
  PRIMARY KEY (cr_c_tier, cr_tt_id, cr_ex_id, cr_from_qty)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS settlement;
CREATE TABLE settlement (
  se_t_id BIGINT(16) NOT NULL,
  se_cash_type VARCHAR(40) NOT NULL,
  se_cash_due_date DATE NOT NULL,
  se_amt DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (se_t_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS trade;
CREATE TABLE trade (
  t_id BIGINT(16) NOT NULL,
  t_dts DATETIME NOT NULL,
  t_st_id CHAR(4) NOT NULL,
  t_tt_id CHAR(3) NOT NULL,
  t_is_cash TINYINT(1) NOT NULL,
  t_s_symb CHAR(15) NOT NULL,
  t_qty MEDIUMINT(7) NOT NULL CHECK (t_qty > 0),
  t_bid_price DECIMAL(8,2) NOT NULL CHECK (t_bid_price > 0),
  t_ca_id BIGINT(12) NOT NULL,
  t_exec_name VARCHAR(49) NOT NULL,
  t_trade_price DECIMAL(8,2),
  t_chrg DECIMAL(10,2) NOT NULL CHECK (t_chrg >= 0),
  t_comm DECIMAL(10,2) NOT NULL CHECK (t_comm >= 0),
  t_tax DECIMAL(10,2) NOT NULL CHECK (t_tax >= 0),
  t_lifo TINYINT(1) NOT NULL,
  PRIMARY KEY (t_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS trade_history;
CREATE TABLE trade_history (
  th_t_id BIGINT(16) NOT NULL,
  th_dts DATETIME NOT NULL,
  th_st_id CHAR(4) NOT NULL,
  PRIMARY KEY (th_t_id, th_st_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS trade_request;
CREATE TABLE trade_request (
  tr_t_id BIGINT(16) NOT NULL,
  tr_tt_id CHAR(3) NOT NULL,
  tr_s_symb CHAR(15) NOT NULL,
  tr_qty MEDIUMINT(7) NOT NULL CHECK (tr_qty > 0),
  tr_bid_price DECIMAL(8,2) NOT NULL CHECK (tr_bid_price > 0),
  tr_b_id BIGINT(12) NOT NULL,
  PRIMARY KEY (tr_t_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS trade_type;
CREATE TABLE trade_type (
  tt_id CHAR(3) NOT NULL,
  tt_name VARCHAR(12) NOT NULL,
  tt_is_sell TINYINT(1) NOT NULL,
  tt_is_mrkt TINYINT(1) NOT NULL,
  PRIMARY KEY (tt_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;





DROP TABLE IF EXISTS company;
CREATE TABLE company (
  co_id BIGINT(12) NOT NULL,
  co_st_id CHAR(4) NOT NULL,
  co_name VARCHAR(60) NOT NULL,
  co_in_id CHAR(2) NOT NULL,
  co_sp_rate CHAR(4) NOT NULL,
  co_ceo VARCHAR(46) NOT NULL,
  co_ad_id BIGINT(12) NOT NULL,
  co_desc VARCHAR(150) NOT NULL,
  co_open_date DATE NOT NULL,
  PRIMARY KEY (co_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS company_competitor;
CREATE TABLE company_competitor (
  cp_co_id BIGINT(12) NOT NULL,
  cp_comp_co_id BIGINT(12) NOT NULL,
  cp_in_id CHAR(2) NOT NULL,
  PRIMARY KEY (cp_co_id, cp_comp_co_id, cp_in_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS daily_market;
CREATE TABLE daily_market (
  dm_date DATE NOT NULL,
  dm_s_symb CHAR(15) NOT NULL,
  dm_close DECIMAL(8,2) NOT NULL,
  dm_high DECIMAL(8,2) NOT NULL,
  dm_low DECIMAL(8,2) NOT NULL,
  dm_vol BIGINT(13) NOT NULL,
  PRIMARY KEY (dm_s_symb, dm_date)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS exchange;
CREATE TABLE exchange (
  ex_id CHAR(6) NOT NULL,
  ex_name VARCHAR(100) NOT NULL,
  ex_num_symb MEDIUMINT(7) NOT NULL,
  ex_open SMALLINT(5) NOT NULL,
  ex_close SMALLINT(5) NOT NULL,
  ex_desc VARCHAR(150),
  ex_ad_id BIGINT(12) NOT NULL,
  PRIMARY KEY (ex_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS financial;
CREATE TABLE financial (
  fi_co_id BIGINT(12) NOT NULL,
  fi_year SMALLINT(5) NOT NULL,
  fi_qtr TINYINT(2) NOT NULL,
  fi_qtr_start_date DATE NOT NULL,
  fi_revenue DECIMAL(15,2) NOT NULL,
  fi_net_earn DECIMAL(15,2) NOT NULL,
  fi_basic_eps DECIMAL(10,2) NOT NULL,
  fi_dilut_eps DECIMAL(10,2) NOT NULL,
  fi_margin DECIMAL(10,2) NOT NULL,
  fi_inventory DECIMAL(15,2) NOT NULL,
  fi_assets DECIMAL(15,2) NOT NULL,
  fi_liability DECIMAL(15,2) NOT NULL,
  fi_out_basic BIGINT(13) NOT NULL,
  fi_out_dilut BIGINT(13) NOT NULL,
  PRIMARY KEY (fi_co_id, fi_year, fi_qtr)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS industry;
CREATE TABLE industry (
  in_id CHAR(2) NOT NULL,
  in_name VARCHAR(50) NOT NULL,
  in_sc_id CHAR(2) NOT NULL,
  PRIMARY KEY (in_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS last_trade;
CREATE TABLE last_trade (
  lt_s_symb CHAR(15) NOT NULL,
  lt_dts DATETIME NOT NULL,
  lt_price DECIMAL(8,2) NOT NULL,
  lt_open_price DECIMAL(8,2) NOT NULL,
  lt_vol BIGINT(13) NOT NULL,
  PRIMARY KEY (lt_s_symb)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;





DROP TABLE IF EXISTS news_item;
CREATE TABLE news_item (
  ni_id BIGINT(12) NOT NULL,
  ni_headline VARCHAR(80) NOT NULL,
  ni_summary VARCHAR(255) NOT NULL,
  ni_item MEDIUMBLOB NOT NULL,
  ni_dts DATETIME NOT NULL,
  ni_source VARCHAR(30) NOT NULL,
  ni_author VARCHAR(30),
  PRIMARY KEY (ni_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS news_xref;
CREATE TABLE news_xref (
  nx_ni_id BIGINT(12) NOT NULL,
  nx_co_id BIGINT(12) NOT NULL,
  PRIMARY KEY (nx_co_id, nx_ni_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS sector;
CREATE TABLE sector (
  sc_id CHAR(2) NOT NULL,
  sc_name VARCHAR(30) NOT NULL,
  PRIMARY KEY (sc_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS security;
CREATE TABLE security (
  s_symb CHAR(15) NOT NULL,
  s_issue CHAR(6) NOT NULL,
  s_st_id CHAR(4) NOT NULL,
  s_name VARCHAR(70) NOT NULL,
  s_ex_id CHAR(6) NOT NULL,
  s_co_id BIGINT(12) NOT NULL,
  s_num_out BIGINT(13) NOT NULL,
  s_start_date DATE NOT NULL,
  s_exch_date DATE NOT NULL,
  s_pe DECIMAL(10,2) NOT NULL,
  s_52wk_high DECIMAL(8,2) NOT NULL,
  s_52wk_high_date DATE NOT NULL,
  s_52wk_low DECIMAL(8,2) NOT NULL,
  s_52wk_low_date DATE NOT NULL,
  s_dividend DECIMAL(10,2) NOT NULL,
  s_yield DECIMAL(5,2) NOT NULL,
  PRIMARY KEY (s_symb)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;





DROP TABLE IF EXISTS address;
CREATE TABLE address (
  ad_id BIGINT(12) NOT NULL,
  ad_line1 VARCHAR(80),
  ad_line2 VARCHAR(80),
  ad_zc_code CHAR(12) NOT NULL,
  ad_ctry VARCHAR(80),
  PRIMARY KEY (ad_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS status_type;
CREATE TABLE status_type (
  st_id CHAR(4) NOT NULL,
  st_name CHAR(10) NOT NULL,
  PRIMARY KEY (st_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS taxrate;
CREATE TABLE taxrate (
  tx_id CHAR(4) NOT NULL,
  tx_name VARCHAR(50) NOT NULL,
  tx_rate DECIMAL(6,5) NOT NULL CHECK (tx_rate >= 0),
  PRIMARY KEY (tx_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS zip_code;
CREATE TABLE zip_code (
  zc_code CHAR(12) NOT NULL,
  zc_town VARCHAR(80) NOT NULL,
  zc_div VARCHAR(80) NOT NULL,
  PRIMARY KEY (zc_code)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;




SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

