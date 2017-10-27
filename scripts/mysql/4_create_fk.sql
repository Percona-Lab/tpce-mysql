SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

ALTER TABLE account_permission ADD CONSTRAINT fk_account_permission_ca
 FOREIGN KEY (ap_ca_id) REFERENCES customer_account (ca_id);
ALTER TABLE customer ADD CONSTRAINT fk_customer_st
 FOREIGN KEY (c_st_id) REFERENCES status_type (st_id);
ALTER TABLE customer ADD CONSTRAINT fk_customer_ad
 FOREIGN KEY (c_ad_id) REFERENCES address (ad_id);
ALTER TABLE customer_account ADD CONSTRAINT fk_customer_account_b
 FOREIGN KEY (ca_b_id) REFERENCES broker (b_id);
ALTER TABLE customer_account ADD CONSTRAINT fk_customer_account_c
 FOREIGN KEY (ca_c_id) REFERENCES customer (c_id);
ALTER TABLE customer_taxrate ADD CONSTRAINT fk_customer_taxrate_tx
 FOREIGN KEY (cx_tx_id) REFERENCES taxrate (tx_id);
ALTER TABLE customer_taxrate ADD CONSTRAINT fk_customer_taxrate_c
 FOREIGN KEY (cx_c_id) REFERENCES customer (c_id);
ALTER TABLE holding ADD CONSTRAINT fk_holding_t
 FOREIGN KEY (h_t_id) REFERENCES trade (t_id);
ALTER TABLE holding ADD CONSTRAINT fk_holding_hs
 FOREIGN KEY (h_ca_id, h_s_symb) REFERENCES holding_summary (hs_ca_id, hs_s_symb);
ALTER TABLE holding_history ADD CONSTRAINT fk_holding_history_t1
 FOREIGN KEY (hh_h_t_id) REFERENCES trade (t_id);
ALTER TABLE holding_history ADD CONSTRAINT fk_holding_history_t2
 FOREIGN KEY (hh_t_id) REFERENCES trade (t_id);
ALTER TABLE holding_summary ADD CONSTRAINT fk_holding_summary_ca
 FOREIGN KEY (hs_ca_id) REFERENCES customer_account (ca_id);
ALTER TABLE holding_summary ADD CONSTRAINT fk_holding_summary_s
 FOREIGN KEY (hs_s_symb) REFERENCES security (s_symb);
ALTER TABLE watch_item ADD CONSTRAINT fk_watch_item_wl
 FOREIGN KEY (wi_wl_id) REFERENCES watch_list (wl_id);
ALTER TABLE watch_item ADD CONSTRAINT fk_watch_item_s
 FOREIGN KEY (wi_s_symb) REFERENCES security (s_symb);
ALTER TABLE watch_list ADD CONSTRAINT fk_watch_list_c
 FOREIGN KEY (wl_c_id) REFERENCES customer (c_id);

ALTER TABLE broker ADD CONSTRAINT fk_broker_st
 FOREIGN KEY (b_st_id) REFERENCES status_type (st_id);
ALTER TABLE cash_transaction ADD CONSTRAINT fk_cash_transaction_t
 FOREIGN KEY (ct_t_id) REFERENCES trade (t_id);
ALTER TABLE charge ADD CONSTRAINT fk_charge_tt
 FOREIGN KEY (ch_tt_id) REFERENCES trade_type (tt_id);
ALTER TABLE commission_rate ADD CONSTRAINT fk_commission_rate_tt
 FOREIGN KEY (cr_tt_id) REFERENCES trade_type (tt_id);
ALTER TABLE commission_rate ADD CONSTRAINT fk_commission_rate_ex
 FOREIGN KEY (cr_ex_id) REFERENCES exchange (ex_id);
ALTER TABLE settlement ADD CONSTRAINT fk_settlement_t
 FOREIGN KEY (se_t_id) REFERENCES trade (t_id);
ALTER TABLE trade ADD CONSTRAINT fk_trade_st
 FOREIGN KEY (t_st_id) REFERENCES status_type (st_id);
ALTER TABLE trade ADD CONSTRAINT fk_trade_tt
 FOREIGN KEY (t_tt_id) REFERENCES trade_type (tt_id);
ALTER TABLE trade ADD CONSTRAINT fk_trade_s
 FOREIGN KEY (t_s_symb) REFERENCES security (s_symb);
ALTER TABLE trade ADD CONSTRAINT fk_trade_ca
 FOREIGN KEY (t_ca_id) REFERENCES customer_account (ca_id);
ALTER TABLE trade_history ADD CONSTRAINT fk_trade_history_t
 FOREIGN KEY (th_t_id) REFERENCES trade (t_id);
ALTER TABLE trade_history ADD CONSTRAINT fk_trade_history_st
 FOREIGN KEY (th_st_id) REFERENCES status_type (st_id);
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_request_t
 FOREIGN KEY (tr_t_id) REFERENCES trade (t_id);
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_request_tt
 FOREIGN KEY (tr_tt_id) REFERENCES trade_type (tt_id);
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_request_s
 FOREIGN KEY (tr_s_symb) REFERENCES security (s_symb);
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_request_b
 FOREIGN KEY (tr_b_id) REFERENCES broker (b_id);

ALTER TABLE company ADD CONSTRAINT fk_company_st
 FOREIGN KEY (co_st_id) REFERENCES status_type (st_id);
ALTER TABLE company ADD CONSTRAINT fk_company_in
 FOREIGN KEY (co_in_id) REFERENCES industry (in_id);
ALTER TABLE company ADD CONSTRAINT fk_company_ad
 FOREIGN KEY (co_ad_id) REFERENCES address (ad_id);
ALTER TABLE company_competitor ADD CONSTRAINT fk_company_competitor_co1
 FOREIGN KEY (cp_co_id) REFERENCES company (co_id);
ALTER TABLE company_competitor ADD CONSTRAINT fk_company_competitor_co2
 FOREIGN KEY (cp_comp_co_id) REFERENCES company (co_id);
ALTER TABLE company_competitor ADD CONSTRAINT fk_company_competitor_in
 FOREIGN KEY (cp_in_id) REFERENCES industry (in_id);
ALTER TABLE daily_market ADD CONSTRAINT fk_daily_market_s
 FOREIGN KEY (dm_s_symb) REFERENCES security (s_symb);
ALTER TABLE exchange ADD CONSTRAINT fk_exchange_ad
 FOREIGN KEY (ex_ad_id) REFERENCES address (ad_id);
ALTER TABLE financial ADD CONSTRAINT fk_financial_co
 FOREIGN KEY (fi_co_id) REFERENCES company (co_id);
ALTER TABLE industry ADD CONSTRAINT fk_industry_sc
 FOREIGN KEY (in_sc_id) REFERENCES sector (sc_id);
ALTER TABLE last_trade ADD CONSTRAINT fk_last_trade_s
 FOREIGN KEY (lt_s_symb) REFERENCES security (s_symb);
ALTER TABLE news_xref ADD CONSTRAINT fk_news_xref_ni
 FOREIGN KEY (nx_ni_id) REFERENCES news_item (ni_id);
ALTER TABLE news_xref ADD CONSTRAINT fk_news_xref_co
 FOREIGN KEY (nx_co_id) REFERENCES company (co_id);
ALTER TABLE security ADD CONSTRAINT fk_security_st
 FOREIGN KEY (s_st_id) REFERENCES status_type (st_id);
ALTER TABLE security ADD CONSTRAINT fk_security_ex
 FOREIGN KEY (s_ex_id) REFERENCES exchange (ex_id);
ALTER TABLE security ADD CONSTRAINT fk_security_co
 FOREIGN KEY (s_co_id) REFERENCES company (co_id);

ALTER TABLE address ADD CONSTRAINT fk_address_zc
 FOREIGN KEY (ad_zc_code) REFERENCES zip_code (zc_code);


SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

