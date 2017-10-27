CREATE INDEX i_fk_customer_st ON customer
 (c_st_id);
CREATE INDEX i_fk_customer_ad ON customer
 (c_ad_id);
CREATE INDEX i_fk_customer_account_b ON customer_account
 (ca_b_id);
CREATE INDEX i_fk_customer_account_c ON customer_account
 (ca_c_id);
CREATE INDEX i_fk_customer_taxrate_tx ON customer_taxrate
 (cx_tx_id);
CREATE INDEX i_fk_holding_hs ON holding
 (h_ca_id, h_s_symb);
CREATE INDEX i_fk_holding_history_t1 ON holding_history
 (hh_h_t_id);
CREATE INDEX i_fk_holding_summary_s ON holding_summary
 (hs_s_symb);
CREATE INDEX i_fk_watch_item_s ON watch_item
 (wi_s_symb);
CREATE INDEX i_fk_watch_list_c ON watch_list
 (wl_c_id);
CREATE INDEX i_fk_broker_st ON broker
 (b_st_id);
CREATE INDEX i_fk_commission_rate_tt ON commission_rate
 (cr_tt_id);
CREATE INDEX i_fk_commission_rate_ex ON commission_rate
 (cr_ex_id);
CREATE INDEX i_fk_trade_st ON trade
 (t_st_id);
CREATE INDEX i_fk_trade_tt ON trade
 (t_tt_id);
CREATE INDEX i_fk_trade_history_st ON trade_history
 (th_st_id);
CREATE INDEX i_fk_trade_request_tt ON trade_request
 (tr_tt_id);
CREATE INDEX i_fk_trade_request_s ON trade_request
 (tr_s_symb);
CREATE INDEX i_fk_trade_request_b ON trade_request
 (tr_b_id);
CREATE INDEX i_fk_company_st ON company
 (co_st_id);
CREATE INDEX i_fk_company_in ON company
 (co_in_id);
CREATE INDEX i_fk_company_ad ON company
 (co_ad_id);
CREATE INDEX i_fk_company_competitor_co2 ON company_competitor
 (cp_comp_co_id);
CREATE INDEX i_fk_company_competitor_in ON company_competitor
 (cp_in_id);
CREATE INDEX i_fk_exchange_ad ON exchange
 (ex_ad_id);
CREATE INDEX i_fk_industry_sc ON industry
 (in_sc_id);
CREATE INDEX i_fk_news_xref_ni ON news_xref
 (nx_ni_id);
CREATE INDEX i_fk_security_st ON security
 (s_st_id);
CREATE INDEX i_fk_security_ex ON security
 (s_ex_id);
CREATE INDEX i_fk_security_co ON security
 (s_co_id);
CREATE INDEX i_fk_address_zc ON address
 (ad_zc_code);
