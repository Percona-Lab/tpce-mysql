CREATE INDEX i_c_tax_id ON customer (c_tax_id);
CREATE INDEX i_co_name ON company (co_name);

CREATE INDEX i_th_t_id_dts ON trade_history (th_t_id, th_dts);
CREATE INDEX i_in_name ON industry (in_name);
CREATE INDEX i_sc_name ON sector (sc_name);
CREATE INDEX i_t_ca_id_dts ON trade (t_ca_id, t_dts);
CREATE INDEX i_t_s_symb_dts ON trade (t_s_symb, t_dts);
CREATE INDEX i_h_ca_id_s_symb_dts ON holding (h_ca_id, h_s_symb, h_dts);

