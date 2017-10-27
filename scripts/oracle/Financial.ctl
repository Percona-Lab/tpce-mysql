OPTIONS (DIRECT=TRUE)
LOAD DATA
INFILE '../../flat_out/Financial.txt'
APPEND
INTO TABLE financial
FIELDS TERMINATED BY '|'
(
  fi_co_id,
  fi_year,
  fi_qtr,
  fi_qtr_start_date DATE "YYYY-MM-DD",
  fi_revenue,
  fi_net_earn,
  fi_basic_eps,
  fi_dilut_eps,
  fi_margin,
  fi_inventory,
  fi_assets,
  fi_liability,
  fi_out_basic,
  fi_out_dilut
)
