CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_tank_inv_oil]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_tank] [nvarchar] (4000) NOT NULL,
  [id_tank] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_fcty_class_1] [nvarchar] (4000) NOT NULL,
  [id_oper_route] [nvarchar] (4000) NOT NULL,
  [id_prod_unit] [nvarchar] (4000) NOT NULL,
  [id_storage] [nvarchar] (4000) NOT NULL,
  [ind_calc_closing_grs_vol_stb] [float],
  [ind_calc_closing_grs_vol_sm3] [float],
  [ind_calc_closing_oil_vol_stb] [float],
  [ind_calc_closing_oil_vol_sm3] [float],
  [ind_calc_closing_wat_vol_bbl] [float],
  [ind_calc_closing_wat_vol_m3] [float],
  [ind_calc_closing_wat_vol_stb] [float],
  [ind_closing_grs_vol_stb] [float],
  [ind_closing_grs_vol_sm3] [float],
  [ind_closing_water_vol_bbl] [float],
  [ind_closing_water_vol_m3] [float],
  [ind_closing_water_vol_stb] [float],
  [ind_opening_grs_vol_stb] [float],
  [ind_opening_grs_vol_sm3] [float],
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
  DISTRIBUTION = HASH(id_tank),
  CLUSTERED COLUMNSTORE INDEX
)
GO