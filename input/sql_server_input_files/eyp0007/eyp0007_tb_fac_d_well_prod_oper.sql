CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_well_prod_oper]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_well] [nvarchar] (4000) NOT NULL,
  [id_well] [nvarchar] (4000) NOT NULL,
  [id_well_hole] [nvarchar] (4000) NOT NULL,
  [ind_alloc_cond_vol_stb] [float] ,
  [ind_alloc_gas_vol_mscf] [float] ,
  [ind_alloc_oil_vol_stb] [float] ,
  [ind_alloc_water_vol_bbl] [float] ,
  [ind_avg_bh_press_psig] [float] , 
  [ind_avg_choke_size] [float] ,
  [ind_annulus_press_psig] [float] ,
  [ind_avg_wh_temp_2_f] [float] ,
  [ind_avg_wh_press_psig] [float],
  [ind_avg_wh_dsc_press_psig] [float],
  [ind_avg_wh_temp_f] [float],
  [ind_avg_wh_dsc_temp_f] [float],
  [fec_create_date] [smalldatetime] ,
  [fec_update_date] [smalldatetime]
 
)
WITH
(
  DISTRIBUTION = HASH(id_well),
  CLUSTERED COLUMNSTORE INDEX
)
GO


