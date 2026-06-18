CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_well_prod_oper_effective]
(
  [fec_production_day] [datetime] NOT NULL,
  [cod_well] [nvarchar] (4000) NOT NULL,
  [cod_vers_wellbore] [nvarchar] (4000) NOT NULL,
  [val_ind_effective_cond_vol_stb] [float] ,
  [val_ind_effective_gas_vol_mscf] [float] ,
  [val_ind_effective_oil_vol_stb] [float] ,
  [val_ind_effective_water_vol_bbl] [float] ,
  [num_days] [int]
 
)
WITH
(
  DISTRIBUTION = HASH(cod_vers_wellbore),
  CLUSTERED COLUMNSTORE INDEX
)
GO