CREATE TABLE [sch_anl].[eyp0007_tb_fac_m_fld_plan_budg]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_field] [nvarchar] (4000) NOT NULL,
  [id_field] [nvarchar] (4000) NOT NULL,
  [id_geo_area] [nvarchar] (4000) NOT NULL,
  [ind_cond_rate_mth_kstbpermth] [float] ,
  [ind_cond_rate_mth_stbpermth] [float] ,
  [ind_cond_rate_mth_net_kstbpermth] [float] ,
  [ind_cond_rate_mth_net_stbpermth] [float] ,
  [ind_cond_rate_mth_wi_kstbpermth] [float] ,
  [ind_cond_rate_mth_wi_stbpermth] [float] ,
  [ind_gas_rate_mth_mmscfpermth] [float] ,
  [ind_gas_rate_mth_kscfpermth] [float] ,
  [ind_gas_rate_mth_net_mmscfpermth] [float] ,
  [ind_gas_rate_mth_net_kscfpermth] [float] ,
  [ind_gas_rate_mth_wi_mmscfpermth] [float] ,
  [ind_gas_rate_mth_wi_kscfpermth] [float] ,
  [ind_ngl_rate_mth_mscfperday] [float] ,
  [ind_ngl_rate_mth_kscfperday] [float] ,
  [ind_ngl_rate_mth_sm3perday] [float] ,
  [ind_ngl_rate_mth_kstbpermth] [float] ,
  [ind_ngl_rate_mth_stbpermth] [float] ,
  [ind_oil_rate_mth_kstbpermth] [float] ,
  [ind_oil_rate_mth_stbpermth] [float] ,
  [ind_oil_rate_mth_net_kstbpermth] [float] ,
  [ind_oil_rate_mth_net_stbpermth] [float] ,
  [ind_oil_rate_mth_wi_kstbpermth] [float] ,
  [ind_oil_rate_mth_wi_stbpermth] [float] ,
  [ind_water_rate_mth_kstbpermth] [float] ,
  [ind_water_rate_mth_stbpermth] [float] ,
  [ind_water_rate_mth_net_kstbpermth] [float] ,
  [ind_water_rate_mth_net_stbpermth] [float] ,
  [ind_water_rate_mth_wi_kstbpermth] [float] ,
  [ind_water_rate_mth_wi_stbpermth] [float] ,
  [fec_create_date] [smalldatetime] ,
  [fec_update_date] [smalldatetime] 
)
WITH
(
  DISTRIBUTION = HASH(id_field),
  CLUSTERED COLUMNSTORE INDEX
)
GO