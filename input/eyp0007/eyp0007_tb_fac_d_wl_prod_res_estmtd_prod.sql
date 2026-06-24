CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_wl_prod_res_estmtd_prod]
(
  [fec_production_day] [datetime2] NOT NULL,
  [id_vers_well] [nvarchar] (4000) NOT NULL,
  [id_well] [nvarchar] (4000) NOT NULL,
  [ind_gas_rate_adj_mscfperday] [float],
  [ind_gas_rate_adj_kscfperday] [float],
  [ind_gas_rate_adj_sm3perday] [float],
  [ind_net_cond_rate_adj_stbperday] [float],
  [ind_net_cond_rate_adj_sm3perday] [float],
  [ind_net_oil_rate_adj_stbperday] [float],
  [ind_net_oil_rate_adj_sm3perday] [float],
  [ind_tot_water_rate_adj_bblperday] [float],
  [ind_tot_water_rate_adj_m3perday] [float],
  [id_commercial_entity] [nvarchar] (4000) NOT NULL,
  [id_facility_class_1] [nvarchar] (4000) NOT NULL,
  [id_well_hookup] [nvarchar] (4000) NOT NULL,
  [id_well_hole] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_productionunit] [nvarchar] (4000) NOT NULL,
  [id_geo_area] [nvarchar] (4000) NOT NULL,
  [id_field_cds] [nvarchar] (4000) NOT NULL,
  [id_field] [nvarchar] (4000) NOT NULL,
  [id_basin] [nvarchar] (4000) NOT NULL,
  [id_operator_route] [nvarchar] (4000) NOT NULL,
  [id_col_point] [nvarchar] (4000) NOT NULL,
  [id_licence] [nvarchar] (4000) NOT NULL,
  [fec_end_date] [smalldatetime],
  [fec_valid_from] [smalldatetime],
  [ind_duration] [float],
  [des_status] [nvarchar] (4000),
  [ind_result_no] [varchar] (8000) NOT NULL,
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = HASH(id_well),
	CLUSTERED COLUMNSTORE INDEX
)
GO