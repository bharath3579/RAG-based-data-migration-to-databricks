CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_wl_st_gas_inj]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_well] [nvarchar] (4000) NOT NULL,
  [id_well] [nvarchar] (4000) NOT NULL,
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
  [ind_avg_choke_size] [float],
  [des_choke_uom] [nvarchar] (4000),
  [des_inj_type] [nvarchar] (4000),
  [ind_calc_on_stream_hrs] [float],
  [ind_alloc_inj_vol_kscf] [float],
  [ind_alloc_inj_vol_mscf] [float],
  [ind_alloc_inj_vol_sm3] [float],
  [ind_annulus_press_barg] [float],
  [ind_annulus_press_psig] [float],
  [ind_avg_bh_press_barg] [float],
  [ind_avg_bh_press_psig] [float],
  [ind_avg_bh_temp_c] [float],
  [ind_avg_bh_temp_f] [float],
  [ind_avg_wh_press_barg] [float],
  [ind_avg_wh_press_psig] [float],
  [ind_avg_wh_temp_c] [float],
  [ind_avg_wh_temp_f] [float],
  [ind_inj_vol_kscf] [float],
  [ind_inj_vol_mscf] [float],
  [ind_inj_vol_sm3] [float],
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = HASH(id_well),
	CLUSTERED COLUMNSTORE INDEX
)
GO