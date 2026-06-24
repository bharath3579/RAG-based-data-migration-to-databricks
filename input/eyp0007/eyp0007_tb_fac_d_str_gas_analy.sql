CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_str_gas_analy]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_stream] [nvarchar] (4000) NOT NULL,
  [id_stream] [nvarchar] (4000) NOT NULL,
  [id_fcty_class_1] [nvarchar] (4000) NOT NULL,
  [id_col_point] [nvarchar] (4000) NOT NULL,
  [id_productionunit] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_operator_route] [nvarchar] (4000) NOT NULL,
  [id_product] [nvarchar] (4000) NOT NULL,
  [id_licence] [nvarchar] (4000) NOT NULL,
  [id_disposition_type] [nvarchar] (4000) NOT NULL,
  [fec_lab_date] [smalldatetime],
  [fec_start_date] [smalldatetime],
  [des_analysis_status] [nvarchar] (4000),
  [ind_analysis_no] [int],
  [des_phase] [nvarchar] (4000),
  [ind_rel_density] [float],
  [ind_sp_grav] [float],
  [fec_valid_from] [smalldatetime],
  [ind_calc_gcv_mmbtuperkscf] [float],
  [ind_calc_gcv_btuperscf] [float],
  [ind_calc_gcv_mjpersm3] [float],
  [ind_density_lbsperkscf] [float],
  [ind_density_kgpersm3] [float],
  [ind_gcr_sm3persm3] [float],
  [ind_gcr_kscfperstb] [float],
  [ind_gcr_scfperstb] [float],
  [ind_gcr_scfperbbls] [float],
  [ind_gcr_mscfperbbls] [float],
  [ind_gcv_mmbtuperkscf] [float],
  [ind_gcv_btuperscf] [float],
  [ind_gcv_mjpersm3] [float],
  [ind_mol_wt_gpermol] [float],
  [ind_o2_ppmv] [float],
  [ind_o2_ppm] [float],
  [ind_sg_mix_api] [float],
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = HASH(id_stream),
	CLUSTERED COLUMNSTORE INDEX
)
GO