CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_str_der_gas]
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
  [ind_alloc_net_vol_mscf] [float],
  [ind_alloc_net_vol_kscf] [float],
  [ind_alloc_net_vol_sm3] [float],
  [ind_grs_vol_gas_mscf] [float],
  [ind_grs_vol_gas_kscf] [float],
  [ind_grs_vol_gas_sm3] [float],
  [ind_net_vol_gas_mscf] [float],
  [ind_net_vol_gas_kscf] [float],
  [ind_net_vol_gas_sm3] [float],
  [ind_o_alloc_gas_vol_mscf] [float],
  [ind_o_alloc_gas_vol_sm3] [float],
  [ind_o_alloc_gas_vol_kscf] [float],
  [ind_o_alloc_gas_vol_15_ksm3_15] [float],
  [ind_o_alloc_gas_vol_15_kscf] [float],
  [ind_p_alloc_gas_vol_mscf] [float],
  [ind_p_alloc_gas_vol_sm3] [float],
  [ind_p_alloc_gas_vol_kscf] [float],
  [ind_p_alloc_gas_vol_15_ksm3_15] [float],
  [ind_p_alloc_gas_vol_15_kscf] [float],
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = HASH(id_stream),
	CLUSTERED COLUMNSTORE INDEX
)
GO