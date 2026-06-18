CREATE TABLE [sch_anl].[eyp0007_tb_fac_m_str_der_ngl]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_stream] [nvarchar] (4000) NOT NULL,
  [id_stream] [nvarchar] (4000) NOT NULL,
  [des_stream_name] [nvarchar] (4000) NOT NULL,
  [id_fcty_class_1] [nvarchar] (4000) NOT NULL,
  [id_col_point] [nvarchar] (4000) NOT NULL,
  [id_productionunit] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_operator_route] [nvarchar] (4000) NOT NULL,
  [id_product] [nvarchar] (4000) NOT NULL,
  [id_licence] [nvarchar] (4000) NOT NULL,
  [id_disposition_type] [nvarchar] (4000) NOT NULL,
  [ind_grs_vol_stb] [float],
  [ind_grs_vol_sm3] [float],
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = HASH(id_stream),
	CLUSTERED COLUMNSTORE INDEX
)
GO