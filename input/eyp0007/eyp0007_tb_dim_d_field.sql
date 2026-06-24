CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_field]
(
  [id_vers] [nvarchar] (4000) NOT NULL,
  [id_field] [nvarchar] (4000) NOT NULL,
  [des_field] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [cod_master_sys] [nvarchar] (4000),
  [des_master_sys] [nvarchar] (4000),
  [des_field_long] [nvarchar] (4000),
  [id_geo_area] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime],
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO