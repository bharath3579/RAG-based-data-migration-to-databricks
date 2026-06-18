CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_choke_model]
(
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_choke_model] [nvarchar] (4000) NOT NULL,
  [des_choke_model] [nvarchar] (4000),
  [fec_start_date] [datetime2] NOT NULL,
  [fec_end_date] [smalldatetime],
  [des_parent_choke_model] [nvarchar] (4000),
  [id_fcty_1] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO