CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_chem_tank]
(
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_chem_tank] [nvarchar] (4000) NOT NULL,
  [des_chem_tank] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [id_col_point] [nvarchar] (4000),
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