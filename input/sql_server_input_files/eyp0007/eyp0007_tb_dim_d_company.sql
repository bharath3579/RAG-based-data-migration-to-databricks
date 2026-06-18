CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_company]
(
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_company] [nvarchar] (4000) NOT NULL,
  [des_company] [nvarchar] (4000),
  [des_official_company] [nvarchar] (4000),
  [fec_start_date] [datetime2] NOT NULL,
  [fec_end_date] [datetime2],
  [fec_create_date] [datetime2],
  [fec_update_date] [datetime2]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO
