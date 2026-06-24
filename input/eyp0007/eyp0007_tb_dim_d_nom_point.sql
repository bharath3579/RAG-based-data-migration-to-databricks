CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_nom_point]
(
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_nom_point] [nvarchar] (4000) NOT NULL,
  [des_nom_point] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [des_location] [nvarchar] (4000),
  [des_uom] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO