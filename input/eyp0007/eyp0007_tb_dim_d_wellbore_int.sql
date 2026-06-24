CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_wellbore_int]
( 
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_well_bore_interval] [nvarchar] (4000) NOT NULL,
  [des_well_bore_interval] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [des_type_interval] [nvarchar] (4000),
  [id_well_bore] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO