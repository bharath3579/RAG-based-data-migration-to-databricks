CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_resv_block]
( 
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_reservoir_block] [nvarchar] (4000) NOT NULL,
  [des_reservoir_block] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO