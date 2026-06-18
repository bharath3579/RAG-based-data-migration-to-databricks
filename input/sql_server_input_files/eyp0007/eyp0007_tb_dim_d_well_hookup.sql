CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_well_hookup]
( 
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_well_hookup] [nvarchar] (4000) NOT NULL,
  [des_well_hookup] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
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