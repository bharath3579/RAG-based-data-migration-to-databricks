CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_basin]
(
  [id_vers] [nvarchar] (4000) NOT NULL,
  [id_basin] [nvarchar] (4000) NOT NULL,
  [des_basin] [nvarchar] (4000),
  [fec_create_date] [smalldatetime] NOT NULL,
  [fec_update_date] [smalldatetime] NOT NULL,
  [des_basin_abbv] [nvarchar] (4000),  
  [id_basin_code] [nvarchar] (4000),
  [id_parent] [nvarchar] (4000),
  [des_basin_type] [nvarchar] (4000)
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO