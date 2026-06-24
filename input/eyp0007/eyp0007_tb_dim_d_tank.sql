CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_tank]
(
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_tank] [nvarchar] (4000) NOT NULL,
  [des_tank] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [des_type_tank] [nvarchar] (4000),  
  [ind_max_vol] [float],
  [ind_max_vol_safe_lim] [float],
  [ind_min_vol] [float],
  [ind_min_vol_safe_lim] [float],
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