CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_dev_surv]
( 
  [cod_vers_wellbore]	[nvarchar] (4000) NOT NULL,
  [cod_vers_well_string] [nvarchar] (4000) NOT NULL,
  [cod_vers_well] [nvarchar] (4000) NOT NULL,
  [cod_uwbi] [nvarchar] (4000),
  [val_station_md] [float],
  [val_station_tvd] [float],
  [val_station_dev] [float],
  [val_station_az] [float],
  [val_station_az_grid] [float],
  [val_station_x_coord] [float], 
  [val_station_y_coord] [float],
  [num_is_certified] [int] 
)
WITH
(
	DISTRIBUTION = HASH(cod_uwbi),
	CLUSTERED COLUMNSTORE INDEX
)
GO



