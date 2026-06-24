CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_com_perf]
( 
  [cod_vers_wellbore]	[nvarchar] (4000) NOT NULL,
  [cod_vers_well_string] [nvarchar] (4000) NOT NULL,
  [cod_vers_well] [nvarchar] (4000) NOT NULL,
  [cod_uwbi] [nvarchar] (4000) ,
  [cod_uwbi_num_stage] [varchar] (4000),
  [num_stage] [int],
  [val_perf_top] [float],
  [val_perf_base] [float],
  [val_perf_orientation] [float] ,
  [val_diameter] [float] ,
  [val_charge_weight] [float], 
  [des_charge] [nvarchar] (100) ,
  [num_shot_density] [int] ,
  [val_carrier_weight] [float] ,
  [des_carrier] [nvarchar] (100) ,
  [fec_interval_shot] [datetime2] 
)
WITH
(
	DISTRIBUTION = HASH(cod_uwbi),
	CLUSTERED COLUMNSTORE INDEX
)
GO


