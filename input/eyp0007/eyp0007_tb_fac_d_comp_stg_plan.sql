CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_comp_stg_plan]
(
  [cod_vers_wellbore] [nvarchar] (4000) NOT NULL,
  [cod_vers_well_string] [nvarchar] (4000) NOT NULL,
  [cod_vers_well] [nvarchar] (4000) NOT NULL,
  [cod_vers] [nvarchar] (4000),
  [cod_uwbi] [varchar] (26),
  [cod_uwbi_num_stage] [varchar] (4000),
  [num_stage] [int],
  [id_asset] [int],
  [val_interval_top] [float],
  [val_interval_base] [float],
  [fec_time_start] [datetime],
  [val_stage_slickwater_volume] [float],
  [val_stage_100mesh_sand_volume] [float],
  [val_stage_40_70_sand_volume] [float],
  [val_stage_30_50_sand_volume] [float],
  [val_stage_20_40_sand_volume] [float],
  [val_fresh_water] [float],
  [val_15hcl_acid] [float]
)
WITH
(
  DISTRIBUTION = HASH(cod_vers),
  CLUSTERED COLUMNSTORE INDEX
)
GO