CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_comp_stg_actual]
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
  [fec_time_end] [datetime],
  [val_pump_duration] [float],
  [val_prefrac_static_wh_press] [float],
  [val_perfrac_isip] [float],
  [val_postfrac_isip] [float],
  [avg_treating_pressure] [float],
  [max_treating_pressure] [float],
  [val_breakdown_pressure] [float],
  [avg_pump_rate] [float],
  [max_pump_rate] [float],
  [val_stage_acid_volume] [float],
  [val_stage_slurry_volume] [float],
  [val_stage_clean_fluid] [float],
  [val_stage_slickwater_volume] [float],
  [val_stage_linear_gel_volume] [float],
  [val_stage_crosslnk_gel_volume] [float],
  [val_stage_100mesh_sand_volume] [float],
  [val_stage_40_70_sand_volume] [float],
  [val_stage_30_50_sand_volume] [float],
  [val_stage_20_40_sand_volume] [float],
  [val_total_proppant] [float],
  [val_fresh_water] [float],
  [val_15hcl_acid] [float],
  [val_time_between_stg] [float],
  [val_press_decline_btw_stg] [float],
  [val_rt_press_decline_btw_stg] [float]
)
WITH
(
  DISTRIBUTION = HASH(cod_vers),
  CLUSTERED COLUMNSTORE INDEX
)
GO