CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_comp_stg_final]
( 
  [cod_vers_wellbore]	[nvarchar] (4000) NOT NULL,
  [cod_vers_well_string] [nvarchar] (4000) NOT NULL,
  [cod_vers_well] [nvarchar] (4000) NOT NULL,
  [cod_uwbi] [nvarchar] (4000) ,
  [cod_uwbi_num_stage] [varchar] (4000),
  [num_stage] [int],
  [val_interval_top] [float] ,
  [val_interval_base] [float] ,
  [fec_time_start] [datetime2],  
  [fec_time_end] [datetime2],
  [val_pump_duration] [float] ,
  [val_prefrac_static_wh_press] [float] ,
  [val_prefrac_isip] [float],
  [val_postfrac_isip] [float] ,
  [val_5min_isip] [float],
  [avg_treating_pressure] [float],
  [max_treating_pressure] [float],
  [val_breakdown_pressure] [float],
  [avg_pump_rate] [float],
  [max_pump_rate] [float],
  [val_stage_acid_volume] [float],
  [val_stage_slurry_volume] [float],
  [val_stage_clean_fluid] [float],
  [val_stage_100mesh_sand_volume] [float],
  [val_stage_40_70_sand_volume] [float],
  [val_stage_30_50_sand_volume] [float],
  [val_stage_20_40_sand_volume] [float],
  [val_total_proppant] [float],
  [val_fresh_water] [float],
  [val_flowback_water] [float],
  [val_stage_mid_perf_depth] [float],
  [val_stage_mid_perf_depth_tvd] [float],
  [val_stage_mid_perf_depth_abs_tvd] [float],
  [val_stage_mid_perf_dev] [float],
  [val_stage_mid_perf_az] [float],
  [val_stage_mid_perf_x_coord] [float],
  [val_stage_mid_perf_y_coord] [float],
  [val_time_between_stg] [float],
  [val_prefrac_stc_wh_press_grad] [float],
  [val_prefrac_isip_grad] [float],
  [val_postfrac_isip_grad] [float],
  [val_press_decline_btw_stg] [float],
  [val_bh_press_aft_perf] [float],
  [val_press_decl_5_min_shut_in] [float],
  [val_rt_press_decline_btw_stg] [float],
  [val_rt_press_decl_5_m_shut_in] [float],
  [val_stg_frac_flowback_water] [float],
  [val_latitude] [float],
  [val_longitude] [float],
  [val_completed_length] [float] 
)
WITH
(
	DISTRIBUTION = HASH(cod_uwbi),
	CLUSTERED COLUMNSTORE INDEX
)
GO



