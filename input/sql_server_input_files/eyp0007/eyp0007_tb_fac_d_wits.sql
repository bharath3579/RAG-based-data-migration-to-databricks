CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_wits]
(
  [cod_vers_wellbore] [nvarchar] (4000) NOT NULL,
  [cod_vers_well_string] [nvarchar] (4000) NOT NULL,
  [cod_vers_well] [nvarchar] (4000) NOT NULL,
  [cod_uwbi] [varchar] (26),
  [cod_uwbi_num_stage] [varchar] (4000),
  [cod_wits] [nvarchar] (4000),
  [fec_date] [datetime],
  [num_stage] [int],
  [val_elapsed_time] [int],
  [val_wellhead_press] [float],
  [val_tot_proppant_conc] [float],
  [val_tot_proppant_mass] [float],
  [val_clean_flow_rate] [float],
  [val_slurry_flow_rate] [float],
  [val_total_clean_vol] [float],
  [val_total_slurry_vol] [float],
  [val_friction_red_vol] [float],
  [val_pump_elapsed_time] [float]
)
WITH
(
  DISTRIBUTION = HASH(cod_vers_wellbore),
  CLUSTERED COLUMNSTORE INDEX
)
GO