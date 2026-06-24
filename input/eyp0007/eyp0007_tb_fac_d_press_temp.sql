CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_press_temp]
(
    [cod_vers_wellbore] [nvarchar] (4000) NOT NULL,
    [cod_vers_well_string] [nvarchar] (4000) NOT NULL,
    [cod_vers_well] [nvarchar] (4000) NOT NULL,
    [cod_uwbi] [nvarchar] (100),
    [cod_job] [nvarchar] (4000),
    [cod_api10] [nvarchar] (100),
    [des_wellbore_name] [nvarchar] (4000),
    [des_source_port] [varchar] (26),
    [des_units_pressure] [varchar] (26),
    [des_units_temperature] [varchar] (26),
    [val_pressure] [float],
    [val_temperature] [float],
    [fec_utctime] [datetime]
)
WITH
(
  DISTRIBUTION = HASH(cod_uwbi),
  CLUSTERED COLUMNSTORE INDEX
)
GO