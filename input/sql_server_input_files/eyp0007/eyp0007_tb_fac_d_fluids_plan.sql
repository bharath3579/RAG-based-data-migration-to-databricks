CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_fluids_plan]
(
  [cod_vers_wellbore] [nvarchar] (4000) NOT NULL,
  [cod_vers_well_string] [nvarchar] (4000) NOT NULL,
  [cod_vers_well] [nvarchar] (4000) NOT NULL,
  [cod_vers] [nvarchar] (4000),
  [cod_uwbi] [varchar] (26),
  [cod_uwbi_num_stage] [varchar] (4000),
  [num_stage] [int] NOT NULL,
  [fec_create_date] [datetime],
  [des_type] [varchar] (100),
  [des_unit_type] [varchar] (100),
  [des_unit] [varchar] (100),
  [val_amount] [float]
)
WITH
(
  DISTRIBUTION = HASH(cod_vers),
  CLUSTERED COLUMNSTORE INDEX
)
GO