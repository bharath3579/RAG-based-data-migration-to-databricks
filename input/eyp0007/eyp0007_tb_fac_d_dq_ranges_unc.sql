CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_dq_ranges_unc]
(
  [fec_process] [smalldatetime] NOT NULL,
  [cod_vers_wellbore] [nvarchar] (4000),
  [des_well_bore] [nvarchar] (4000),
  [cod_vers_well_hole] [nvarchar] (4000),
  [des_well_hole] [nvarchar] (4000),
  [des_source_system] [nvarchar] (4000),
  [des_business_term] [nvarchar] (4000),
  [des_unit] [nvarchar] (4000),
  [des_functional_dataset] [nvarchar] (4000),
  [des_business_unit] [nvarchar] (4000),
  [des_county] [nvarchar] (4000),
  [des_pad] [nvarchar] (4000),
  [des_category] [nvarchar] (4000),
  [des_source_dataset] [nvarchar] (4000),
  [des_source_attribute] [nvarchar] (4000),
  [num_stage] [int],
  [des_attribute] [nvarchar] (4000) NOT NULL,
  [des_dataset] [nvarchar] (4000) NOT NULL,
  [val_value] [float],
  [des_rule_type] [nvarchar] (4000) NOT NULL,
  [val_max] [float],
  [val_min] [float],
  [ind_warning] [int] NOT NULL,
  [fec_time_start] [datetime2],
  [fec_time_end] [datetime2]
)
WITH
(
	DISTRIBUTION = HASH(des_dataset),
	CLUSTERED COLUMNSTORE INDEX
)
GO