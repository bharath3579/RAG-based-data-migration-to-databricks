CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_dq_completness_unc]
(
  [fec_process] [smalldatetime] NOT NULL,
  [cod_vers_well_hole] [nvarchar] (4000),
  [cod_vers_wellbore] [nvarchar] (4000),
  [des_business_unit] [nvarchar] (4000),
  [des_source_system] [nvarchar] (4000),
  [des_business_term] [nvarchar] (4000),
  [des_functional_dataset] [nvarchar] (4000),
  [des_county] [nvarchar] (4000),
  [des_pad] [nvarchar] (4000),
  [des_category] [nvarchar] (4000),
  [des_object_name] [nvarchar] (4000),
  [des_source_dataset] [nvarchar] (4000),
  [des_source_attribute] [nvarchar] (4000),
  [des_dataset] [nvarchar] (4000) NOT NULL,
  [des_attribute] [nvarchar] (4000) NOT NULL,
  [val_value] [float],
  [ind_warning] [int] NOT NULL,
  [fec_drilling_end] [datetime2],
  [fec_completion_end] [datetime2],
  [fec_start_date] [datetime2]
)
WITH
(
	DISTRIBUTION = HASH(des_dataset),
	CLUSTERED COLUMNSTORE INDEX
)
GO