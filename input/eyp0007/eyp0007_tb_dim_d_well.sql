CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_well]
( 
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_well] [nvarchar] (4000) NOT NULL,
  [des_well] [nvarchar] (4000),
  [id_cds] [nvarchar] (4000),
  [des_cds] [nvarchar] (4000),
  [des_conduit_name] [nvarchar] (4000),
  [id_vers_well_hole] [nvarchar] (4000) NOT NULL,
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [bol_alloc_flag] [char] (1),
  [des_choke_uom] [nvarchar] (4000),
  [id_commercial_entity] [nvarchar] (4000),
  [id_country_ws] [nvarchar] (4000),
  [des_lift_system] [nvarchar] (4000),
  [id_field_cds] [nvarchar] (4000),
  [des_on_stream_method] [nvarchar] (4000),
  [id_fcty_1] [nvarchar] (4000),
  [id_well_hookup] [nvarchar] (4000),
  [bol_operator_flag] [char] (1),
  [des_pump_type] [nvarchar] (4000),
  [des_well_class] [nvarchar] (4000),
  [des_well_function] [nvarchar] (4000),
  [id_well_hole] [nvarchar] (4000),
  [des_well_main_fluid] [nvarchar] (4000),
  [des_well_meter_freq] [nvarchar] (4000),
  [des_ref_object] [nvarchar] (4000),
  [des_type_well] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO