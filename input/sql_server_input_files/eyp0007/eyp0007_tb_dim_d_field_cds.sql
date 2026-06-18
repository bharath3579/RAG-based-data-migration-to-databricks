CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_field_cds]
(
  [id_vers] [nvarchar] (4000) NOT NULL,
  [id_field] [nvarchar] (4000) NOT NULL,
  [des_field] [nvarchar] (4000),
  [cod_field] [nvarchar] (4000),
  [fec_create_date] [smalldatetime] NOT NULL,
  [fec_update_date] [smalldatetime] NOT NULL,
  [id_basin] [nvarchar] (4000),
  [des_primary_reservoir] [nvarchar] (4000),
  [des_country_name] [nvarchar] (4000),
  [des_data_source] [nvarchar] (4000),
  [ind_easting] [float],
  [des_field_type] [nvarchar] (4000),
  [des_geo_coordinate_system] [nvarchar] (4000),
  [ind_latitude] [float],
  [ind_longitud] [float],
  [ind_northing] [float],
  [id_notional_well] [nvarchar] (4000),
  [des_notional_wellbore] [nvarchar] (4000),
  [bol_onshore_or_offshore] [nvarchar] (4000),
  [des_original_coordinate_system] [nvarchar] (4000),
  [des_original_coordinate_unit] [nvarchar] (4000),
  [id_parent] [nvarchar] (4000),
  [des_parent] [nvarchar] (4000),
  [des_type_parent] [nvarchar] (4000),
  [ind_pipeline_distance] [float],
  [des_state_or_province] [nvarchar] (4000)
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO