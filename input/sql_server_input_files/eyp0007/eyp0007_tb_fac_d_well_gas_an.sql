CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_well_gas_an]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_well] [nvarchar] (4000) NOT NULL,
  [id_well] [nvarchar] (4000) NOT NULL,
  [id_commercial_entity] [nvarchar] (4000) NOT NULL,
  [id_facility_class_1] [nvarchar] (4000) NOT NULL,
  [id_well_hookup] [nvarchar] (4000) NOT NULL,
  [id_well_hole] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_productionunit] [nvarchar] (4000) NOT NULL,
  [id_geo_area] [nvarchar] (4000) NOT NULL,
  [id_field_cds] [nvarchar] (4000) NOT NULL,
  [id_field] [nvarchar] (4000) NOT NULL,
  [id_basin] [nvarchar] (4000) NOT NULL,
  [id_operator_route] [nvarchar] (4000) NOT NULL,
  [id_col_point] [nvarchar] (4000) NOT NULL,
  [id_licence] [nvarchar] (4000) NOT NULL,
  [fec_lab_date] [smalldatetime],
  [fec_valid_from] [smalldatetime],
  [ind_analysis_no] [bigint] ,
  [ind_analysis_no2] [bigint] ,
  [des_analysis_status] [nvarchar] (4000) ,
  [fec_create_date] [smalldatetime] ,
  [fec_update_date] [smalldatetime] 
)
WITH
(
	DISTRIBUTION = HASH(id_well),
	CLUSTERED COLUMNSTORE INDEX
)
GO