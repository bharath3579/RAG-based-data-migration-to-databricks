CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_dq_ranges]
(
  [fec_process] [smalldatetime] NOT NULL,
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_well] [nvarchar] (4000) NOT NULL,
  [id_commercial_entity] [nvarchar] (4000) NOT NULL,
  [id_facility_class_1] [nvarchar] (4000) NOT NULL,
  [des_facility_class_1] [nvarchar] (4000) NOT NULL,
  [id_well_hookup] [nvarchar] (4000) NOT NULL,
  [id_well_hole] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [des_area] [nvarchar] (4000) NOT NULL,
  [id_productionunit] [nvarchar] (4000) NOT NULL,
  [des_productunit] [nvarchar] (4000) NOT NULL,
  [id_geo_area] [nvarchar] (4000) NOT NULL,
  [id_field_cds] [nvarchar] (4000) NOT NULL,
  [id_field] [nvarchar] (4000) NOT NULL,
  [id_basin] [nvarchar] (4000) NOT NULL,
  [id_operator_route] [nvarchar] (4000) NOT NULL,
  [id_col_point] [nvarchar] (4000) NOT NULL,
  [id_licence] [nvarchar] (4000) NOT NULL,
  [id_choke_model] [nvarchar] (4000) NOT NULL,
  [des_functional_dataset] [nvarchar] (4000) NOT NULL,
  [des_attribute] [nvarchar] (4000) NOT NULL,
  [des_uom] [nvarchar] (4000) NOT NULL,
  [ind_value] [float] NOT NULL,
  [ind_warning] [int] NOT NULL
)
WITH
(
	DISTRIBUTION = HASH(id_productionunit),
	CLUSTERED COLUMNSTORE INDEX
)
GO