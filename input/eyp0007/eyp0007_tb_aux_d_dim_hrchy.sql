CREATE TABLE [sch_anl].[eyp0007_tb_aux_d_dim_hrchy]
( 
  [id_productionunit]	[nvarchar] (4000) NOT NULL,
  [des_productunit] [nvarchar] (4000),
  [id_area] [nvarchar] (4000) NOT NULL,
  [des_area] [nvarchar] (4000),
  [id_facility_class_1] [nvarchar] (4000) NOT NULL,
  [des_facility_class_1] [nvarchar] (4000),
  [id_well_hookup] [nvarchar] (4000),
  [des_well_hookup] [nvarchar] (4000),
  [id_well_hole] [nvarchar] (4000),
  [des_well_hole] [nvarchar] (4000),
  [id_well] [nvarchar] (4000),
  [des_well] [nvarchar] (4000),
  [id_stream] [nvarchar] (4000),
  [des_stream] [nvarchar] (4000), 
  [id_choke_model] [nvarchar] (4000),
  [des_choke_model] [nvarchar] (4000),   
  [id_final] [nvarchar] (4000) NOT NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_final)
)
GO






