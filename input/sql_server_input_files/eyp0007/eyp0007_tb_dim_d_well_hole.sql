CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_well_hole]
( 
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_well_hole] [nvarchar] (4000) NOT NULL,
  [des_well_hole] [nvarchar] (4000),
  [id_cds] [nvarchar] (4000),
  [des_cds] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,  
  [fec_end_date] [smalldatetime],
  [ind_bh_latitude] [float],
  [ind_bh_longitude] [float],
  [des_country] [nvarchar] (4000),
  [ind_latitude] [float],
  [ind_longitude] [float],
  [id_fcty_1] [nvarchar] (4000),
  [des_status] [nvarchar] (4000),
  [id_basin] [varchar] (60),
  [des_common_name] [nvarchar] (4000),
  [des_county] [varchar] (40),
  [des_country_name] [varchar] (25),
  [des_ground_elevation] [nvarchar] (4000),
  [des_ground_elevation_unit] [varchar] (12),
  [des_normalized_geographic_coordinate_system] [varchar] (80),
  [des_onshore_or_offshore] [varchar] (40),
  [des_slot_name] [nvarchar] (4000),
  [fec_spud_date] [datetime2],
  [des_state_or_province] [varchar] (40),
  [des_well_name] [nvarchar] (4000),
  [ind_normalized_latitud] [float],
  [ind_normalized_longitude] [float],
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO