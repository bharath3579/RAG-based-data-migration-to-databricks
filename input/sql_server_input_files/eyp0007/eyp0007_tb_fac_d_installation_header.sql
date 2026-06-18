CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_installation_header]
(
  [cod_installation]          NVARCHAR(4000)   NOT NULL,
  [des_installation]          NVARCHAR(4000)   NULL,
  [des_country]               NVARCHAR(4000)   NULL,
  [des_existence_kind]        NVARCHAR(4000)   NULL,
  [des_type]                  NVARCHAR(4000)   NULL,
  [des_function]              NVARCHAR(4000)   NULL,
  [des_status]                NVARCHAR(4000)   NULL,
  [fec_installation]          DATETIME         NULL,
  [fec_removal]               DATETIME         NULL,
  [des_operator]              NVARCHAR(4000)   NULL,
  [des_onshore_offshore]      NVARCHAR(4000)   NULL,
  [val_latitude]              FLOAT            NULL,
  [val_longitude]             FLOAT            NULL,
  [des_geo_coord_sys]         NVARCHAR(4000)   NULL,
  [val_x_coord]               FLOAT            NULL,
  [val_y_coord]               FLOAT            NULL,
  [des_xy_unit]               NVARCHAR(4000)   NULL,
  [des_xy_coord_sys]          NVARCHAR(4000)   NULL,
  [val_z_coord]               FLOAT            NULL,
  [des_vertical_unit]         NVARCHAR(4000)   NULL,
  [des_vertical_datum]        NVARCHAR(4000)   NULL,
  [des_location_type]         NVARCHAR(4000)   NULL,
  [fec_create]                DATETIME         NULL,
  [cod_create_user]           NVARCHAR(4000)   NULL,
  [fec_update]                DATETIME         NULL,
  [cod_update_user]           NVARCHAR(4000)   NULL,
  [cod_field]                 NVARCHAR(4000)   NULL,
  [des_field]                 NVARCHAR(4000)   NULL,
  [des_facility_short_cod]                 NVARCHAR(4000)   NULL,
  [des_facility_tag_short_cod]                 NVARCHAR(4000)   NULL,
  [des_sap_functional_cod]                 NVARCHAR(4000)   NULL,
  [des_description]            NVARCHAR(4000)   NULL,
  [des_data_source]            NVARCHAR(4000)   NULL,
  [des_responsible_org]            NVARCHAR(4000)   NULL,
  [des_org_long_id]            NVARCHAR(4000)   NULL
)
WITH
(
  DISTRIBUTION = HASH(cod_installation),
  CLUSTERED COLUMNSTORE INDEX
)
GO
