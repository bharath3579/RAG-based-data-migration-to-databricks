CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_resp_org_header]
(
  [cod_resp_org]                 NVARCHAR(4000)   NOT NULL,
  [cod_long_id]                  NVARCHAR(4000)   NULL,
  [des_kind]                     NVARCHAR(4000)   NULL,
  [des_address_1]                NVARCHAR(4000)   NULL,
  [des_address_2]                NVARCHAR(4000)   NULL,
  [des_address_3]                NVARCHAR(4000)   NULL,
  [des_postal_code]              NVARCHAR(4000)   NULL,
  [des_postal_country]           NVARCHAR(4000)   NULL,
  [des_more_address]             NVARCHAR(4000)   NULL,
  [des_description]              NVARCHAR(4000)   NULL,
  [des_data_source]              NVARCHAR(4000)   NULL,
  [fec_create]                   DATETIME         NULL,
  [fec_create_user_id]           DATETIME         NULL,
  [fec_update]                   DATETIME         NULL,
  [fec_update_user_id]           DATETIME         NULL,
  [cod_org_long]                 NVARCHAR(4000)   NULL,
  [des_org_kind]                 NVARCHAR(4000)   NULL,
  [fec_start]                    DATETIME         NULL,
  [fec_end]                      DATETIME         NULL,
  [cod_installation]             NVARCHAR(4000)   NULL
)
WITH
(
  DISTRIBUTION = HASH(cod_resp_org),
  CLUSTERED COLUMNSTORE INDEX
)
GO
