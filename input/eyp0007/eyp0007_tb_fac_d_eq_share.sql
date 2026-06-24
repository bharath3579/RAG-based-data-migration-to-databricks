CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_eq_share]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_commercial_entity] [nvarchar] (4000) NOT NULL,
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [des_fluid_type] [nvarchar] (4000),
  [id_company] [nvarchar] (4000) NOT NULL,
  [ind_eco_share] [float],
  [des_partner_role] [nvarchar] (4000),
  [ind_field_part] [float],
  [ind_gross_up] [float],
  [ind_net_gross_up] [float],
  [ind_net_part] [float],
  [bol_tik] [char] (3),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime],
  [fec_adhoc_update] [smalldatetime] NOT NULL
)
WITH
(
	DISTRIBUTION = HASH(id_commercial_entity),
	CLUSTERED COLUMNSTORE INDEX
)
GO