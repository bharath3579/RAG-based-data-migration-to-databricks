CREATE TABLE [sch_anl].[eyp0007_tb_fac_m_ob_it_comment]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_facility_class_1] [nvarchar] (4000) NOT NULL,
  [id_facility_class_1] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_productionunit] [nvarchar] (4000) NOT NULL,
  [id_comment] [bigint] NOT NULL,
  [des_comment_even_type] [nvarchar] (4000) ,
  [des_comment_type] [nvarchar] (4000) ,
  [des_comments] [nvarchar] (4000) ,
  [des_report_ind] [nvarchar] (4000) ,
  [fec_start_date] [smalldatetime] ,
  [fec_create_date] [smalldatetime] ,
  [fec_update_date] [smalldatetime] ,
)
WITH
(
  DISTRIBUTION = HASH(id_facility_class_1),
  CLUSTERED COLUMNSTORE INDEX
)
GO