CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_chem_tnk_stat]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_chem_tank] [nvarchar] (4000) NOT NULL,
  [id_chem_tank] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_collec_point] [nvarchar] (4000) NOT NULL,
  [id_fcty_class_1] [nvarchar] (4000) NOT NULL,
  [id_oper_route] [nvarchar] (4000) NOT NULL,
  [id_prod_unit] [nvarchar] (4000) NOT NULL,
  [id_chem_product] [nvarchar] (4000) NOT NULL,
  [des_comments] [nvarchar] (4000) ,
  [des_product_type] [nvarchar] (4000) ,
  [des_tank_uom] [nvarchar] (4000) ,
  [ind_closing] [float] ,
  [ind_consumed] [float] ,
  [ind_filled] [float] ,
  [ind_opening] [float] ,
  [fec_create_date] [smalldatetime] ,
  [fec_update_date] [smalldatetime] 
)
WITH
(
  DISTRIBUTION = HASH(id_chem_tank),
  CLUSTERED COLUMNSTORE INDEX
)
GO
