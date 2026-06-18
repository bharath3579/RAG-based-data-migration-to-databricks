CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_ch_inj_pnt_st]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_chem_inj_point] [nvarchar] (4000) NOT NULL,
  [id_chem_inj_point] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_collec_point] [nvarchar] (4000) NOT NULL,
  [id_fcty_class_1] [nvarchar] (4000) NOT NULL,
  [id_oper_route] [nvarchar] (4000) NOT NULL,
  [id_prod_unit] [nvarchar] (4000) NOT NULL,
  [id_well] [nvarchar] (4000) NOT NULL,
  [id_stream] [nvarchar] (4000) NOT NULL,
  [id_tank] [nvarchar] (4000) NOT NULL,
  [id_eqpm] [nvarchar] (4000) NOT NULL,
  [id_chem_tank] [nvarchar] (4000) NOT NULL,
  [id_storage] [nvarchar] (4000) NOT NULL,
  [ind_chem_diff_vol] [float] ,
  [ind_dosage] [float] ,
  [des_uom] [nvarchar] (4000) ,
  [des_comments] [nvarchar] (4000) ,
  [des_asset] [nvarchar] (4000) ,
  [des_asset_type] [nvarchar] (4000) ,
  [des_chem_product_name] [nvarchar] (4000) ,
  [ind_inj_ppm] [float] ,
  [ind_inj_vol_literperday] [float] ,
  [ind_inj_vol_lperhr] [float] ,
  [ind_inj_vol_liter] [float] ,
  [fec_create_date] [smalldatetime] ,
  [fec_update_date] [smalldatetime] 
)
WITH
(
  DISTRIBUTION = HASH(id_chem_inj_point),
  CLUSTERED COLUMNSTORE INDEX
)
GO
