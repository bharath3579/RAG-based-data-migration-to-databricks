CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_chem_prod]
( 
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_chem_product] [nvarchar] (4000) NOT NULL,
  [des_chem_product] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [des_product_type] [nvarchar] (4000),
  [des_vendor] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO