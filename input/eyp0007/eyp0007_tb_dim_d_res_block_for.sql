CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_res_block_for]
( 
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_resv_block_form] [nvarchar] (4000) NOT NULL,
  [cod_resv_block_form] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO