CREATE TABLE [sch_anl].[eyp0007_tb_aux_d_dim_s_hrchy]
( 
  [id_final] [nvarchar] (200) NOT NULL,
  [id_vers_well] [nvarchar] (200) NOT NULL,
  [id_vers_wbore] [nvarchar] (200) NOT NULL,
  [id_vers_wbore_int] [nvarchar] (200) NOT NULL,
  [id_vers_perf_int] [nvarchar] (200) NOT NULL,
  [id_vers_res_bl_for] [nvarchar] (200) NOT NULL,
  [des_well] [nvarchar] (200) NOT NULL,
  [des_well_bore] [nvarchar] (200) NOT NULL,
  [des_country] [nvarchar] (200) NOT NULL,
  [des_well_bore_interval] [nvarchar] (200) NOT NULL,
  [des_perf_interval] [nvarchar] (200) NOT NULL,
  [cod_resv_block_form] [nvarchar] (200) NOT NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_final)
)
GO
