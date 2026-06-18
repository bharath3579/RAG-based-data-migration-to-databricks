CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_licence]
(
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_licence] [nvarchar] (4000) NOT NULL,
  [des_licence] [nvarchar] (4000),
  [fec_start_date] [datetime2] NOT NULL,
  [fec_end_date] [datetime2],
  [des_reserves_area] [nvarchar] (4000),  
  [cod_reservoir] [nvarchar] (4000),
  [des_operation] [nvarchar] (4000),
  [cod_sicor] [nvarchar] (4000),
  [cod_society] [nvarchar] (4000),
  [bol_oil_primary_ind] [char] (1),
  [bol_condensate_ind] [char] (1),
  [bol_ngl_ind] [char] (1),
  [bol_gas_ind] [char] (1),
  [bol_gas_dissolved_ind] [char] (1),
  [fec_create_date] [datetime2],
  [fec_update_date] [datetime2]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO