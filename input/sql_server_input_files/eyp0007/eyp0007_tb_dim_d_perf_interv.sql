CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_perf_interv]
(
  [id_vers] [nvarchar] (4000) NOT NULL,
  [id_perf_interval] [nvarchar] (4000) NOT NULL,
  [des_perf_interval] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [ind_bottom_perf_tvd] [float],
  [ind_bottom_perf_md] [float],
  [id_resv_block_formation] [nvarchar] (4000),
  [ind_top_perf_md] [float],
  [ind_top_perf_tvd] [float],
  [id_well_bore_int] [nvarchar] (4000),
  [id_well_bore] [nvarchar] (4000),
  [id_well] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]

)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO