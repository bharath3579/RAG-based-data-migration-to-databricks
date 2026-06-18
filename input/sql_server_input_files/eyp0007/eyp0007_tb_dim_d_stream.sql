CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_stream]
(
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_stream] [nvarchar] (4000) NOT NULL,
  [des_stream] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [des_stream_category] [nvarchar] (4000),
  [des_stream_meter_freq] [nvarchar] (4000),
  [des_stream_phase] [nvarchar] (4000),
  [des_type_stream] [nvarchar] (4000),
  [des_alloc_period] [nvarchar] (4000),
  [id_col_point] [nvarchar] (4000),
  [id_disp_type] [nvarchar] (4000),
  [id_fcty_1] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED INDEX (id_vers)
)
GO