CREATE TABLE [sch_anl].[eyp0007_tb_dim_d_eqpm]
(
  [id_vers]	[nvarchar] (4000) NOT NULL,
  [id_eqpm] [nvarchar] (4000) NOT NULL,
  [des_eqpm] [nvarchar] (4000),
  [fec_start_date] [smalldatetime] NOT NULL,
  [fec_end_date] [smalldatetime],
  [des_type_eqpm] [nvarchar] (4000),
  [id_code_pump_model] [nvarchar] (4000),
  [num_pump_ser] [nvarchar] (4000),
  [id_tag] [nvarchar] (4000),
  [des_uom] [nvarchar] (4000),
  [id_counter] [nvarchar] (4000),
  [id_col_point] [nvarchar] (4000),
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