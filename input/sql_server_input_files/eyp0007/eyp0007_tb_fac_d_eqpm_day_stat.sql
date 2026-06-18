CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_eqpm_day_stat]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_eqpm] [nvarchar] (4000) NOT NULL,
  [id_eqpm] [nvarchar] (4000) NOT NULL,
  [id_fcty_class_1] [nvarchar] (4000) NOT NULL,
  [id_col_point] [nvarchar] (4000) NOT NULL,
  [ind_calc_on_stream_hrs] [float] ,
  [des_class_name_type] [nvarchar] (4000) ,
  [ind_on_stream_hrs] [float] ,
  [ind_on_stream_hrs_ytd] [float] ,
  [ind_running_hrs] [float] ,
  [ind_total_on_stream_hrs] [float] ,
  [ind_total_running_hrs] [float] ,
  [ind_co_emission_tonnes] [float] ,
  [ind_ethane_emission_tonnes] [float] ,
  [ind_fuel_usage_tonnes] [float] ,
  [ind_hap_emissions_tonnes] [float] ,
  [ind_hcho_emission_tonnes] [float] ,
  [ind_metalic_emission_tonnes] [float] ,
  [ind_nox_emission_tonnes] [float] ,
  [ind_organic_emission_tonnes] [float] ,
  [ind_pm10_emission_tonnes] [float] ,
  [ind_pm25_emission_tonnes] [float] ,
  [ind_pmcond_emission_tonnes] [float] ,
  [ind_pmtotal_emission_tonnes] [float] ,
  [ind_sox_emission_tonnes] [float] ,
  [ind_voc_emission_tonnes] [float] ,
  [fec_create_date] [smalldatetime] ,
  [fec_update_date] [smalldatetime] 
)
WITH
(
  DISTRIBUTION = HASH(id_eqpm),
  CLUSTERED COLUMNSTORE INDEX
)
GO
