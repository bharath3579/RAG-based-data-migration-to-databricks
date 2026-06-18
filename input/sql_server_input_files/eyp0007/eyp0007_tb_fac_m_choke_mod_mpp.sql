
CREATE TABLE [sch_anl].[eyp0007_tb_fac_m_choke_mod_mpp]
(
  [fec_production_day] [smalldatetime] NOT NULL,
  [id_vers_choke_model] [nvarchar] (4000) NOT NULL,
  [id_choke_model] [nvarchar] (4000) NOT NULL,
  [id_area] [nvarchar] (4000) NOT NULL,
  [id_facility_class_1] [nvarchar] (4000) NOT NULL,
  [id_prod_unit] [nvarchar] (4000) NOT NULL,
  [ind_cpd_qty_boe] [float], 
  [ind_empp_qty_boe] [float], 
  [ind_epp_qty_boe] [float], 
  [ind_mpp_qty_boe] [float],
  [ind_well_potential_boe] [float], 
  [ind_min_choke_boe] [float],
  [des_min_choke] [nvarchar] (4000),
  [fec_create_date] [smalldatetime],
  [fec_update_date] [smalldatetime]
)
WITH
(
	DISTRIBUTION = HASH(id_choke_model),
	CLUSTERED COLUMNSTORE INDEX
)
GO
