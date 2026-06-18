CREATE TABLE [sch_anl].[eyp0007_tb_fac_d_drilling]
(
  [cod_vers_wellbore] [nvarchar] (4000) NOT NULL,
  [cod_uwbi] [nvarchar] (4000),
  [val_md] [float],
  [val_Temperature] [float],
  [val_GR] [float],
  [val_GR_at_bit] [float],
  [val_ROP] [float],
  [val_RPM] [float],
  [val_SPP] [float],
  [val_WOB] [float],
  [val_Flow_Out] [float],
  [val_Total_Gas] [float],
  [val_C1] [float],
  [val_C2] [float],
  [val_C3] [float],
  [val_C4] [float],
  [val_iC4] [float],
  [val_nC4] [float],
  [val_C5] [float],
  [val_Hole_Diameter] [float],
  [val_Date] [float],
  [val_Time] [float]
)
WITH
(
  DISTRIBUTION = HASH(cod_vers_wellbore),
  CLUSTERED COLUMNSTORE INDEX
)
GO
