USE [DB_TRAFFIC_FLOW]
GO

-- View for extracting traffic flow data
CREATE OR ALTER VIEW [dbo].[vw_FACT_TRAFFIC_FLOW]
AS
SELECT [fk_tiempo]
      ,[fk_hora]
      ,[fk_sentido]
      ,[fk_tipo_vehiculo]
      ,[fk_forma_pago]
      ,[CANTIDAD_PASOS]
      ,'TRAIN' AS TIPO_CONJUNTO
      ,NULL AS ID_PREDICCION
  FROM [dbo].[fact_flujo]
  WHERE fk_tiempo <= 20171130

  UNION ALL

  SELECT [fk_tiempo]
        ,[fk_hora]
        ,[fk_sentido]
        ,[fk_tipo_vehiculo]
        ,[fk_forma_pago]
        ,[CANTIDAD_PASOS]
        ,'TEST' AS TIPO_CONJUNTO
        ,CONCAT([fk_tiempo], [fk_hora]) AS ID_PREDICCION
  FROM [dbo].[fact_flujo]
  WHERE fk_tiempo > 20171130
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[vw_FACT_TRAFFIC_FLOW_FORECASTING]
AS
SELECT 
    CONVERT(DATETIME, CONVERT(VARCHAR(10), F.FECHA, 120) + ' ' + CONVERT(VARCHAR(8), H.HORA, 108)) AS date_time,
    H.HORA_INT AS [hour],
    H.PERIODO AS [period],
    F.MES_ANIO AS [month],
    F.DIA_SEMANA AS [weekday],
    S.SENTIDO AS sentido,
    V.TIPO_VEHICULO AS tipo_vehiculo,
    P.FORMA_PAGO AS forma_pago,
    B.CANTIDAD_PASOS AS traffic_count,
    TIPO_CONJUNTO = CAST(IIF(TIPO_CONJUNTO = 'TRAIN', 1, 0) AS INT)
  FROM [dbo].[vw_FACT_TRAFFIC_FLOW] B
  INNER JOIN [dbo].[dim_tiempo] F ON B.fk_tiempo = F.id_fecha
  INNER JOIN [dbo].[dim_hora] H ON B.fk_hora = H.id_hora
  INNER JOIN [dbo].[dim_sentido] S ON B.fk_sentido = S.id_sentido
  INNER JOIN [dbo].[dim_tipo_vehiculo] V ON B.fk_tipo_vehiculo = V.id_tipo_vehiculo
  INNER JOIN [dbo].[dim_forma_pago] P ON B.fk_forma_pago = P.id_forma_pago
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[vw_FORECASTING_TRAFFIC_FLOW]
AS
  SELECT 
      FORMAT([DATE_TIME], 'yyyyMMdd') AS FECHA_ID,
      DATEPART(HOUR, [DATE_TIME]) AS HORA_ID,
      P.[MODEL_TRAFFIC_FORECASTER],
      P.[MODEL_TRAFFIC_EXOGENEAS],
      P.[MODEL_TRAFFIC_EXOGENEAS_LGBM],
      P.[MODEL_TRAFFIC_EXOGENEAS_CatBoost],
      P.[MODEL_TRAFFIC_EXOGENEAS_LSTM]
  FROM
    (
        SELECT 
            [DATE_TIME],
            [PREDICCION],
            [MODELO]
        FROM 
            [dbo].[FORECASTING_TRAFFIC_FLOW]
    ) A
    PIVOT(
      MAX(PREDICCION) for MODELO in (
        [MODEL_TRAFFIC_FORECASTER]
        , [MODEL_TRAFFIC_EXOGENEAS]
        , [MODEL_TRAFFIC_EXOGENEAS_LGBM]
        , [MODEL_TRAFFIC_EXOGENEAS_CatBoost]
        , [MODEL_TRAFFIC_EXOGENEAS_LSTM]
      )
    )AS P
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[vw_PREDICCION_TRAFFIC_FLOW]
AS
-- Definir la CTE para seleccionar los datos
WITH CTE_Prediccion AS (
    SELECT 
        [DATE_TIME],
        [PREDICCION],
        [MODELO]
    FROM 
        [dbo].[PREDICCION_TRAFFIC_FLOW]
)

-- Pivotear los datos usando la CTE
SELECT 
    CONCAT(FORMAT([DATE_TIME] ,'yyyyMMdd'), DATEPART(HOUR,[DATE_TIME])) AS ID_PREDICCION,
    [MODEL_FORECASTER]
	, [MODEL_EXOGENEAS]
	, [MODEL_EXOGENEAS_CatBoost]
	, [MODEL_EXOGENEAS_LGBM]
	, [MODEL_EXOGENEAS_LSTM]
FROM 
    (
        SELECT 
            [DATE_TIME],
            [PREDICCION],
            [MODELO]
        FROM 
            CTE_Prediccion
    ) AS SourceTable
PIVOT
    (
        MAX([PREDICCION])
        FOR [MODELO] IN ([MODEL_FORECASTER]
							, [MODEL_EXOGENEAS]
							, [MODEL_EXOGENEAS_CatBoost]
							, [MODEL_EXOGENEAS_LGBM]
							, [MODEL_EXOGENEAS_LSTM])
    ) AS PivotTable;

GO