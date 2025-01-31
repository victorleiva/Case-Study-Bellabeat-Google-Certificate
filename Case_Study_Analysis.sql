CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Full_table_1` AS
WITH DailyAndIntensity AS 
(
SELECT
  Daily_Act_1.Id AS Id, Daily_Act_1.ActivityDate AS ActivityDate, Daily_Act_1.Activity_time, Daily_Act_1.Total_time AS Total_time, Daily_Act_1.Porcentage_of_activity AS Porcentage_activity, Inten_1.promedio_diario_intensidad AS intensity_average, Daily_Act_1.TotalDistance AS Distance, Daily_Act_1.Calories AS Calories
FROM
  `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_1_NO_NULLS` AS Daily_Act_1

INNER JOIN 
  `warm-aegis-419014.Case_Study_Bellabeat.Intensity_1_Summary` AS Inten_1 ON Daily_Act_1.Id = Inten_1.Id AND Daily_Act_1.ActivityDate = Inten_1.dia
),

DailyIntensitySleep AS 
(
SELECT
  dayint.Id AS Id, dayint.ActivityDate AS ActivityDate, dayint.Total_time AS Total_time, dayint.Porcentage_activity AS Porcentage_activity, dayint.intensity_average AS Intensity, dayint.Distance AS Distance, dayint.Calories AS Calories, Sleep_1.value AS Sleep_stage, Sleep_1.PromedioDuracionMinutos AS Sleep_duration
FROM
  DailyAndIntensity AS dayint
INNER JOIN `warm-aegis-419014.Case_Study_Bellabeat.Sleep_1_Summary` AS Sleep_1 ON dayint.Id = Sleep_1.Id AND dayint.ActivityDate = Sleep_1.FechaEstado
)

SELECT
  dayints.Id AS Id, dayints.ActivityDate AS ActivityDate, dayints.Total_time AS Total_time, dayints.Porcentage_activity AS Porcentace_activity, Steps_1.TotalHorasActivas AS Activity_time, dayints.Intensity AS Intensity, Steps_1.TotalPasos AS Steps_d, dayints.Distance AS Distance, dayints.Calories AS Calories, dayints.Sleep_stage AS Sleep_stage, dayints.Sleep_duration AS Sleep_duration
FROM
  DailyIntensitySleep AS dayints
INNER JOIN `warm-aegis-419014.Case_Study_Bellabeat.Steps_1_Summary` AS Steps_1 ON dayints.Id = Steps_1.Id AND dayints.ActivityDate = Steps_1.FechaDia;


CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Full_table_2` AS
WITH DailyAndIntensity AS 
(
SELECT
  Daily_Act_2.Id AS Id, Daily_Act_2.ActivityDate AS ActivityDate, Daily_Act_2.Activity_time AS Activitytime, Daily_Act_2.Total_time AS Total_time, Daily_Act_2.Porcentage_of_activity AS Porcentage_activity, Inten_2.promedio_diario_intensidad AS intensity_average, Daily_Act_2.TotalDistance AS Distance, Daily_Act_2.Calories AS Calories
FROM
  `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_2_NO_NULLS` AS Daily_Act_2

INNER JOIN 
  `warm-aegis-419014.Case_Study_Bellabeat.Intensity_2_Summary` AS Inten_2 ON Daily_Act_2.Id = Inten_2.Id AND Daily_Act_2.ActivityDate = Inten_2.dia
),

DailyIntensitySleep AS 
(
SELECT
  dayint.Id AS Id, dayint.ActivityDate AS ActivityDate, dayint.Activitytime AS Activitytime, dayint.Total_time AS Total_time, dayint.Porcentage_activity AS Porcentage_activity, dayint.intensity_average AS Intensity, dayint.Distance AS Distance, dayint.Calories AS Calories, Sleep_1.value AS Sleep_stage, Sleep_1.PromedioDuracionMinutos AS Sleep_duration
FROM
  DailyAndIntensity AS dayint
INNER JOIN `warm-aegis-419014.Case_Study_Bellabeat.Sleep_2_Summary` AS Sleep_1 ON dayint.Id = Sleep_1.Id AND dayint.ActivityDate = Sleep_1.FechaEstado
)

SELECT
  dayints.Id AS Id, dayints.ActivityDate AS ActivityDate, dayints.Total_time AS Total_time, dayints.Activitytime AS Activitytime, dayints.Porcentage_activity AS Porcentace_activity, Steps_1.TotalHorasActivas AS Activity_time, dayints.Intensity AS Intensity, Steps_1.TotalPasos AS Steps_d, dayints.Distance AS Distance, dayints.Calories AS Calories, dayints.Sleep_stage AS Sleep_stage, dayints.Sleep_duration AS Sleep_duration
FROM
  DailyIntensitySleep AS dayints
INNER JOIN `warm-aegis-419014.Case_Study_Bellabeat.Steps_2_Summary` AS Steps_1 ON dayints.Id = Steps_1.Id AND dayints.ActivityDate = Steps_1.FechaDia;

### Create a table with the day of highest frequency of activity per person 
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Dias_mayor_ejercitacion_1` AS
WITH FilasUnicas AS (
    SELECT
        Id,
        ActivityDate,
        Steps_d,
        Activity_time, -- Incluimos MinutosActivos aquí
        ROW_NUMBER() OVER (PARTITION BY Id, ActivityDate ORDER BY ActivityDate) AS FilaNum
    FROM
        `warm-aegis-419014.Case_Study_Bellabeat.Full_table_1`
),
ResumenDiario AS (
    SELECT
        Id,
        DATE(ActivityDate) AS FechaDia,
        SUM(Steps_d) AS TotalPasos,
        SUM(Activity_time) AS TotalMinutosActivos -- Sumamos los minutos activos
    FROM
        FilasUnicas
    WHERE FilaNum = 1
    GROUP BY Id, FechaDia
),
ActividadPonderada AS (
    SELECT
        Id,
        FechaDia,
        TotalPasos,
        TotalMinutosActivos,
        -- Ponderación: Ajusta estos valores según la importancia que le des a cada métrica
        (TotalPasos * 0.6) + (TotalMinutosActivos * (0.4/60)) AS PuntuacionActividad -- Ponderación y conversión a horas
    FROM ResumenDiario
),
RankingActividad AS (
  SELECT
        Id,
        FechaDia,
        TotalPasos,
        TotalMinutosActivos,
        PuntuacionActividad,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY PuntuacionActividad DESC) AS RankingDia
    FROM ActividadPonderada
)
SELECT
    Id,
    FechaDia,
    TotalPasos,
    TotalMinutosActivos,
    PuntuacionActividad
FROM RankingActividad
WHERE RankingDia <= 3
ORDER BY Id, PuntuacionActividad DESC;

CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Dias_mayor_ejercitacion_2` AS
WITH FilasUnicas AS (
    SELECT
        Id,
        ActivityDate,
        Steps_d,
        Activitytime, -- Incluimos MinutosActivos aquí
        ROW_NUMBER() OVER (PARTITION BY Id, ActivityDate ORDER BY ActivityDate) AS FilaNum
    FROM
        `warm-aegis-419014.Case_Study_Bellabeat.Full_table_2`
),
ResumenDiario AS (
    SELECT
        Id,
        DATE(ActivityDate) AS FechaDia,
        SUM(Steps_d) AS TotalPasos,
        SUM(Activitytime) AS TotalMinutosActivos -- Sumamos los minutos activos
    FROM
        FilasUnicas
    WHERE FilaNum = 1
    GROUP BY Id, FechaDia
),
ActividadPonderada AS (
    SELECT
        Id,
        FechaDia,
        TotalPasos,
        TotalMinutosActivos,
        -- Ponderación: Ajusta estos valores según la importancia que le des a cada métrica
        (TotalPasos * 0.6) + (TotalMinutosActivos * (0.4/60)) AS PuntuacionActividad -- Ponderación y conversión a horas
    FROM ResumenDiario
),
RankingActividad AS (
  SELECT
        Id,
        FechaDia,
        TotalPasos,
        TotalMinutosActivos,
        PuntuacionActividad,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY PuntuacionActividad DESC) AS RankingDia
    FROM ActividadPonderada
)
SELECT
    Id,
    FechaDia,
    TotalPasos,
    TotalMinutosActivos,
    PuntuacionActividad
FROM RankingActividad
WHERE RankingDia <= 3
ORDER BY Id, PuntuacionActividad DESC;

##### Create a table with the basic stats for all of the variables
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Basic_Stats` AS
WITH DatosConvertidos AS(
    SELECT 
      CAST(Id AS FLOAT64) AS Id, 
      CAST(Total_time AS FLOAT64) AS Total_time,
      CAST(Activity_time AS FLOAT64) AS Activity_time,
      CAST(Intensity AS FLOAT64) AS Intensity,
      CAST(Steps_d AS FLOAT64) AS Steps_d,
      CAST(Distance AS FLOAT64) AS Distance,
      CAST(Calories AS FLOAT64) AS Calories
    FROM `warm-aegis-419014.Case_Study_Bellabeat.Full_table_1`  
),
UnpivotedData AS (
    SELECT *
    FROM DatosConvertidos
    UNPIVOT(valor FOR columna IN (Id, Total_time, Activity_time, Intensity, Steps_d, Distance, Calories))
),
AgregacionesPorColumna AS (
  SELECT 
    'Maximo' AS tipo_calculo,
    MAX(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    MAX(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    MAX(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    MAX(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    MAX(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    MAX(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    MAX(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData
  UNION ALL
  SELECT 
    'minimo' AS tipo_calculo,
    MIN(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    MIN(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    MIN(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    MIN(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    MIN(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    MIN(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    MIN(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData
  UNION ALL
    SELECT 
    'Average' AS tipo_calculo,
    AVG(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    AVG(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    AVG(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    AVG(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    AVG(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    AVG(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    AVG(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData
 UNION ALL
    SELECT 
    'Standar Deviation' AS tipo_calculo,
    STDDEV_SAMP(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    STDDEV_SAMP(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    STDDEV_SAMP(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    STDDEV_SAMP(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    STDDEV_SAMP(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    STDDEV_SAMP(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    STDDEV_SAMP(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData  
  UNION ALL
    SELECT 
    'Sum' AS tipo_calculo,
    SUM(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    SUM(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    SUM(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    SUM(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    SUM(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    SUM(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    SUM(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData
)
SELECT * FROM AgregacionesPorColumna;

CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Basic_Stats_2` AS
WITH DatosConvertidos AS(
    SELECT 
      CAST(Id AS FLOAT64) AS Id, 
      CAST(Total_time AS FLOAT64) AS Total_time,
      CAST(Activitytime AS FLOAT64) AS Activity_time,
      CAST(Intensity AS FLOAT64) AS Intensity,
      CAST(Steps_d AS FLOAT64) AS Steps_d,
      CAST(Distance AS FLOAT64) AS Distance,
      CAST(Calories AS FLOAT64) AS Calories
    FROM `warm-aegis-419014.Case_Study_Bellabeat.Full_table_2`  
),
UnpivotedData AS (
    SELECT *
    FROM DatosConvertidos
    UNPIVOT(valor FOR columna IN (Id, Total_time, Activity_time, Intensity, Steps_d, Distance, Calories))
),
AgregacionesPorColumna AS (
  SELECT 
    'Maximo' AS tipo_calculo,
    MAX(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    MAX(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    MAX(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    MAX(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    MAX(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    MAX(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    MAX(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData
  UNION ALL
  SELECT 
    'minimo' AS tipo_calculo,
    MIN(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    MIN(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    MIN(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    MIN(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    MIN(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    MIN(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    MIN(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData
  UNION ALL
    SELECT 
    'Average' AS tipo_calculo,
    AVG(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    AVG(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    AVG(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    AVG(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    AVG(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    AVG(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    AVG(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData
 UNION ALL
    SELECT 
    'Standar Deviation' AS tipo_calculo,
    STDDEV_SAMP(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    STDDEV_SAMP(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    STDDEV_SAMP(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    STDDEV_SAMP(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    STDDEV_SAMP(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    STDDEV_SAMP(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    STDDEV_SAMP(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData  
  UNION ALL
    SELECT 
    'Sum' AS tipo_calculo,
    SUM(CASE WHEN columna = 'Id' THEN valor ELSE NULL END) AS Id,
    SUM(CASE WHEN columna = 'Total_time' THEN valor ELSE NULL END) AS Total_time,
    SUM(CASE WHEN columna = 'Activity_time' THEN valor ELSE NULL END) AS Activity_time,
    SUM(CASE WHEN columna = 'Intensity' THEN valor ELSE NULL END) AS Intensity,
    SUM(CASE WHEN columna = 'Steps_d' THEN valor ELSE NULL END) AS Steps_d,
    SUM(CASE WHEN columna = 'Distance' THEN valor ELSE NULL END) AS Distance,
    SUM(CASE WHEN columna = 'Calories' THEN valor ELSE NULL END) AS Calories
  FROM UnpivotedData
)
SELECT * FROM AgregacionesPorColumna;
