## Process and preparation of the table Daily Activity 1
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_1_NO_NULLS` AS
SELECT  
  Id, 
  ActivityDate,
  (SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes]) AS val WHERE val IS NOT NULL)AS Activity_time,
  (SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes]) AS val WHERE val IS NOT NULL)AS Total_time,
  SAFE_DIVIDE((SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes]) AS val WHERE val IS NOT NULL),(SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes]) AS val WHERE val IS NOT NULL))* 100 AS Porcentage_of_activity,
  TotalSteps, 
  TotalDistance, 
  Calories
FROM 
  `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_1` 
WHERE NOT (Id IS NULL OR Id = 0
         OR ActivityDate IS NULL
         OR TotalSteps IS NULL OR TotalSteps = 0
         OR TotalDistance IS NULL OR TotalDistance = 0
         OR Calories IS NULL OR Calories = 0)

ORDER BY
  Activity_time DESC;

CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_1_NULLS` AS
SELECT  
  Id, 
  ActivityDate,
  (SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes]) AS val WHERE val IS NOT NULL)AS Activity_time,
  (SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes]) AS val WHERE val IS NOT NULL)AS Total_time,
  SAFE_DIVIDE((SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes]) AS val WHERE val IS NOT NULL),(SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes]) AS val WHERE val IS NOT NULL))* 100 AS Porcentage_of_activity,
  TotalSteps, 
  TotalDistance, 
  Calories
FROM 
  `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_1` 
WHERE (Id IS NULL OR Id = 0
         OR ActivityDate IS NULL
         OR TotalSteps IS NULL OR TotalSteps = 0
         OR TotalDistance IS NULL OR TotalDistance = 0
         OR Calories IS NULL OR Calories = 0)

ORDER BY
  Activity_time DESC

## Process and preparation of the table Daily Activity 2
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_2_NO_NULLS` AS
SELECT  
  Id, 
  ActivityDate,
  (SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes]) AS val WHERE val IS NOT NULL)AS Activity_time,
  (SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes]) AS val WHERE val IS NOT NULL)AS Total_time,
  SAFE_DIVIDE((SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes]) AS val WHERE val IS NOT NULL),(SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes]) AS val WHERE val IS NOT NULL))* 100 AS Porcentage_of_activity,
  TotalSteps, 
  TotalDistance, 
  Calories
FROM `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_2` 

WHERE NOT (Id IS NULL OR Id = 0
         OR ActivityDate IS NULL
         OR TotalSteps IS NULL OR TotalSteps = 0
         OR TotalDistance IS NULL OR TotalDistance = 0
         OR Calories IS NULL OR Calories = 0)

ORDER BY
  Activity_time DESC;

CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_2_NULLS` AS
SELECT  
  Id, 
  ActivityDate,
  (SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes]) AS val WHERE val IS NOT NULL)AS Activity_time,
  (SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes]) AS val WHERE val IS NOT NULL)AS Total_time,
  SAFE_DIVIDE((SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes]) AS val WHERE val IS NOT NULL),(SELECT SUM(val) FROM UNNEST([VeryActiveMinutes, FairlyActiveMinutes, LightlyActiveMinutes, SedentaryMinutes]) AS val WHERE val IS NOT NULL))* 100 AS Porcentage_of_activity,
  TotalSteps, 
  TotalDistance, 
  Calories
FROM `warm-aegis-419014.Case_Study_Bellabeat.Daily_Activity_2` 

WHERE (Id IS NULL OR Id = 0
         OR ActivityDate IS NULL
         OR TotalSteps IS NULL OR TotalSteps = 0
         OR TotalDistance IS NULL OR TotalDistance = 0
         OR Calories IS NULL OR Calories = 0)

ORDER BY
  Activity_time DESC;

## Process and preparation of the table Intensity 1
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Intensity_1_Summary` AS
SELECT
    Id,
    DATE(ActivityHour) AS dia,
    AVG(AverageIntensity) AS promedio_diario_intensidad

FROM `warm-aegis-419014.Case_Study_Bellabeat.Intensity_1` 

GROUP BY Id, dia;

## Process and preparation of the table Intensity 2
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Intensity_2_Summary` AS
SELECT
    Id,
    DATE(ActivityHour) AS dia,
    AVG(AverageIntensity) AS promedio_diario_intensidad

FROM `warm-aegis-419014.Case_Study_Bellabeat.Intensity_2` 

GROUP BY Id, dia;

## Process and preparation of the table Sleep 1
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Sleep_1_Summary` AS
WITH EstadosConDuracion AS 
(
  SELECT
    Id,
    date,
    value,
    LAG(value, 1, NULL) OVER (PARTITION BY Id ORDER BY date) AS EstadoAnterior,
  FROM
    `warm-aegis-419014.Case_Study_Bellabeat.Sleep_1`
),
EstadosConInicioFin AS 
(
  SELECT
    Id,
    date,
    value,
    CASE WHEN EstadoAnterior IS NULL OR value != EstadoAnterior THEN date ELSE NULL END AS InicioEstado,
  FROM EstadosConDuracion
),
DuracionEstados AS 
(
  SELECT
        Id,
        value,
        DATE(date) as FechaEstado,
        date,
        LEAD(date, 1) OVER (PARTITION BY Id ORDER BY date) AS FechaFinEstado,
        TIMESTAMP_DIFF(LEAD(date, 1) OVER (PARTITION BY Id ORDER BY date), date, MINUTE) AS DuracionMinutos
    FROM EstadosConInicioFin
    WHERE InicioEstado IS NOT NULL
)
SELECT
  Id,
  value,
  FechaEstado,
  SUM(DuracionMinutos) AS PromedioDuracionMinutos
FROM
  DuracionEstados
GROUP BY Id, value, FechaEstado
ORDER BY Id, value, FechaEstado;

## Process and preparation of the table Sleep 2
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Sleep_2_Summary` AS
WITH EstadosConDuracion AS 
(
  SELECT
    Id,
    date,
    value,
    LAG(value, 1, NULL) OVER (PARTITION BY Id ORDER BY date) AS EstadoAnterior,
  FROM
    `warm-aegis-419014.Case_Study_Bellabeat.Sleep_2`
),
EstadosConInicioFin AS 
(
  SELECT
    Id,
    date,
    value,
    CASE WHEN EstadoAnterior IS NULL OR value != EstadoAnterior THEN date ELSE NULL END AS InicioEstado,
  FROM EstadosConDuracion
),
DuracionEstados AS 
(
  SELECT
        Id,
        value,
        DATE(date) as FechaEstado,
        date,
        LEAD(date, 1) OVER (PARTITION BY Id ORDER BY date) AS FechaFinEstado,
        TIMESTAMP_DIFF(LEAD(date, 1) OVER (PARTITION BY Id ORDER BY date), date, MINUTE) AS DuracionMinutos
    FROM EstadosConInicioFin
    WHERE InicioEstado IS NOT NULL
)
SELECT
  Id,
  value,
  FechaEstado,
  SUM(DuracionMinutos) AS PromedioDuracionMinutos
FROM
  DuracionEstados
GROUP BY Id, value, FechaEstado
ORDER BY Id, value, FechaEstado;

## Process and preparation of the table Steps 1
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Steps_1_Summary` AS
WITH HorasActivas AS 
(
  SELECT
    Id,
    ActivityHour,
    StepTotal,
    DATE(ActivityHour) AS FechaDia,  -- Extraemos el día aquí
    CASE WHEN StepTotal > 0 THEN 1 ELSE 0 END AS Activo -- Marca las horas activas
  FROM
    `warm-aegis-419014.Case_Study_Bellabeat.Steps_1`
),
ConteoHorasActivas AS 
(
  SELECT 
    Id,
    DATE(ActivityHour) as FechaDia,
    COUNTIF(Activo = 1) as HorasActivasDia
  FROM 
    HorasActivas
  GROUP BY Id, FechaDia
)

SELECT
  ha.Id,
  ha.FechaDia,
  SUM(ha.StepTotal) AS TotalPasos,
  SUM(cha.HorasActivasDia) AS TotalHorasActivas
FROM 
  HorasActivas AS ha
INNER JOIN ConteoHorasActivas cha ON ha.Id = cha.Id AND ha.FechaDia = cha.FechaDia
GROUP BY ha.Id, ha.FechaDia; -- Agrupamos también por la fecha

## Process and preparation of the table Steps 2
CREATE OR REPLACE TABLE `warm-aegis-419014.Case_Study_Bellabeat.Steps_2_Summary` AS
WITH HorasActivas AS 
(
  SELECT
    Id,
    ActivityMinute,
    Steps,
    DATE(ActivityMinute) AS FechaDia,  -- Extraemos el día aquí
    CASE WHEN Steps > 0 THEN 1 ELSE 0 END AS Activo -- Marca las horas activas
  FROM
    `warm-aegis-419014.Case_Study_Bellabeat.Steps_2`
),
ConteoHorasActivas AS 
(
  SELECT 
    Id,
    DATE(ActivityMinute) as FechaDia,
    COUNTIF(Activo = 1) as HorasActivasDia
  FROM 
    HorasActivas
  GROUP BY Id, FechaDia
)

SELECT
  ha.Id,
  ha.FechaDia,
  SUM(ha.Steps) AS TotalPasos,
  SUM(cha.HorasActivasDia) AS TotalHorasActivas
FROM 
  HorasActivas AS ha
INNER JOIN ConteoHorasActivas cha ON ha.Id = cha.Id AND ha.FechaDia = cha.FechaDia
GROUP BY ha.Id, ha.FechaDia; -- Agrupamos también por la fecha
