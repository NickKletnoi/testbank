
########### WaterLevel Measurements #################

CREATE TABLE [dbo].[WaterLevelMeasurements](
	[StationId] [int] NOT NULL,
	[Time] [datetime] NOT NULL,
	[WaterLevel] [decimal](10, 6) NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[WaterLevelStations](
	[StationId] [int] NOT NULL,
	[Name] [varchar](max) NOT NULL,
	[Longitude] [decimal](10, 6) NULL,
	[Latitude] [decimal](10, 6) NULL,
 CONSTRAINT [PK_WaterLevelStations] PRIMARY KEY CLUSTERED
(
	[StationId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


CREATE TRIGGER [dbo].[trgAfterWaterLevelMeasurements_DupRem] ON [dbo].[WaterLevelMeasurements]
FOR INSERT
AS

WITH DUPS AS
( SELECT StationId, Time, ROW_NUMBER() OVER (PARTITION BY StationId, Time ORDER BY Time) AS MK
  FROM [dbo].[WaterLevelMeasurements]
  --WHERE SUBSTRING(Time,6,2)+'/'+SUBSTRING(Time,9,2)+'/'+SUBSTRING(Time,1,4) = CONVERT(varchar(10),GETUTCDATE(),101)
  )
DELETE FROM DUPS WHERE MK > 1;
DELETE from [dbo].[WaterLevelMeasurements]  where  datepart(minute, Time) != '00';
GO

ALTER TABLE [dbo].[WaterLevelMeasurements] ENABLE TRIGGER [trgAfterWaterLevelMeasurements_DupRem]
GO


################## Weather Observations #################

CREATE TABLE [dbo].[WeatherObservations_AirPressure_stg](
	[StationId] [int] NULL,
	[ObservationTime] [datetime] NULL,
	[AirPressure] [varchar](20) NULL
) ON [PRIMARY]
GO


CREATE TRIGGER [dbo].[trgAfterWeatherObservations_AirPressure_stg_DupRem] ON [dbo].[WeatherObservations_AirPressure_stg]
FOR INSERT
AS

WITH DUPS AS
( SELECT
StationId,
[ObservationTime],
[AirPressure],
ROW_NUMBER() OVER (PARTITION BY StationId,ObservationTime,AirPressure ORDER BY ObservationTime desc) AS MK
  FROM [dbo].[WeatherObservations_AirPressure_stg]

  )
DELETE FROM DUPS WHERE MK > 1;
GO

ALTER TABLE [dbo].[WeatherObservations_AirPressure_stg] ENABLE TRIGGER [trgAfterWeatherObservations_AirPressure_stg_DupRem]
GO



CREATE TABLE [dbo].[WeatherObservations_AirTemp_stg](
	[StationId] [int] NULL,
	[ObservationTime] [datetime] NULL,
	[AirTemperature] [varchar](20) NULL
) ON [PRIMARY]
GO


CREATE TRIGGER [dbo].[trgAfterWeatherObservations_AirTemp_stg_DupRem] ON [dbo].[WeatherObservations_AirTemp_stg]
FOR INSERT
AS

WITH DUPS AS
( SELECT
StationId,
[ObservationTime],
[AirTemperature],
ROW_NUMBER() OVER (PARTITION BY StationId,ObservationTime,AirTemperature ORDER BY ObservationTime desc) AS MK
  FROM [dbo].[WeatherObservations_AirTemp_stg]

  )
DELETE FROM DUPS WHERE MK > 1;
GO

ALTER TABLE [dbo].[WeatherObservations_AirTemp_stg] ENABLE TRIGGER [trgAfterWeatherObservations_AirTemp_stg_DupRem]
GO


CREATE TABLE [dbo].[WeatherObservations_WaterTemp_stg](
	[StationId] [int] NULL,
	[ObservationTime] [datetime] NULL,
	[WaterTemperature] [varchar](20) NULL
) ON [PRIMARY]
GO


CREATE TRIGGER [dbo].[trgAfterWeatherObservations_WaterTemp_stg_DupRem] ON [dbo].[WeatherObservations_WaterTemp_stg]
FOR INSERT
AS

WITH DUPS AS
( SELECT
StationId,
[ObservationTime],
[WaterTemperature],
ROW_NUMBER() OVER (PARTITION BY StationId,ObservationTime,WaterTemperature ORDER BY ObservationTime desc) AS MK
  FROM [dbo].[WeatherObservations_WaterTemp_stg]

  )
DELETE FROM DUPS WHERE MK > 1;
GO

ALTER TABLE [dbo].[WeatherObservations_WaterTemp_stg] ENABLE TRIGGER [trgAfterWeatherObservations_WaterTemp_stg_DupRem]
GO



CREATE TABLE [dbo].[WeatherObservations_Wind_stg](
	[StationId] [int] NULL,
	[ObservationTime] [datetime] NULL,
	[WindSpeed] [varchar](20) NULL,
	[WindDirection] [varchar](20) NULL
) ON [PRIMARY]
GO

CREATE TRIGGER [dbo].[trgAfterWeatherObservations_Wind_stg_DupRem] ON [dbo].[WeatherObservations_Wind_stg]
FOR INSERT
AS

WITH DUPS AS
( SELECT
StationId,
[ObservationTime],
[WindSpeed],
[WindDirection],
ROW_NUMBER() OVER (PARTITION BY StationId,ObservationTime, [WindSpeed], [WindDirection] ORDER BY ObservationTime desc) AS MK
  FROM [dbo].[WeatherObservations_Wind_stg]

  )
DELETE FROM DUPS WHERE MK > 1;
GO

ALTER TABLE [dbo].[WeatherObservations_Wind_stg] ENABLE TRIGGER [trgAfterWeatherObservations_Wind_stg_DupRem]
GO
