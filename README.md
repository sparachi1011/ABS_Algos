# Cleaning up Anomoly Run Data
- Please find the order of tables to be wiped out while clearing Anomalies, FYR.
- Note:  XYZ is the RunID you want to wipeout.

```
DELETE FROM [AD].[Messages] WHERE RunID in(SELECT RunID FROM [AD].[AlgorithmRunStatus] WHERE RunID='XYZ')
DELETE FROM [AD].[AlgorithmRunDetailStatus] WHERE RunID in (SELECT RunID FROM [AD].[AlgorithmRunStatus] WHERE RunID='XYZ')
DELETE FROM [AD].[AnomalySensorDetails] WHERE AlarmID in (SELECT AlarmID FROM [AD].[Alarms] WHERE RunID='XYZ')
DELETE FROM [AD].[Alarms] WHERE RunID in (SELECT RunID FROM [AD].[AlgorithmRunStatus] WHERE RunID='XYZ')
DELETE FROM [AD].[AlgorithmRunStatus] WHERE RunID='XYZ'
```