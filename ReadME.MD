## NOAA - Weather Watch

### Description: 
Analyze and create meaning out of NOAA weather data.

### Approach: 
Picked a weather station (Niagara), extracted data from all available years and derived trend of monthly day time average temparature, day of min and max temperatures by year.

### Details:
noaa_dl.py --station 725280-14733 --concurrency 10 ====> Extracts data for Niagara weather station(725280-14733) from all years, merges into a single file.

WeatherJob.scala ====> Gives trend of monthly day time average temparature, day of min and max temperatures by year

weather_watch\resources\InputDataSet.txt ===> Sample input dataset

weather_watch\out\results.txt ===> results




### Used Artifacts

```
spark 2.2
java 1.8
scala 2.11
sbt

```

