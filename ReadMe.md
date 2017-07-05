
our config file is different 

 {
  "state": "FL",
  "team": 9,
  "StationId":"12832",
  "basedata_links": [
"https://www.ncei.noaa.gov/orders/cdo/995475.csv"],
  "link": "https://www.ncei.noaa.gov/orders/cdo/1000169.csv",
  "AWSAccess":"",
  "AWSSecret":"",
  "notificationEmail":"test@test.com"
}

Please make sure to use this
 basedata_links cannot be blank
Also we will have to build the image again if any of the file is updated

Part1
docker pull khushbuprkh/path1
docker run -ti khushbuprkh/path1

Part2
docker pull khushbuprkh/part2
docker run -ti khushbuprkh/part2