# energy-usage-behavior
Analysis of energy usage in the presence/absence of bad air quality
## Selected Cities and Load Zones

The analysis includes a set of representative cities from major US electricity markets (NEISO, NYISO, CAISO). Each city is mapped to its corresponding load zone, which is used for aggregating and analyzing energy usage and air quality data.

Below is a table listing each city, its state, and the associated load zone:

| City           | State         | (GridStatus) Load Zone    |
|----------------|--------------|-----------------------|
| Portland       | Maine        | .Z.MAINE              |
| Manchester     | New Hampshire| .Z.NEWHAMPSHIRE       |
| Burlington     | Vermont      | .Z.VERMONT            |
| Providence     | Rhode Island | .Z.RHODEISLAND        |
| Bridgeport     | Connecticut  | .Z.CONNECTICUT        |
| Brockton       | Massachusetts| .Z.WCMASS             |
| Springfield    | Massachusetts| .Z.SEMASS             |
| Boston         | Massachusetts| .Z.NEMASSBOST         |
| Buffalo        | New York     | west                  |
| Rochester      | New York     | genese                |
| Syracuse       | New York     | centrl                |
| Plattsburgh    | New York     | north                 |
| Utica          | New York     | mhk_vl                |
| Albany         | New York     | capitl                |
| Poughkeepsie   | New York     | hud_vl                |
| White Plains   | New York     | millwd                |
| Yonkers        | New York     | dunwod                |
| New York City  | New York     | nyc                   |
| Hempstead      | New York     | longil                |
| San Jose       | California   | (not mapped)          |
| Los Angeles    | California   | (not mapped)          |
| Truckee        | California   | (not mapped)          |
| Fresno         | California   | (not mapped)          |
| Sacramento     | California   | (not mapped)          |
| Redding        | California   | (not mapped)          |

**Note:** For California cities, load zones are not explicitly mapped in the current dataset and are marked as "(not mapped)".

This mapping is defined and generated in `helper.py` and saved to `Data/city_zone_info.csv` for use in further analysis.
