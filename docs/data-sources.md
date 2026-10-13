# Project data sources

## BTS On-Time Performance

The verified flight partitions are the January 2024 and January 2025 BTS On-Time Performance CSV files. Each Bronze manifest records the exact filename, byte size, SHA-256 checksum, row count, ingestion time, and HDFS URI.

Official archive directory: [BTS TranStats PREZIP](https://transtats.bts.gov/PREZIP/)

| Partition | Rows | Columns | Size | SHA-256 |
| --- | ---: | ---: | ---: | --- |
| January 2024 | 547,271 | 110 | 246,830,488 bytes | `e92febedf6cd51bd87eca0be2fba3f5c1f383b1da41119db7f1cc1a26c4d3286` |
| January 2025 | 539,747 | 110 | 243,177,378 bytes | `d7c7d59452cad1215d9605e8ff350a4bad7282750765084fe57928a5ad275453` |

January 2024 and January 2025 are like-period inputs for the cross-year comparison.

Do not edit the local landing file or its HDFS Bronze copy.

## OpenFlights airport reference data

Selected source: [`airports.dat`](https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat)

Source documentation: [OpenFlights airport data](https://openflights.org/data.php)

License text: [OpenFlights data license](https://github.com/jpatokal/openflights/blob/master/data/LICENSE)

Retrieval and ingestion time: `2026-10-11T16:36:35.986369Z`

Recorded source facts:

- rows: 7,698;
- fields per row: 14;
- size: 1,127,225 bytes;
- SHA-256: `9387cdb38df5bd664da823f8ccb69fdd9b33a1888f5b7cca09c34a3cd9ff59f9`;
- HDFS Bronze path: `/flight-delay/bronze/reference/openflights/airports/airports.dat`.

### Why this file was selected

OpenFlights describes `airports.dat` as its high-quality, airport-only export. The alternative `airports-extended.dat` also contains train stations, ferry terminals, and user contributions. Those additional record types do not fit the flight-airport coordinate join.

The GitHub file is a static snapshot that OpenFlights updates only sometimes. The pipeline will therefore preserve the downloaded bytes and record a retrieval timestamp and SHA-256 checksum. A checksum identifies the exact snapshot even when the source URL stays the same.

### Format

The file is UTF-8 CSV without a header row. Each record has 14 fields:

1. OpenFlights airport ID.
2. Airport name.
3. City.
4. Country.
5. Three-letter IATA code.
6. Four-letter ICAO code.
7. Latitude in decimal degrees.
8. Longitude in decimal degrees.
9. Altitude in feet.
10. UTC offset in hours.
11. Daylight-saving-time code.
12. TZ database timezone.
13. Record type.
14. Data source.

OpenFlights uses `\N` for a missing value. The parser must convert that marker to null. It must not treat it as the two-character airport code `\N`.

### License and attribution

The OpenFlights database is available under the Open Database License 1.0. Rights in individual database contents are available under the Database Contents License. Public use requires source acknowledgement and compliance with the applicable license terms.

Required project attribution:

> Contains information from the OpenFlights Airports Database, available under the Open Database License (ODbL).

Keep this notice in the report, dashboard, and documentation that accompanies a public derived database or produced work.

OpenFlights states that the data is not suitable for navigation and provides no accuracy warranty. This project uses it only for analytical maps and airport-level summaries.

### Validation and join results

The explicit parser read all 7,698 rows with 14 named string fields. Validation produced:

- 6,071 join-ready airports with unique three-letter IATA codes;
- 1,626 rejected rows with a missing IATA code;
- 1 rejected row with an invalid IATA code;
- no invalid or out-of-range latitude or longitude values.

Typed accepted and explained rejected records are stored as checksum-versioned HDFS Silver Parquet. The accepted dataset uses integer airport IDs and altitude values and double latitude, longitude, and timezone values.

Both endpoints of 539,747 January flight rows were left joined to the accepted airport reference. The output retained all 539,747 flights. Origin and destination each matched 539,516 rows and missed 231 rows. Both endpoints matched for 539,285 rows; 462 rows had at least one unmatched endpoint.
