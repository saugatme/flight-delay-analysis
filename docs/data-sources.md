# Data sources

## BTS flight records

The pipeline uses monthly US domestic flight records from the BTS Reporting Carrier On-Time Performance dataset. Records include flight dates, carriers, origin and destination airports, delays, cancellations and diversions.

Download monthly archives from [BTS TranStats](https://transtats.bts.gov/PREZIP/) and extract one CSV per reporting month. The current parser expects 110 source columns and selects 28 required fields. Changes to the source format may require updates to the schema checks.

Ingestion preserves the original file and records its identity and row count. Exact source checksums and processing timestamps are stored in generated manifests rather than maintained in this guide.

## OpenFlights airports

The pipeline uses the airport-only [`airports.dat`](https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat) export to add airport names and coordinates. The extended export includes other transport facilities that are unnecessary for airport joins.

The file is a headerless CSV with 14 fields, including airport ID, name, city, country, IATA and ICAO codes, coordinates, altitude and timezone information. The `\N` marker represents a missing value.

Validation selects records with valid IATA codes and geographic values. Flights are joined to the reference by origin and destination codes. Unmatched flights remain in the dataset, and quality records report missing matches. As the reference is a snapshot, airport coverage may differ from the flight reporting period.

### Attribution

Contains information from the OpenFlights Airports Database, available under the Open Database License (ODbL).

See the [source documentation](https://openflights.org/data.php) and [license](https://github.com/jpatokal/openflights/blob/master/data/LICENSE) for applicable terms. OpenFlights data is used for analytical summaries and maps, not navigation.
