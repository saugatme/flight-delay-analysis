# Project Process Log

## 2026-10-08 - Local environment

- Selected WSL 2 and Ubuntu for the Linux development environment.
- Stored the project under `~/projects` instead of OneDrive.
- Installed uv 0.12.23 and uv-managed Python 3.12.15.
- Confirmed Java 17.0.20.1 and Git 2.43.0.
- Installed and authenticated GitHub CLI.
- Created a private GitHub repository.
- Initialized a packaged uv project with a `src/` layout.
- Added PySpark, pandas, PyArrow, Plotly, and Kaleido.
- Added pytest and Ruff as development tools.

Reason: WSL gives Hadoop and Spark a Linux environment while the user continues to work on Windows. The repository stays in the WSL filesystem because Linux tools are slower and less reliable when they work through `/mnt/c` or OneDrive.

## 2026-10-10 - Spark setup check

- Started PySpark 4.2.0 in local mode with Java 17.
- Created and displayed a one-row Spark DataFrame.
- Confirmed that Python, Java, and PySpark work together in WSL.
- Confirmed that Ruff passes.
- Observed normal WSL and native Hadoop startup warnings.

Result meaning: this test proved that the local development runtime works. It did not prove that Spark runs on a distributed cluster.

## 2026-10-10 - BTS source inspection

- Downloaded the BTS January 2025 On-Time Performance archive.
- Kept the archive and extracted CSV in `data/raw/2025-01/`.
- Confirmed 539,747 flight records and 110 source columns.
- Confirmed that all 28 required columns are present.
- Confirmed that raw data is excluded from Git.

Result meaning: the file matches the expected January partition and contains the fields needed for the first pipeline version. The row count does not prove that every value is valid.

## 2026-10-10 - Initial typed Parquet data

- Added the first CSV-to-Parquet pipeline and five tests.
- Kept all input fields as strings during CSV reading.
- Applied explicit date, integer, double, and Boolean types.
- Rejected missing business keys and invalid cancellation or diversion flags.
- Wrote 539,747 rows with 28 typed columns to local Parquet.
- Read the Parquet data back and confirmed the row count.

Result meaning: no row was lost during the first type-conversion test. This local Parquet output is a development artifact. It is not the final Silver layer because the approved proposal requires HDFS.

## 2026-10-10 - Proposal alignment review

- Reviewed the approved project proposal again.
- Confirmed that raw flight data must be stored in HDFS.
- Confirmed that PySpark and Spark SQL must process the data.
- Confirmed that processed results must be stored in MongoDB.
- Confirmed that Plotly Dash must serve the final interactive views.
- Replaced the local-only target architecture with an HDFS-to-Spark-to-MongoDB pipeline plan.

Reason: the earlier local-only plan did not match the stated tool commitments in the proposal.

## 2026-10-10 - Native Hadoop installation in WSL

- Rejected the existing `/mnt/c/hadoop` installation for WSL use.
- Observed `bash\r` errors from Windows CRLF line endings.
- Installed Apache Hadoop 3.5.0 under `~/opt/hadoop-3.5.0`.
- Verified the official SHA-512 checksum before extraction.
- Set `JAVA_HOME` to `/usr/lib/jvm/java-17-openjdk-amd64`.
- Set `HADOOP_HOME` and placed Linux Hadoop before Windows Hadoop in `PATH`.
- Confirmed that `hdfs` resolves to the Linux installation.

Result meaning: Hadoop loads Linux shell scripts and Java libraries without using the incompatible Windows copy.

## 2026-10-10 - HDFS configuration and startup

- Configured `fs.defaultFS` as `hdfs://localhost:9000`.
- Configured replication factor `1` for one DataNode.
- Stored NameNode metadata under `~/hadoop-data/namenode`.
- Stored DataNode blocks under `~/hadoop-data/datanode`.
- Installed and started the OpenSSH server.
- Created password-free SSH access from the WSL user to localhost.
- Formatted the new NameNode once.
- Started the NameNode, DataNode, and SecondaryNameNode.
- Confirmed one live DataNode with `hdfs dfsadmin -report`.
- Confirmed zero missing blocks and zero corrupt replicas.

Result meaning: HDFS is operational in pseudo-distributed mode. Each daemon runs as a separate Java process, but all daemons run on one physical computer.

Resolved configuration errors:

- A second XML declaration in `core-site.xml` caused an XML parser error.
- A missing opening `<configuration>` tag in `hdfs-site.xml` caused a multiple-root error.
- Both files now return the expected values through `hdfs getconf`.

## 2026-10-10 - HDFS layer layout and shared configuration

- Created `/flight-delay/bronze/bts/year=2025/month=01`.
- Created `/flight-delay/silver/flights`.
- Created `/flight-delay/gold`.
- Added `config/pipeline.yml`.
- Added PyYAML as a project dependency.
- Stored the run date partition, local landing pattern, HDFS layer names, data-quality expectations, delay threshold, and MongoDB endpoint in YAML.
- Confirmed that Python can parse the YAML file.

Result meaning: the storage contract exists, and future Python code can avoid fixed input and output paths.

## Current stop point

- HDFS is running and empty except for the directory structure.
- The BTS CSV has not been copied into HDFS Bronze.
- The existing typed Parquet data remains local.
- MongoDB is not installed.
- No Gold table or dashboard is complete.

Next action: implement and test the configuration loader and Bronze ingestion command. Do not start Gold analysis before the HDFS Bronze-to-Silver path works.
