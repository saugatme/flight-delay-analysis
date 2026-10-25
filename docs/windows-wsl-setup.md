# Local environment setup

The project is developed on Ubuntu under WSL 2. Spark uses local execution, and HDFS runs in pseudo-distributed mode. The instructions below assume an x86-64 Ubuntu environment.

## Requirements

- Python 3.12 and uv
- Java 17
- Apache Hadoop 3.5.0
- MongoDB 8.0.28 binaries, or a compatible local MongoDB installation
- Git, curl, unzip and OpenSSH

On Windows, install Ubuntu with `wsl --install -d Ubuntu` from an administrator PowerShell session. Run the remaining commands in Ubuntu. A Linux filesystem location is recommended for the repository and service data.

## Python and Java

```bash
sudo apt update
sudo apt install --yes git openjdk-17-jdk curl unzip openssh-server
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Open a new shell, clone the repository and install dependencies:

```bash
git clone https://github.com/saugatme/flight-delay-analysis.git
cd flight-delay-analysis
uv python install 3.12
uv sync --all-groups
```

## Hadoop

Install the Linux Hadoop 3.5.0 distribution under `~/opt/hadoop-3.5.0`. Set the following in your shell configuration:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME="$HOME/opt/hadoop-3.5.0"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
```

Set `JAVA_HOME` in Hadoop's `hadoop-env.sh` as well. In `core-site.xml`, configure:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

In `hdfs-site.xml`, configure single-node replication and persistent storage. Replace `YOUR_USER` with your Linux username:

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///home/YOUR_USER/hadoop-data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///home/YOUR_USER/hadoop-data/datanode</value>
    </property>
</configuration>
```

Hadoop's startup scripts require passwordless localhost SSH. Start the SSH service and authorise an existing public key, or create a dedicated key if none exists:

```bash
sudo systemctl enable --now ssh
```

For a new SSH key:

```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
ssh -o StrictHostKeyChecking=accept-new localhost true
```

Initialise a **new, empty HDFS installation** once:

```bash
mkdir -p ~/hadoop-data/namenode ~/hadoop-data/datanode
hdfs namenode -format
start-dfs.sh
hdfs dfsadmin -report
```

Formatting creates a filesystem namespace and is not part of routine startup. An existing installation should be started without reformatting. The report should show one live DataNode. Pipeline commands create their own output directories.

## MongoDB

Install MongoDB binaries under `~/opt/mongodb-8.0.28`, or set `MONGODB_HOME` to your installation directory. The helper scripts use a dedicated local data directory and bind to `127.0.0.1:27017`.

For a fresh database with no existing users or `.env` file, initialise credentials from the repository root:

```bash
./scripts/start_local_mongodb.sh --no-auth
uv run python scripts/configure_mongodb_users.py
./scripts/stop_local_mongodb.sh
./scripts/start_local_mongodb.sh
```

The configuration script creates database users and saves generated connection strings in the ignored `.env` file. The application account has read/write access to the project database. Subsequent sessions use authenticated startup and the existing environment file; user creation is a one-time operation.

## Verify and run

```bash
uv run flight-delay-spark-check
hdfs dfsadmin -report
```

Continue with the [operations guide](operations.md) to prepare reference data, process a reporting month and start the dashboard.

## Service endpoints

| Service | Local endpoint |
| --- | --- |
| HDFS client | `hdfs://localhost:9000` |
| NameNode web interface | `http://localhost:9870` |
| MongoDB | `127.0.0.1:27017` |
| Dashboard | `http://127.0.0.1:8050` |

This is a local development configuration. HDFS is not configured with Kerberos, and these services should remain on the local network interface.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Hadoop cannot find Java | Set `JAVA_HOME` in the shell and `hadoop-env.sh`. |
| Hadoop startup cannot connect over SSH | Confirm the SSH service is active and `ssh localhost true` succeeds without a password. |
| HDFS connection refused | Check `jps`, the Hadoop logs and whether the NameNode is running. |
| Hadoop reports an XML error | Each configuration file needs one `configuration` root with valid property elements. |
| MongoDB cannot start | Inspect the log in `~/.local/state/flight-delay-mongodb/` and check whether port 27017 is already in use. |
| MongoDB authentication fails | Confirm `.env` is loaded and its credentials match the configured database users. |
