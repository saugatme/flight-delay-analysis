# Windows and WSL Setup Guide

## Purpose

This guide is for a person who uses Windows and wants to run this project without Docker.

Run Hadoop, Spark, Python, and MongoDB inside WSL. Do not mix Windows Hadoop scripts with Linux Hadoop scripts. Use Windows only for the desktop, browser, editor, and WSL host.

## Architecture on Windows

```text
Windows
  -> WSL 2
     -> Ubuntu 24.04
        -> Python and PySpark
        -> Hadoop HDFS
        -> MongoDB, when installed
        -> project repository
```

The current HDFS setup is pseudo-distributed. The NameNode, DataNode, and SecondaryNameNode are separate Java processes on one computer.

## 1. Install WSL and Ubuntu

Open PowerShell as Administrator:

```powershell
wsl --install -d Ubuntu
```

Restart Windows if Windows requests it. Open Ubuntu and create the Linux user account.

Check the distribution:

```bash
lsb_release -ds
uname -m
```

This project was tested with Ubuntu 24.04.4 LTS on `x86_64`.

## 2. Keep the repository in WSL

Use a Linux path such as:

```text
/home/YOUR_USER/projects/flight-delay-analysis
```

Do not keep the active repository under `/mnt/c`, OneDrive, or another Windows-mounted directory. Linux permissions, shell scripts, Spark temporary files, and HDFS storage work more reliably in the WSL filesystem.

Open the WSL repository from VS Code with the WSL extension. A normal Windows VS Code window and a WSL VS Code window are not the same runtime.

## 3. Install base tools

In Ubuntu:

```bash
sudo apt update
sudo apt install --yes git gh openjdk-17-jdk curl unzip openssh-server
```

Install uv:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
exec "$SHELL"
```

Clone and prepare the project:

```bash
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/saugatme/flight-delay-analysis.git
cd flight-delay-analysis
uv python install 3.12
uv sync --all-groups
```

## 4. Install Hadoop inside WSL

Do not use a Hadoop installation from `C:\hadoop` or `/mnt/c/hadoop`. Windows copies can contain CRLF line endings. WSL then reports errors such as:

```text
/usr/bin/env: 'bash\r': No such file or directory
```

Download the Linux Hadoop 3.5.0 archive from Apache. Verify its published SHA-512 checksum before extraction.

Install it under:

```text
~/opt/hadoop-3.5.0
```

Add these values once to `~/.bashrc`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME="$HOME/opt/hadoop-3.5.0"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
```

Reload and verify:

```bash
source ~/.bashrc
command -v hdfs
hdfs version
```

The `hdfs` path must point into `/home/YOUR_USER/opt/hadoop-3.5.0/`, not `/mnt/c/hadoop/`.

## 5. Configure pseudo-distributed HDFS

Set `JAVA_HOME` in `$HADOOP_CONF_DIR/hadoop-env.sh`.

Set `fs.defaultFS` in `core-site.xml`:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

Set storage and replication in `hdfs-site.xml`. Replace `YOUR_USER`:

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

Keep only one XML declaration and one `configuration` root in each file.

## 6. Configure localhost SSH

Start SSH:

```bash
sudo systemctl enable --now ssh
```

If no SSH key exists, create one:

```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
ssh -o StrictHostKeyChecking=accept-new localhost true
```

The last command must not request a password.

## 7. Format HDFS once

Create new empty storage directories. Replace `YOUR_USER` if required:

```bash
mkdir -p ~/hadoop-data/namenode
mkdir -p ~/hadoop-data/datanode
```

Format a new NameNode only once:

```bash
hdfs namenode -format
```

Warning: do not repeat the format command after HDFS contains data. A format creates a new filesystem namespace.

## 8. Start and check HDFS

```bash
start-dfs.sh
jps
hdfs dfsadmin -report
```

Expected Java processes:

- NameNode.
- DataNode.
- SecondaryNameNode.
- Jps.

The HDFS report must show one live DataNode and zero missing or corrupt blocks.

Create the project directories:

```bash
hdfs dfs -mkdir -p /flight-delay/bronze/bts/year=2025/month=01
hdfs dfs -mkdir -p /flight-delay/silver/flights
hdfs dfs -mkdir -p /flight-delay/gold
hdfs dfs -ls -R /flight-delay
```

## 9. Use the correct ports

- Use `hdfs://localhost:9000` in Spark and HDFS clients.
- Open `http://localhost:9870` in a browser for the NameNode interface.

Do not open port 9000 in a browser. It is a Hadoop IPC port, not an HTTP port.

## 10. Start and stop a work session

Start:

```bash
source ~/.bashrc
start-dfs.sh
jps
hdfs dfsadmin -report
cd ~/projects/flight-delay-analysis
uv sync --all-groups
```

Stop HDFS when required:

```bash
stop-dfs.sh
```

## 11. Windows firewall and access

The services are for local development. Do not expose ports 9000, 9870, or 27017 to a public network. This setup does not use Kerberos and is not a production security configuration.

## 12. MongoDB status

MongoDB is required by the project proposal, but its WSL installation is not complete at this checkpoint. Follow the project checklist before dashboard work begins. Do not place database credentials in YAML or Git.

## Common problems

### `bash\r` in an error

Cause: WSL used a Windows shell script with CRLF line endings.

Action: confirm that `command -v hdfs` points to the Linux Hadoop installation under `/home`.

### `JAVA_HOME is not set`

Cause: Hadoop cannot locate the Java installation.

Action: set `JAVA_HOME` in both `~/.bashrc` and `hadoop-env.sh`.

### `Connection refused` for SSH

Cause: the OpenSSH server is not installed or active.

Action: install `openssh-server` and start the `ssh` service.

### XML parser error

Cause: a Hadoop XML file has two XML declarations, two root elements, or a missing root tag.

Action: check the file with line numbers. Keep one XML declaration and one `configuration` root.

### Browser reports an HTTP request to an IPC port

Cause: the browser opened port 9000.

Action: use port 9870 for the NameNode web interface.
