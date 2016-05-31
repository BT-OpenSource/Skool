# Skool
An open Source hadoop Data Integration Tool.

### Overview
Skool - Hadoop Data integration tool which automates data engineering and can bring data into and out of Hadoop from RDBMS (0.1 version supports Oracle only 10G and above) and from files (Comma seprated, tab seprated, muti charcter file seprator).
This tool takes a property/configuration file as input from the user and runs the application which validates the following –
-	All the user provided details.
-	Connection to source database.
-	Sqoop import (for one record) as a check.
-	Generates all required files and pushes them to HDFS.
-	Generates a workflow to be scheduled in Oozie.

#### Problem Statement:
With increased adoption of big data comes the challenge of integrating existing data sitting in various relational and file based systems with the big data and Hadoop infrastructure. There are open source connectors (ex. Sqoop) and utilities (ex. Httpfs/Curl on Linux) provided by database vendors and big data community to make it easy to exchange data, however in our experience engineering teams spend an inordinate amount of time writing code to simply shift data in and out of a big data system using these utilities.  Part of this is due to varying data structures in databases that need to be moved across making it essentially a bespoke development every time and part is due to integrating it with security mechanisms like Kerberos which are deployed in enterprise big data installations.  Typical data transfer coding involves handling various datatypes, accounting for volumes and variety of data to be transferred, writing test cases, creating workflow jobs for milestone / delta transfer and creating requisite Hive tables.  Testing all these steps also requires extensive engineering effort.
We also evaluated the following tools available in the market or in the open source community none of which were able to meet the use cases we were trying to address:

-	Gobblin (LinkedIn contributed Open Source tool) - more focused on management of data flow schedule rather than ingestion or extraction
-	Apache Nifi (Open Source) - does not cover requisite end to end flow, integration with Kerberos
-	ODI (covered under Oracle EWL) has limited big data integration capability

In order to make it faster and easier to exchange data from/to a big data system, we propose Skool - Data Integration Tool.  This tool covers the following aspects:

-	Seamless data transfer into a relational database (Oracle/Sql Server/MySql/Netezza or any JDBC compliant database)
-	Seamless data transfer from a relational database into Hadoop (includes automated creation of Ozzie Workflows and Hive Tables)
-	File transfer and Hive table creation for file based transfers into Hadoop
-	Automatic generation and deployment of file creation scripts and jobs from Hadoop or Hive Tables

#### Advantages of a  Skool:
- Effort to write numerous lines of code for data integration would be replaced with a few clicks, thus saving valuable time.
- Code consistency  & standard would be maintained.
- Capture lineage and avoid making data swamp.
- Generates an executable file/Ozzie Workflow which can be used across the platform with ease.

This tool gives the advantage to just run the application and have all the scripts, required files and the Oozie coordinator and workflow xml generated automatically which in turn will perform milestone/incremental pulls as AVRO data files, have HIVE table created over them.
One of the catching features of this tool is that it will give the user an AUDIT table for all the workflow actions, its failures if any along with more useful details which will help keep a track of the entire workflow.

### Version
0.1

### Tech

Skool uses a number of open source projects to work properly:

* [Apache Hadoop] - A distibuted file system
* [Sqoop] - Tool to transfer bulk data efficiently between hadoop and RDBMS
* [Hive] - data warehouse infrastructure built on top of Hadoop for providing data summarization, query, and analysis
* [Pig] - high-level platform for creating MapReduce programs used with Hadoop
* [Oozie] - Oozie is a workflow scheduler system to manage Apache Hadoop jobs.

And of course Skool itself is open source with a [public repository][Skool] on GitHub
 
#### Scope
A UI based open source tool which will ingest data automatically from following sources
-	Any JDBC compliant database (Oracle/SqlServer/Netezza) → HDFS/Hive/Impala Metastore.
-	HDFS/Hive/Impala Metastore → Any JDBC compliant database (Oracle/SqlServer/Netezza)
-	Files (CSV, JSon, XML) → HDFS/Hive/Impala Metastore
-	Hadoop → Hadoop transfer
-	HDFS/Hive/Impala Metastore Files → File extract to downstream systems.

#### Key Features
-	Skool generates code which can be automatically executed (or scheduled) for delta and milestone replication with defined frequency of data refresh.
-	Skool is configurable to select tables/columns/files which are to be transferred in our out of Hadoop.
-	Inbuilt optimization of storage to deliver performant code – Skool takes into consideration table size, database partitions, file formats and compression.
	
### Installation
 ```sh
 create a directory eg: mkdir skool_tool
 cd skool_tool
 mkdir libs
 mkdir configuration
 git clone [git-repo-url]
 cd Skool/dataintegration/configuration

**edit the configuration.properties.template, password.properties.template and log4j.properties.template and rename it to configuration.properties, password.properties and log4j.properties. For editing configuration.properties file as per your cluster specifications follow the comments in the template file (configuration.properties.template)**

**you need to get ojdbc6-11.2.0.3.jar and place in the <Skool/di_tool_runnable/> directory**

cd ..
mvn install
cp target/libs/* ../../libs
cp target/dataintegration-0.0.1-SNAPSHOT.jar ../../
cp configuration/configuration.properties ../../configuration/
cd ../..
cp Skool/di_tool_runnable/* configuration/
mv configuration/run.sh .
sh run.sh 
```


