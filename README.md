# Skool
An open Source hadoop Data Integration Tool.

### Overview
Skool is designed for those users who want to land their data in HDFS and perform analytics on them. This tool takes a property/configuration file as input from the user and runs the application which validates the following –
-	All the user provided details.
-	Connection to source database.
-	Sqoop import (for one record) as a check.
-	Generates all required files and pushes them to HDFS.
-	Generates a workflow to be scheduled in Oozie.

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

And of course Skool itself is open source with a [public repository][Skool]
 on GitHub
 
### Installation
 ```sh
 create a directory eg: mkdir skool_tool
 cd skool_tool
 mkdir libs
 git clone [git-repo-url]
 cd Skool/dataintegration
```
edit the configuration.properties.template and password.properties.template and rename it to configuration.properties and password.properties
```sh
mvn install
cp target/libs/* ../../libs
cp target/dataintegration-0.0.1-SNAPSHOT.jar ../../
cd ../..
```
copy the configuration.properties edited earlier to the directory skool_tool
```sh
cp Skool/di_tool_runnable/* .
sh run.sh 
```
#### License
The MIT License (MIT)
Copyright (c) 2016 BT Plc (www.btplc.com) Contact nitin.2.goyal@bt.com

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.




 
