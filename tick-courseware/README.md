# TICK - Telegraf, InfluxDb, Chronograf & Kapacitor #

## Description ##

* Telegraf is a server agent for collecting & reporting metrics with number of supported formats and plug-ins
* InfluxDb is an open source time series database optimized for fast, high-availability storage and retrieval of time series data
* InfluxDb is tailored for operations monitoring, application metrics, sensor data and real-time analytics use-cases
* Chronograf is an open source web application and UI to visualize InfluxDb monitoring data, to create alerts, automation rules, and to oversee ETL Jobs
* Kapacitor is an open source data processing framework, suitable for streaming data and for batch data, also assisting in creating alerts and detecting abnormalities

## Audience ##

* Operations looking to consolidate logs processing and automation
* Developers dealing with high-volume incoming data and a requirement to process and/or to visualize the data
* Database Administrators on a quest of enriching their skill set with Non-Rdbms products and their approach
* Business Intelligence professionals curious about what's there in addition to traditional Data Warehouse products

## Course's Objectives ##

* Bring TICK novices to the level where they would be comfortable with installing, configuring and using TICK stack
* Setup, configure and troubleshoot TICK products in a collaborative and explorative environment through a series of practical labs using a sandbox
* Develop InfluxDb queries, continuous queries, and database schema in accordance with the best practices
* Write to and Read data from InfluxDb using CLI and HTTP API
* Learn data retention principles and down-sampling practices
* Application and host metrics capture best practices
* Configure Telegraf to collect and to process host metrics
* Make use of Chronograf to create alerts based on InfluxDb data
* Incorporate data streaming and batch processing with Kapacitator

## Prerequisites ##

* Recent hands-on experience with Linux bash terminal
* Familiarity with data formats: Json, Csv, Logs, etc.
* Recent administrative and troubleshooting experience on Linux OS
* Scripting and/or programming recent experience

## Outline ##

### Introduction to InfluxDb ###
* Time series data and database
* InfluxDb Terminology, basic concepts, implementation, setup, and basic operations
* Design Insides and Trade-offs  
* Comparison to Sql
* Schema Design
* Storage Engine
* Enterprise Clustering

### Writing Data ###
* Overview of alternatives
* Creating database
* HTTP API
* CLI (command-line interface)
* Node.Js client library

### Reading Data ###
* Overview of alternatives
* HTTP API
* CLI (command-line interface)
* Node.Js client library

### Schema Design ###
* Recommended Practices
* Discouraged Practices
* Data Granularity
* Multi-purpose Data

### Data Retention ###
* Shard Groups
* Down-sampling
* Continuous Query
* Retention Policy

### Telegraf ###
* Collecting & reporting metrics
* Architecture
* Input Data Formats
* Input plug-ins
* Output Data Formats
* Output plug-ins

### Chronograf ###
* Multi-purpose User Interface
* Infrastructure Monitoring
* Alert Management
* ETL Jobs Monitoring
* Database Management

**replace with grafana for Cisco**

### Kapacitator ###
* Data Processing Framework
* Streaming data
* Batch data
* Data transformation
* Writing user defined functions
* Integration with external services

**remove for Cisco**
**replace with application and host metric best practices**

### Preparing for Production ###
* Capacity planning
* Hardware requirements
* Cloud hosting
* Docker Container
* Monitoring and troubleshooting
* Backup and Restore

**reduce coverage**

**3 days?**
**Cisco is using HAProxy for Micro Services**
