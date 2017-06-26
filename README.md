# PlayScale - Scalable Data Replay
## Consulting Project

### Overview
PlayScale is a plug and play tool to easily replay offline datasets and test/implement stream processing topologies
using Apache Kafka and Kafka Streams. The main goal for this project was to develop an ingestion and processing
framework with Apache Kafka and Kafka Streams that scales and processes historical data quickly.
Specifically, allowing fast replay of historical data makes it easy to optimize and enhance downstream
application components. This is a common trend with high-growth companies that want to iterate quickly and get
enhancements into production without delay. In the short term, PlayScale will be used offline for testing purposes.
In the long term it will be used as the foundation to replace an online processing system.

### Dependencies ###
    Java 8 is required
    Apache Kafka should be installed and running
    All dependenices can be found in pom.xml
    mvn package will automatically download and include all dependencies in an "uberjar"

### Download and Jar ###
    git clone https://github.com/cbia4/InsightDataEngineering.git
    cd project_dir
    mvn package (this will create a jar - target/PlayScale.jar)

### Configure ###
    rename config_example.properties to config.properties
    fill in all configuration options (bootstrap.servers=localhost:9092)
    If running on an AWS cluster, drop a jar onto each instance in the cluster)

### Run the Streaming Application ###
    java -cp target/PlayScale.jar playscale.PlayScale stream
    There should be one instance running per Kafka broker

### Run the Consumer Application ###
    java -cp target/PlayScale.jar playscale.PlayScale consumer
    This should exist on a separate instance if running on a cluster

### Run the Producer Application ###
    java -cp target/PlayScale.jar playscale.PlayScale producer
    This should exist on a separate instance if running on a cluster




