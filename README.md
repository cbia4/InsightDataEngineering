# PlayScale - Scalable Data Replay
## Consulting Project

### Overview
PlayScale is a plug and play tool to easily replay offline datasets and test/implement stream processing topologies
using Apache Kafka and Kafka Streams. The main goal for this project was to develop a system to easily replay
historical data in order to optimize and enhance downstream application components. PlayScale in the short term will
be used offline for testing purposes, but also provides a foundation to build an online processing system.

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

### Run the Consumer Application ###
    java -cp target/PlayScale.jar playscale.PlayScale consumer

### Run the Producer Application ###
    java -cp target/PlayScale.jar playscale.PlayScale producer




