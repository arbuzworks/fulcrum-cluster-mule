# Fulcrum HA
Fulcrum is High Availability Clustering solution powered by leading Open Source In-Memory Data Fabric Apache Ignite for Mule ESB Community Edition. It improves the availability, scalability and reliability of applications deployed into Mule ESB Community Edition version 3.5.0 and 3.6.0.

# Build
Run the following command from the terminal:

    mvn package

Distribution will be located at distributions/target/fulcrum-cluster-mule-x.x.x.tar

# Distribution
The tar file contains the following directory structure:

./apps                 - demo application  
./lib                  - cluster jar  
./README.txt           - this file  

# Installation
Unpack the tar file into desired installation location. 

## Cluster Jar
1. Copy lib/fulcrum-cluster.jar to ${MULE_HOME}/lib/mule folder on every node of Mule ESB.
2. Copy conf/fulcrum-cluster.properties to ${MULE_HOME}/conf folder on every node of Mule ESB.
3. Make sure that the value of fulcrum.clusterNodeId property is unique on every node (e.g. 1, 2, 3).
4. Start mule servers.

## Demo Application
1. Copy apps/mule-cluster-demo.zip to ${MULE_HOME}/apps folder on every node of Mule ESB.
 
# Feedback
Tell us about any issues, suggestions or other comments you have.
You can post on - dev@arbuzworks.com

Enjoy!  
Fulcrum Team
