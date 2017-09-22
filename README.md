# Netstat-Dependency
![alt text](https://raw.githubusercontent.com/Confman/Netstat-Dependency/master/Capture.PNG)

## Getting Started
This project leverages netstat output that is collected from all servers in order to create dependency map between them by utilizing Spark, GraphX and Neo4j.
It also generates json and gexf graph outputs for other possible use cases.

Netstat output should be named as establishedConnections.out and put into public folder.
Netstat output format should be as follows:
<server_hostname>,<server_ip>,<source_or_target_port>,<target_ip>,<source_or_target_port>,<process_name>
  
This repository's establishedConnections.out is a mock one. Code is also tested with a realistic output of over 360k entries. 

### Prerequisites
Scala: 2.10.6
Spark: 2.1
JRE: 1.8
Neo4j: 3.2.5

### Installing
Install Neo4j to your localhost.
Neo4j database should be put into the project folder.
Suggested username&password: neo4j-admin123

Import this repository as a maven project to your IDE.

## License
This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
