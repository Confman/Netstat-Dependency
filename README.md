# Netstat-Dependency
This project is intended to leverage netstat output collected from all servers in order to create dependency map between servers utilizing Spark, GraphX and Neo4J.
It also generates json and gexf graph outputs.

Netstat output should be named as establishedConnections.out and put into public folder.
Netstat output format should be as follows:
<hostname>,<ip>,<source_or_target_port>,<target_ip>,<source_or_target_port>,<process_name>
  
This repository's establishedConnections.out is a mock one. Code is also tested with a realistic output of over 360000 entries. 

# Requirements
Scala: 2.10.6
Spark: 2.1
JRE: 1.8
Neo4J: 3.2.5

Neo4j database should be put into the project folder.
Suggested username&password: neo4j-admin123
