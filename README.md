This project aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. As a vehicle passes a toll plaza, the vehicle’s data like vehicle_id,vehicle_type,toll_plaza_id and timestamp are streamed to Kafka. 
Create a data pipe line that collects the streaming data and loads it into a database.

Objectives:
Create a database and table using PostgreSQL
Start Zookeeper
Start Kafka server
Install Kafka python driver
Create topic named 'toll' in Kafka
Create toll trafic generator
Modify the streaming data generator to steam to toll topic
Create streaming data connecting to local database
Save streaming data to csv format
Verify if data is stored in the database
