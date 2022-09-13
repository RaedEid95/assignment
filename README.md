# Avertra Data Engineer Assignment V1

### Description 
Apache Kafka is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications

#### Pros

<ul>
  <li>Low Latency</li>
  <li>High Throughput</li>
  <li>Fault tolerance</li>
  <li>Durability</li>
</ul>


#### Cons

<ul>
  <li>Do not have complete set of monitoring tools</li>
  <li>Message tweaking issues</li>
  <li>Do not support wildcard topic selection</li>
  <li>Reduces Performance</li>
</ul>


#### Intiate The Code

	- sudo docker compose up 
	- run the etl.ipynb file 

### Stopwatch 

	when use the full data :
		
		- 38 seconds to fill the topics in kafka 
		- 7 seconds to consume the messages 
		- 26 seconds to finish the training 
