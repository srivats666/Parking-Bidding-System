# [SmartPARK](http://smartpark.pw)

## Table of contents
1. [Introduction](README.md#introduction)
2. [AWS Clusters](README.md#aws-clusters) 
3. [Data Pipeline](README.md#data-pipeline)
4. [Performance](README.md#performance)
5. [Presentation](README.md#presentation)


## Introduction 
[Back to Table of contents](README.md#table-of-contents)

[SmartPARK](http://smartpark.pw) is a platform which lets users find the closest available parking spots and bid for them. 

## AWS Clusters
[Back to Table of contents](README.md#table-of-contents)

[SmartPARK](http://smartpark.pw) runs on four clusters on AWS:
<ul>
<li>4 m3.large nodes for Spark Streaming</li>
<li>2 m3.large nodes for Kafka</li>
<li>4 m3.large nodes for ElasticSearch </li>
<li>1 m3.large node for Flask</li>
</ul>
As of October, 2016, this system costs ~$26 a day with AWS on-demand instances used.

## Data Pipeline
[Back to Table of contents](README.md#table-of-contents)

The image below depicts the underlying data pipeline.

![Alt text](/pipeline.png?raw=true "Pipeline")

### Data source
The data streams are synthesized and replayed based on the sample json response from sfpark.org

The parking stream sends the parking id and its updated availability. The userbid stream sends the user id, bid amount and lat, long. 

### Spark Streaming
Spark Streaming receives a 10 sec parking and userbid stream window. Spark bulk updates ElasticSearch with their latest availabilities. Then does a bulk search query to get list of available parking spots around the users. Assigns the user to a parking spot based on his bid amount.

### Data bases
ElasticSearch is used to store the parking lot information and its availabilities. Its geospatial capabilities are used to find spots based on user's lat, long. Redis is used to maintain the assignment state and also to publish the assignment results back to the user using web sockets.


## Performance
[Back to Table of contents](README.md#table-of-contents)

The current system processes around 3000 events per sec over a 10 second window.

## Presentation
[Presentation](http://bit.do/sPark).
