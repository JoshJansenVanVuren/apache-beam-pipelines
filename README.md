# apache-beam-pipelines
Various batch and streaming apache beam pipeline implementations and examples. This README serves as a skeleton for getting the implementations to work on your own machine. All the implementations are coded in JAVA.

## Preliminaries
1. You need a java JDK: I used version 8 (jdk-8u231-linux-x64.tar.gz for ubuntu from java's website) this is a useful [tutorial](https://www.javahelps.com/2015/03/install-oracle-jdk-in-ubuntu.html)
2. Install Apache Maven (I used version 3.6.0)
3. It's probably useful to have done the MinimalWordCount example through Apache's tutorial on their [website](https://beam.apache.org/get-started/wordcount-example/#minimalwordcount-example) and to look through some of the Apache Documentation to get an idea of the programming model.
4. An IDE is useful, VS Code is the one I use.

## Google Cloud Preliminaries
1. You need a google cloud account.
2. Know how to use/create these:
    * [Pub/Sub](https://cloud.google.com/pubsub/docs/overview) - Basically a cloud message queue implementation
    * DataFlow - A runner for the pipeline
    * Bucket - A cloud storage mechanism
    * BigQuery - Basically a cloud database.
3. Some useful articles
    * [Streaming with Pub/Sub](https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub)
    * [Apache Concepts / Programming Model](https://cloud.google.com/dataflow/docs/concepts/beam-programming-model)
    * [A long article on different streaming methods](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101)

## batch-working
Example of a batch pipeline, runs on the local runner so no cloud functionalities.

`BioStatsPipe.java` is the main class.

`Constants.java` encompasses all the constants for the classes.

`Utils.java` defines the validator for the input data.

`Person.java` defines a record for the data to be passed through the pipeline

`biostats.csv` is the input file

1. Clone the code to your local machine.
2. Run the following Maven command through the command line (no runner is specified, so it reverts to the local runner)
```
mvn compile exec:java -Dexec.mainClass=org.ambrite.josh.BioStatsPipe
```

## stream-working-pub-sub
This folder is an implementation of a streaming pipeline that is subscribed to a Pub/Sub Article whose messages are passed through a pipeline (DataFlow) and is then output to another Pub/Sub.

### Geting the streaming Pub/Sub Working
`BioStatsPipe.java` is the main class.

`Constants.java` encompasses all the constants for the classes.

`Utils.java` defines the validator for the input data.

`Person.java` defines a record for the data to be passed through the pipeline

`biostats.csv` is the input file (however we publish this file from the cloud storage as described later)

The functionality of this project will run similarly to that of the tutorial provided by [Google Cloud](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven).
	
1. Clone the code to your local machine.
2. Setup a Google Cloud Project `{PROJECT-ID}`. There are a few authentication processes that need to happen to see google cloud example and `GOOGLE_APPLICATION_CREDENTIALS`. 
3. Setup a Bucket to store output data `{BUCKET-ID}`
4. Setup a Pub/Sub Topic to listen to `{INPUT-TOPIC}`
5. Setup a Pub/Sub Topic to output to `{OUTPUT-TOPIC-FOR-INVALID-RECORDS}` and `{OUTPUT-TOPIC-FOR-VALID-RECORDS}`
6. Use the following Maven command through the command line (this starts the DataFlow stream)
```
mvn  -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=org.ambrite.josh.bioStatsPipe \
      -Dexec.cleanupDaemonThreads=false \
      -Dexec.args=" \
            --project={PROJECT-ID} \
            --stagingLocation=gs://{BUCKET-ID}/staging \
            --tempLocation=gs://{BUCKET-ID}/temp \
            --runner=DataflowRunner \
            --inputTopic=projects/{PROJECT-ID}/topics/{INPUT-TOPIC} \
            --invalidOutputTopic=projects/{PROJECT-ID}/topics/{OUTPUT-TOPIC-FOR-INVALID-RECORDS} \
            --validOutputTopic=projects/{PROJECT-ID}/topics/{OUTPUT-TOPIC-FOR-VALID-RECORDS}"
```
7. Now create a bucket to hold the `biostats.csv` input file, use GCP to navigate to the topic your runner is 'subscribed' to, in the top navbar find the IMPORT option and select 'Cloud Storage Text File' and complete the inputs, this publishes a message to the article, once this runs the data should be processed via the pipeline (you can view active jobs through [DataFlow](https://console.cloud.google.com/dataflow)) and final messages published to the output topics.

## stream-working-bigquery
This folder is an implementation of a streaming pipeline that is subscribed to a Pub/Sub Article whose messages are passed through the pipeline (DataFlow) and is then output to a Big Query table.

### Geting the streaming Pub/Sub Working
`BioStatsPipe.java` is the main class.

`Constants.java` encompasses all the constants for the classes.

`Utils.java` defines the validator for the input data.

`Person.java` defines a record for the data to be passed through the pipeline

`biostats.csv` is the input file (however we publish this file from the cloud storage as described later)


The functionality of this project will run similarly to that of the above implementation.
	
1. Clone the code to your local machine.
2. Setup a Google Cloud Project `{PROJECT-ID}`. There are a few authentication processes that need to happen to see google cloud example and `GOOGLE_APPLICATION_CREDENTIALS`. 
3. Setup a Bucket to store output data `{BUCKET-ID}`
4. Setup a Pub/Sub Topic to listen to `{INPUT-TOPIC}`
5. Setup a Big Query dataset `{BIG QUERY DATASET}` and table `{TABLE}`
6. The fields in the table need to adhere to the following:
    * name	: STRING
    * sex	: STRING	
    * age	: INT
    * weight	: INT
    * height	: INT
6. Use the following Maven command through the command line (this starts the DataFlow stream)
```
mvn  -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=org.ambrite.josh.BioStatsPipe \
      -Dexec.cleanupDaemonThreads=false \
      -Dexec.args=" \
            --project={PROJECT-ID} \
            --stagingLocation=gs://{BUCKET-ID}/staging \
            --tempLocation=gs://{BUCKET-ID}/temp \
            --runner=DataflowRunner \
            --inputTopic=projects/{PROJECT-ID}/topics/{INPUT-TOPIC-ID} \
            --bigQueryTable={PROJECT-ID}:{BIG QUERY DATASET}.{TABLE}"
```
7. Now create a bucket to hold the `biostats.csv` input file, use GCP to navigate to the topic your runner is 'subscribed' to, in the top navbar find the IMPORT option and select 'Cloud Storage Text File' and complete the inputs, this publishes a message to the article, once this runs the data should be processed via the pipeline (you can view active jobs through [DataFlow](https://console.cloud.google.com/dataflow)) and final data should end up in the Big Query Table.

## word-count-beam
This file contains the various quick start word count tutorials available on the [Apache website](https://beam.apache.org/get-started/wordcount-example/).
