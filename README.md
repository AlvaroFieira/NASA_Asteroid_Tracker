
# Near-Earth Object (NEO) Data Pipeline Project

## Project Overview
This project is a data pipeline system designed to process, classify, and visualize information about Near-Earth Objects (NEOs) collected from NASA's NEO API. The project ingests real-time data, processes it in memory, applies machine learning for classification, and provides a web-based dashboard for visualizing the results.

## Project Components
The project includes the following major components:

1. **Logging Configuration**
The logging setup is initialized at the beginning of the script using `setup_logging()` from `src.logging_config`. This allows for comprehensive logging throughout the data ingestion, processing, and classification tasks.

2. **Environment Configuration**
Environment variables, including API keys and endpoint URLs, are loaded via dotenv to secure access to NASA's NEO API.

3. **Stream Processing**
The `SparkStreamProcessor` class from `src.stream_processing` consumes data from a Kafka topic (neo-data) and processes the data into memory for further operations.

4. **Data Ingestion**
Data is fetched from NASA's NEO API using the `NASADataIngestion` class from `src.ingestion`. This component periodically requests new data and pushes it to the Kafka stream. This enables real-time data flow into the system.

5. **Machine Learning Classification**
Using an incremental logistic regression model provided by the `AsteroidClassifier` class from `src.classification`, the pipeline classifies NEOs as hazardous or non-hazardous based on incoming data.

6. **Memory Batch Processing**
The `MemoryBatchProcessor` class from `src.batch_processing` handles batch processing on data stored in memory. The classifier is applied to the data at specified intervals to continuously update predictions.

7. **Dashboard Visualization**
A dashboard built using Flask, provided by the `DashboardVisualization` class from `src.visualization`, visualizes the data and model results.

The dashboard has three tabs:

- Home: Shows real vs. predicted hazardousness of NEOs.
- Table: Displays a tabulated view of NEO data.
- Predict: Allows users to predict an asteroid’s hazardousness based on input data.

## Execution Flow

The main script initiates threading to execute the components in parallel. These threads enable the system to run in real time and handle large volumes of data efficiently:

1. **Stream Processing Thread:** Initializes Kafka streaming.
2. **Data Ingestion Thread:** Ingests data from NASA’s API and streams it to Kafka.
3. **Batch Processing Thread:** Performs batch processing at regular intervals.
4. **Dashboard Thread:** Launches the Flask dashboard.


## Requirements and Setup

To run this project, both the Python worker and driver must use the same version.
Ensure that the following tools and versions are installed and configured:

- Python: 3.10.0 (Used in both the worker and driver)
- PySpark: 3.5.3
- Scala: 2.12.18
- Java: 11.0.25

### Java Setup
1. Download Java 11 from Oracle’s website https://www.oracle.com/in/java/technologies/downloads/#java11-windows
2. Install Java and add the Java binary path to system environment variables as JAVA_HOME.

### Kafka Setup
1. Download the latest Kafka from Apache’s website.
https://www.apache.org/dyn/closer.cgi?path=/kafka/3.8.0/kafka_2.13-3.8.0.tgz

2. Extract the contents, rename the folder to `kafka`.

3. Open command prompt from within this new kafka folder `c:\{dir}\kafka` and type the following to start zookeeper to synchronise brokers:
    ```bash
    .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

4. Open another command prompt `c:\{dir}\kafka`, type the following to start kafka server:
    ```bash
    .\bin\windows\kafka-server-start.bat .\config\server.properties

5. Open another command prompt `c:\{dir}\kafka\bin\windows`, type the following to create topic named `neo-data`:
    ```bash
    kafka-topics.bat --create --bootstrap-server localhost:9092 --topic neo-data

6. In same command prompt, type the following to create producer:
    ```bash
    kafka-console-producer.bat --broker-list localhost:9092 --topic neo-data

7. Open another command prompt `c:\{dir}\kafka\bin\windows` and type the following to create consumer:
    ```bash
    kafka-console-consumer.bat --topic neo-data --bootstrap-server localhost:9092 --from-beginning

8. Anything you write in the producer terminal should appear in the consumer terminal.

### Spark Setup
1. Download latest spark: https://www.apache.org/dyn/closer.lua/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
2. Download winutils for downloaded version of hadoop: https://github.com/steveloughran/winutils
3. Save `winutils.exe` and `hadoop.dll` in a folder named bin within a folder named `hadoop`.
4. Create the following system variables (add to Path system variable as well):
- `HADOOP_HOME = C:\Users\{user}\hadoop\bin`
- `JAVA_HOME = C:\Program Files\Java\jdk-11\bin`
- `SPARK_HOME = C:\Users\{user}\spark\spark-3.5.3-bin-hadoop3\bin`
- `PYSPARK_HOME = C:\Users\{user}\AppData\Local\Programs\Python\Python310\python.exe`

### Running the Project
1. Install the required Python packages listed in `requirements.txt`.
2. Ensure environment variables `NASA_NEO_API_KEY`, `NASA_NEO_API_ENDPOINT` are set.
3. Run the `main.py` script to start the data pipeline.

### Notes
1. Make sure that Kafka and Spark are running before starting the pipeline.
2. This setup is essential for real-time data processing and visualization.
