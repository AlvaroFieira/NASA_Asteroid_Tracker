from src.stream_processing import SparkStreamProcessor
from src.batch_processing import MemoryBatchProcessor
from src.visualization import DashboardVisualization
from src.classification import AsteroidClassifier
from src.logging_config import setup_logging
from src.ingestion import NASADataIngestion
from dotenv import load_dotenv
import threading
import time
import os

# ---------------------------------------------------------------------------------------------------------------------#
# SCRIPT INITIALIZATION
# Load the environment variables and start the logger.

setup_logging()
load_dotenv()

NASA_NEO_API_KEY = os.getenv('NASA_NEO_API_KEY')
NASA_NEO_API_ENDPOINT = os.getenv('NASA_NEO_API_ENDPOINT')

# ---------------------------------------------------------------------------------------------------------------------#
# STREAM PROCESSING
# The first step is to initialize the kafka stream processor.
# This processor will read the stream and push the data into memory.

# Create stream processor object to consume data from kafka and add to memory
stream_processor = SparkStreamProcessor(
    kafka_server='localhost:9092',
    topic='neo-data'
)


def start_stream():
    stream_processor.start_stream()


# ---------------------------------------------------------------------------------------------------------------------#
# DATA INGESTION
# Once the stream processor has begun, we can start pushing new data to the stream.
# The data ingestion object will fetch data from the NASA NEO API and push it to the Kafka stream.

# Create ingestion object to fetch data from API and stream to Kafka
ingestion = NASADataIngestion(
    api_key=NASA_NEO_API_KEY,
    api_endpoint=NASA_NEO_API_ENDPOINT,
    kafka_server='localhost:9092',
    topic='neo-data'
)


# Once pyspark is reading from the stream, we can start ingesting data (ingestion)
def start_data_ingestion():
    while True:
        # Delay start of data ingestion to allow kafka stream processor to start
        time.sleep(15)

        # Fetch data from NASA NEO API
        data = ingestion.fetch_neo_data()

        # Stream the fetched data to kafka
        ingestion.stream_to_kafka(data)


# ---------------------------------------------------------------------------------------------------------------------#
# ML CLASSIFIER
# We now have data flowing into the Kafka stream and then into memory.
# The next step is to create an ML model to process the incoming data.
# The ML model is an incremental logistic regression classifier.
# It will predict if an Asteroid is hazardous/non-hazardous.

# Initialising Asteroid classifier
classifier = AsteroidClassifier()

# ---------------------------------------------------------------------------------------------------------------------#
# MEMORY BATCH PROCESSOR
# The next step is to extract the data from memory in batches, and to apply the ML model on the data.

# Initialise memory batch processor
memory_processor = MemoryBatchProcessor(classifier)


def start_batch_processing():
    # Delay to ensure pyspark stream processor and data ingestion have begun
    time.sleep(60)
    memory_processor.run(interval=45)  # Process the data every 60 seconds


# ---------------------------------------------------------------------------------------------------------------------#
# DASHBOARD VISUALIZATION
# Use flask to create an HTML dashboard that can be viewed on the browser.
# The dashboard has 3 tabs:
# 1. home: contains 2 graphs showing real vs predicted hazardousness for asteroids in the test set.
# 2. table: contains tabulated data for the test set displayed in the graphs on home page.
# 3. predict: enables users to assess the potential hazard of an asteroid based on their input data.

# Start flask dashboard to visualise model results
dashboard = DashboardVisualization()


def start_dashboard_visualization():
    dashboard.start_dashboard(memory_processor)


# ---------------------------------------------------------------------------------------------------------------------#
# START THREADING
# Create threads for each of the tasks above to allow the process to run in parallel.

# Create threads
stream_thread = threading.Thread(target=start_stream)
ingestion_thread = threading.Thread(target=start_data_ingestion)
batch_thread = threading.Thread(target=start_batch_processing)
dashboard_thread = threading.Thread(target=start_dashboard_visualization)

# Start threads
stream_thread.start()
ingestion_thread.start()
batch_thread.start()
dashboard_thread.start()

# Join the threads to ensure the main program waits for them to finish (if needed)
stream_thread.join()
ingestion_thread.join()
batch_thread.join()
dashboard_thread.join()
