from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
from typing import Optional
import logging
import time


# In spark transformations like union or count are lazily evaluated (not computed immediately)
# Using cache to store results in memory once it is computed once, this is done to avoid re-computation after each loop

class MemoryBatchProcessor:
    def __init__(self, classifier):
        self.logger = logging.getLogger(__name__)
        self.spark = SparkSession.builder.appName('MemoryBatchProcessor').getOrCreate()
        self.classifier = classifier
        self.latest_predictions: Optional[DataFrame] = None
        self.latest_accuracy = None
        self.latest_close_approach_date = datetime.now().date() - timedelta(days=1)

    def process_batch(self, batch_df):
        """Process the batch of data and update the ML model."""

        # If there's no data in the batch, return
        if batch_df.count() == 0:
            self.logger.info("Batch Processing: No new data to process.")
            return

        # Prepare the data for model training by creating features column
        self.logger.info("Batch Processing: Preparing data for ML model (creating features column).")
        prepared_data = self.classifier.prepare_data(batch_df).cache()

        # Split the data into train and test sets
        self.logger.info("Batch Processing: Splitting data into test and training sets.")
        train_data, test_data = prepared_data.randomSplit([0.8, 0.2])
        self.logger.info(f"Batch Processing: "
                         f"Data set: {prepared_data.count()}. "
                         f"Training data set: {train_data.count()}. "
                         f"Test data set: {test_data.count()}.")

        # Train or update the model
        self.logger.info("Batch Processing: Training/updating model.")
        self.classifier.train_or_update_model(train_data)

        # Make predictions on the test data
        self.logger.info("Batch Processing: Making new predictions using latest test data.")
        self.latest_predictions = self.classifier.make_predictions(test_data)

        # Evaluate the model
        self.logger.info("Batch Processing: Evaluating model using predictions.")
        self.latest_accuracy = self.classifier.evaluate_model(self.latest_predictions)
        self.logger.info(f"Batch Processing: Model updated. "
                         f"New accuracy: {self.latest_accuracy}.")

        # Obtain latest close approach date
        self.latest_close_approach_date = self.classifier.find_latest_date(self.latest_predictions)
        self.logger.info(f"Batch Processing: High watermark updated. "
                         f"Value: {self.latest_close_approach_date}")

    def get_total_points_used_for_training(self):
        """Find how many points have been used by the classifier to train the model."""
        return self.classifier.previous_train_data.count()

    def get_latest_accuracy(self):
        return self.latest_accuracy

    def make_prediction_from_input(self, input_df):
        """Make a prediction from user input."""

        print(input_df)

        input_df = self.spark.createDataFrame(input_df)

        # input_df.printSchema()
        # input_df.collect()
        # input_df.show()

        simple_data = [(1, "2024-10-20", 123456.0)]
        simple_df = self.spark.createDataFrame(simple_data, ["id", "close_approach_date", "miss_distance_km"])

        # Try running collect or show
        simple_df.show()
        simple_df.collect()

        # Prepare input data by assembling features
        prepared_input = self.classifier.prepare_data(input_df).cache()

        prepared_input.show()

        # Use the trained model to make predictions
        prediction = self.classifier.model.transform(prepared_input)

        prediction.show()

        # Extract the prediction result (hazardous or not)
        predicted_value = prediction.select('prediction').collect()[0][0]

        return predicted_value

    def get_latest_predictions(self):
        """Method to return the latest predictions."""
        return self.latest_predictions

    def get_latest_close_approach_date(self):
        """Method to return the latest close approach date used to train the model."""
        return self.latest_close_approach_date

    def run(self, interval=60):
        """Run the process to query memory table and update the model at regular intervals."""
        while True:
            # Query the in-memory table
            self.logger.info(f"Batch Processing: Extracting memory table. "
                             f"Low watermark: {self.latest_close_approach_date + timedelta(days=1)}.")
            batch_df = self.spark.sql(
                f"SELECT * FROM streaming_data "
                f"WHERE CAST(close_approach_date AS DATE) > CAST('{self.latest_close_approach_date}' AS DATE)"
            ).cache()

            batch_df.show()

            self.logger.info(f"Batch Processing: Memory table extracted. "
                             f"Low watermark: {self.latest_close_approach_date + timedelta(days=1)}. "
                             f"Rows: {batch_df.count()}.")

            # Process the batch
            self.logger.info(f"Batch Processing: Processing memory table.")
            self.process_batch(batch_df)

            # Sleep for the specified interval
            time.sleep(interval)
