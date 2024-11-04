from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import to_date, col, when
from pyspark.ml.feature import VectorAssembler
import logging


class AsteroidClassifier:
    def __init__(self, model=None):
        self.logger = logging.getLogger(__name__)
        self.model = model
        self.previous_train_data = None

    @staticmethod
    def prepare_data(batch_df):
        """Prepare the features and labels for ML model."""
        assembler = VectorAssembler(
            inputCols=[
                'estimated_diameter_km_min',
                'estimated_diameter_km_max',
                'miss_distance_km',
                'relative_velocity_km_s'
            ],
            outputCol='features')

        prepared_data = assembler.transform(batch_df)

        return prepared_data

    def train_or_update_model(self, train_data):
        """Train a new model or update the existing model with new batch data."""

        lr = LogisticRegression(
            labelCol='is_potentially_hazardous_asteroid',
            featuresCol='features'
        )

        # If the model does not exist (i.e. has not been trained yet), create model using train_data.
        if self.model is None:

            self.logger.info("Classification: Training new model.")
            self.model = lr.fit(train_data)
            self.logger.info(f"Classification: Model trained. "
                             f"Data points used to train model: {train_data.count()}.")

            # Store training data for future updates
            self.previous_train_data = train_data.cache()

        # If the model exists already, combine old and new training sets to retrain model.
        else:

            self.logger.info("Classification: Updating model with new batch.")
            self.logger.info(f"Classification: "
                             f"Previous training data: {self.previous_train_data.count()}. "
                             f"New training data: {train_data.count()}.")
            combined_data = self.previous_train_data.union(train_data).cache()
            self.logger.info(f"Classification: "
                             f"Combined data: {combined_data.count()}.")

            # Create model using new combined data set.
            self.model = lr.fit(combined_data)

            self.logger.info(f"Classification: Model retrained. "
                             f"Total data points used to train model: {combined_data.count()}.")

            # Store the new training data for future updates
            self.previous_train_data = combined_data.cache()

    def make_predictions(self, test_data):
        """Make predictions on test data."""
        self.logger.info("Classification: Transforming test data to obtain predictions.")
        predictions = self.model.transform(test_data)
        return predictions

    @staticmethod
    def evaluate_model(predictions):
        """Evaluate model accuracy."""
        evaluator = MulticlassClassificationEvaluator(
            labelCol='is_potentially_hazardous_asteroid', predictionCol='prediction', metricName='accuracy')
        accuracy = evaluator.evaluate(predictions)
        return accuracy

    @staticmethod
    def find_latest_date(predictions):
        """Find the latest approach date in data frame."""

        # Convert 'close_approach_date' from StringType to DateType
        df_with_date = predictions.withColumn('close_approach_date',
                                              to_date(col('close_approach_date'), 'yyyy-MM-dd'))

        # Sort the DataFrame by 'close_approach_date' in descending order and get the top row
        latest_date_row = df_with_date.orderBy(col('close_approach_date').desc()).first()

        # Extract the 'close_approach_date' value from the first row
        latest_close_approach_date = latest_date_row['close_approach_date']

        return latest_close_approach_date

    @staticmethod
    def print_predictions(predictions):
        """Format and display the results."""
        results_df = predictions.select(
            col("name"),
            col("is_potentially_hazardous_asteroid").alias("real_value"),
            col("prediction").alias("predicted_value"),
            col("estimated_diameter_km_min"),
            col("miss_distance_km")
        )

        results_df = (results_df
                      .withColumn("real_value", when(results_df.real_value == 1.0, 'Hazardous')
                                  .otherwise('Not Hazardous'))
                      .withColumn("predicted_value",
                                  when(results_df.predicted_value == 1.0, 'Hazardous')
                                  .otherwise('Not Hazardous')))

        results_df.show(truncate=False)
