from src.batch_processing import MemoryBatchProcessor
from flask import Flask, render_template, request
from pyspark.sql.functions import col, when
from datetime import datetime, timedelta
import plotly.express as px
import pandas as pd


class DashboardVisualization:
    def __init__(self):
        self.app = Flask(__name__)

    @staticmethod
    def process_data(data):
        processed_data = data.select(
            col("name"),
            col("close_approach_date"),
            col("estimated_diameter_km_max"),
            col("estimated_diameter_km_min"),
            col("relative_velocity_km_s"),
            col("miss_distance_km"),
            col("is_potentially_hazardous_asteroid").alias("real_value"),
            col("prediction").alias("predicted_value")
        )

        processed_data = (processed_data
                          .withColumn("real_value",
                                      when(processed_data.real_value == 1.0, 'Hazardous')
                                      .otherwise('Not Hazardous'))
                          .withColumn("predicted_value",
                                      when(processed_data.predicted_value == 1.0, 'Hazardous')
                                      .otherwise('Not Hazardous')))
        return processed_data

    @staticmethod
    def create_plot_real_values(df):
        """Create Plotly plot for real values."""
        fig = px.scatter(df, x='miss_distance_km', y='estimated_diameter_km_min',
                         color='real_value',
                         title='Real-time Asteroid Tracking (Real Values)')
        return fig.to_html(full_html=False)

    @staticmethod
    def create_plot_predicted_values(df):
        """Create Plotly plot for predicted values."""
        fig = px.scatter(df, x='miss_distance_km', y='estimated_diameter_km_min',
                         color='predicted_value',
                         title='Real-time Asteroid Tracking (Predicted Values)')
        return fig.to_html(full_html=False)

    @staticmethod
    def dataframe_to_html(df):
        """Convert the Spark DataFrame to HTML."""
        # Convert Spark DataFrame to Pandas for easier HTML conversion
        pandas_df = df.toPandas()  # Convert PySpark DataFrame to Pandas DataFrame
        return pandas_df.to_html(classes='table table-striped', index=False)

    def start_dashboard(self, memory_processor: MemoryBatchProcessor):
        """Start the Flask web server."""

        @self.app.route('/')
        def dashboard():

            data = memory_processor.get_latest_predictions()

            if data is None:
                return '<h1> No data available. </h1>'
            else:
                plot_data = self.process_data(data)
                plot_data_pd = plot_data.toPandas()
                start_date = datetime.now().date()
                total_points = memory_processor.get_total_points_used_for_training()
                accuracy = memory_processor.get_latest_accuracy()
                end_date = memory_processor.get_latest_close_approach_date() - timedelta(days=1)

                return render_template('dashboard.html',
                                       plot_real_values=self.create_plot_real_values(plot_data_pd),
                                       plot_predicted_values=self.create_plot_predicted_values(plot_data_pd),
                                       start_date=start_date,
                                       end_date=end_date,
                                       total_points=total_points,
                                       accuracy=accuracy)

        @self.app.route('/table')
        def table_view():

            data = memory_processor.get_latest_predictions()

            if data is None:
                return '<h1> No data available. </h1>'
            else:
                # Convert the DataFrame to HTML
                table_html = self.dataframe_to_html(data)
                start_date = datetime.now().date()
                end_date = memory_processor.get_latest_close_approach_date() - timedelta(days=1)
                return render_template('table.html',
                                       table_data=table_html,
                                       start_date=start_date,
                                       end_date=end_date)

        @self.app.route('/predict', methods=['GET', 'POST'])
        def predict():
            if request.method == 'POST':
                input_data = {
                    'estimated_diameter_km_min': float(request.form['estimated_diameter_km_min']),
                    'estimated_diameter_km_max': float(request.form['estimated_diameter_km_max']),
                    'relative_velocity_km_s': float(request.form['relative_velocity_km_s']),
                    'miss_distance_km': float(request.form['miss_distance_km'])
                }

                input_df = pd.DataFrame([input_data])

                # Make prediction using user inputs
                prediction = memory_processor.make_prediction_from_input(input_df)
                is_hazardous = "Hazardous" if prediction == 1.0 else "Not Hazardous"

                # Pass the prediction and input data back to the template
                return render_template('form.html', prediction=is_hazardous, input_data=input_data)
            return render_template('form.html')

        self.app.run(debug=True, use_reloader=False)
