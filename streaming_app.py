import time  # Import the time module

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("SimpleJsonStreaming").getOrCreate()  # type: ignore

    # Define the schema for the input JSON data
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    # Define input and output paths
    input_path = "data/lz"
    output_path = "data/output"
    checkpoint_location = "data/checkpoint"  # Checkpoint directory for fault tolerance

    # Read the stream from the input directory
    # Spark will process any files added to this directory
    streaming_df = spark.readStream.schema(schema).json(input_path)

    # Add the 'name_length' column
    processed_df = streaming_df.withColumn("name_length", length(col("name")))

    # Write the stream to the output directory in JSON format
    query = (
        processed_df.writeStream.outputMode("append")
        .format("json")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    # Monitor the stream and stop after 60 seconds of inactivity
    print("Streaming query started. Monitoring for inactivity...")
    last_data_timestamp = time.time()
    timeout_seconds = 60

    while query.isActive:
        current_progress = query.lastProgress
        if current_progress and current_progress["numInputRows"] > 0:
            # Data was processed, reset the timer
            last_data_timestamp = time.time()
            print(
                f"Processed {current_progress['numInputRows']} rows. Resetting inactivity timer."
            )
        else:
            # No data processed in the last trigger interval
            elapsed_time = time.time() - last_data_timestamp
            if elapsed_time > timeout_seconds:
                print(
                    f"No new data received for {timeout_seconds} seconds. Stopping the stream."
                )
                query.stop()
                break  # Exit the monitoring loop

        # Wait for a short interval before checking again
        time.sleep(5)  # Check every 5 seconds

    print("Streaming query stopped.")
