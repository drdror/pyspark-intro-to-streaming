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
    input_path = "data/input"
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

    # Wait for the stream to terminate (e.g., by manual interruption)
    query.awaitTermination()
