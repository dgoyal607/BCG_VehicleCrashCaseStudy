from pyspark.sql import SparkSession


def read_csv_data(spark, input_file_path):

    """
    Reads data from a CSV file into a Spark DataFrame.

    Args:
    - spark (SparkSession): The SparkSession object.
    - input_file_path (str): The path to the CSV file to be read.

    Returns:
    - DataFrame: The DataFrame containing the data read from the CSV file.
    """
    return spark.read.csv(input_file_path, header=True, inferSchema=True)


def write_df(df, output_file_path, file_format):

    """
    Writes a DataFrame to an output file in the specified format.

    Args:
    - df (DataFrame): The DataFrame to be written to the output file.
    - output_file_path (str): The path to the output file.
    - file_format (str): The format in which the DataFrame should be saved (e.g., 'parquet', 'csv').

    Returns:
    - None
    """
    df.write.format(file_format).mode('overwrite').option('header', True).save(output_file_path)


