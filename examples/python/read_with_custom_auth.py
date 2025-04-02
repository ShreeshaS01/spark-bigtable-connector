# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import struct
import time
from typing import Tuple

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import BinaryType

# Constants
PROJECT_ID_PROPERTY_NAME = 'bigtableProjectId'
INSTANCE_ID_PROPERTY_NAME = 'bigtableInstanceId'
TABLE_NAME_PROPERTY_NAME = 'bigtableTableName'
CREATE_NEW_TABLE_PROPERTY_NAME = 'createNewTable'

def double_to_binary(value):
    """Convert a double to binary representation."""
    if value is None:
        return None
    return struct.pack('>d', value)

def binary_to_double(binary_data):
    """Convert binary data to a double."""
    if binary_data is None or len(binary_data) == 0:
        return float('nan')
    return struct.unpack('>d', binary_data)[0]

def get_catalog(table_name: str) -> str:
    """Generate the catalog JSON for BigTable."""
    return (f"""{{
        "table":{{"namespace":"default", "name":"{table_name}", "tableCoder":"PrimitiveType"}},
        "rowkey":"wordCol",
        "columns":{{
            "word":{{"cf":"rowkey", "col":"wordCol", "type":"string"}},
            "count":{{"cf":"example_family", "col":"countCol", "type":"long"}},
            "frequency_binary":{{"cf":"example_family", "col":"frequencyCol", "type":"binary"}}
        }}
    }}""")

def create_example_bigtable(spark: SparkSession, create_new_table: str,
                            project_id: str, instance_id: str, table_name: str):
    """Create an example BigTable with sample data."""
    double_to_binary_udf = F.udf(double_to_binary, BinaryType())

    # Create sample data - using 1000 records matching the Scala implementation
    data = [(f'word{i}', i, i / 1000.0) for i in range(1000)]
    df_with_double = spark.createDataFrame(data, ["word", "count", "frequency_double"])

    print("Created the DataFrame:")
    df_with_double.show()

    # Convert double to binary for storage
    df_to_write = (df_with_double
                   .withColumn("frequency_binary", double_to_binary_udf(F.col("frequency_double")))
                   .drop("frequency_double"))

    # Write to BigTable
    df_to_write.write \
        .format('bigtable') \
        .option('catalog', get_catalog(table_name)) \
        .option('spark.bigtable.project.id', project_id) \
        .option('spark.bigtable.instance.id', instance_id) \
        .option('spark.bigtable.create.new.table', create_new_table) \
        .save()

    print('DataFrame was written to BigTable.')

def parse_args() -> Tuple[str, str, str, str]:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(f'--{PROJECT_ID_PROPERTY_NAME}', help='Bigtable project ID.')
    parser.add_argument(f'--{INSTANCE_ID_PROPERTY_NAME}', help='Bigtable instance ID.')
    parser.add_argument(f'--{TABLE_NAME_PROPERTY_NAME}', help='Bigtable table name.')
    parser.add_argument(f'--{CREATE_NEW_TABLE_PROPERTY_NAME}', default='true',
                        help='Whether to create a new Bigtable table.')
    args = vars(parser.parse_args())

    bigtable_project_id = args.get(PROJECT_ID_PROPERTY_NAME)
    bigtable_instance_id = args.get(INSTANCE_ID_PROPERTY_NAME)
    bigtable_table_name = args.get(TABLE_NAME_PROPERTY_NAME)
    create_new_table = args.get(CREATE_NEW_TABLE_PROPERTY_NAME)

    if not (bigtable_project_id and bigtable_instance_id and bigtable_table_name):
        raise ValueError(
            f'Bigtable project ID, instance ID, and table ID should be specified '
            f'using --{PROJECT_ID_PROPERTY_NAME}=X, --{INSTANCE_ID_PROPERTY_NAME}=Y, '
            f'and --{TABLE_NAME_PROPERTY_NAME}=Z, respectively.'
        )

    return bigtable_project_id, bigtable_instance_id, bigtable_table_name, create_new_table

def main():
    """Main function to execute the BigTable read with custom auth."""
    # Parse command line arguments
    project_id, instance_id, table_name, create_new_table = parse_args()

    # Create Spark session
    spark = (SparkSession \
        .builder \
        .appName("BigtableReadWithCustomAuth") \
        .master("local[*]") \
        .getOrCreate())

    # Create example BigTable
    create_example_bigtable(spark, create_new_table, project_id, instance_id, table_name)

    # Reference to Scala CustomAccessTokenProvider class
    token_provider_class = "spark.bigtable.example.customauth.CustomAccessTokenProvider"

    try:
        # Give the token provider time to initialize in the JVM
        time.sleep(5)

        # Read from BigTable using the Scala CustomAccessTokenProvider
        read_df = spark.read \
            .format("bigtable") \
            .option("catalog", get_catalog(table_name)) \
            .option("spark.bigtable.project.id", project_id) \
            .option("spark.bigtable.instance.id", instance_id) \
            .option("spark.bigtable.gcp.accesstoken.provider", token_provider_class) \
            .load()

        print("Reading data from Bigtable...")
        read_df.show(50)

        # Add binary to double conversion
        binary_to_double_udf = F.udf(binary_to_double, returnType=F.DoubleType())
        read_df_with_double = (read_df
                               .withColumn("frequency_double", binary_to_double_udf(F.col("frequency_binary")))
                               .drop("frequency_binary"))

        print("Reading the DataFrame with converted doubles from Bigtable:")
        read_df_with_double.show(50)

        # Wait to allow token refresh to happen (mimicking the Scala test)
        print("Waiting for 20 seconds to test token refresh...")
        time.sleep(20)

        # Read again to test with possibly refreshed token
        print("Reading data again with potentially refreshed token...")
        read_df = spark.read \
            .format("bigtable") \
            .option("catalog", get_catalog(table_name)) \
            .option("spark.bigtable.project.id", project_id) \
            .option("spark.bigtable.instance.id", instance_id) \
            .load()

        read_df.show(5)  # Just show a few rows to verify it works

    except Exception as e:
        print(f"Error reading data: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()