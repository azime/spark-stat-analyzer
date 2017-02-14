from includes.database import Database
import argparse
import config
from pyspark.sql import SparkSession
from includes import utils


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("-i", "--input", help="Directory.", required=True, type=utils.check_and_get_path)
    parser.add_argument("-s", "--start_date", help="The start date - format YYYY-MM-DD.",
                        type=utils.date_format, required=True)
    parser.add_argument("-e", "--end_date", help="The end date - format YYYY-MM-DD.",
                        type=utils.date_format, required=True)
    parser.add_argument("-a", "--analyzer", help="Analyzer name.",
                        required=True, type=utils.analyzer_value)

    args = parser.parse_args()
    database = Database(dbname=config.db["dbname"], user=config.db["user"],
                        password=config.db["password"], schema=config.db["schema"],
                        host=config.db['host'], port=config.db['port'])

    spark_context = SparkSession.builder.appName(__file__).getOrCreate()

    analyzer = args.analyzer(args.input, args.start_date, args.end_date, spark_context, database)
    analyzer.launch()
    analyzer.terminate()
