from includes.database import Database
from includes.config import Config
import argparse
from pyspark.sql import SparkSession
from includes import utils
from datetime import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("-i", "--input", help="Directory.", required=True, type=utils.check_and_get_path)
    parser.add_argument("-s", "--start_date", help="The start date - format YYYY-MM-DD.",
                        type=utils.date_format, required=True)
    parser.add_argument("-e", "--end_date", help="The end date - format YYYY-MM-DD.",
                        type=utils.date_format, required=True)
    parser.add_argument("-a", "--analyzer", help="Analyzer name.",
                        required=True, type=utils.analyzer_value)
    parser.add_argument("-c", "--config", help="config file name.",
                        required=True, type=utils.check_and_get_file)

    args = parser.parse_args()

    config = Config(args.config)

    database = Database(dbname=config.dbname, user=config.user,
                        password=config.password, schema=config.schema,
                        host=config.host, port=config.port,
                        insert_count=config.insert_count)

    spark_session = SparkSession.builder.appName(__file__).getOrCreate()

    analyzer = args.analyzer(args.input, args.start_date, args.end_date, spark_session, database)
    analyzer.launch()
    analyzer.terminate(datetime.now())
