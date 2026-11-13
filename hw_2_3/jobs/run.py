import argparse
import json

from pipeline import execute, session


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--context', required=True)
    context = json.loads(parser.parse_args().context)
    spark = session()
    try:
        execute(spark, context)
    finally:
        spark.stop()
