import argparse
import datetime
import json
import logging
import time
from apache_beam.dataframe.convert import to_dataframe
import apache_beam as beam
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
import pandas as pd
from apache_beam.io import WriteToText #Streaming pipeline not supported yet for python sdk
from apache_beam.io import fileio

def roundTime(dt=None, roundTo=60):
   if dt == None : dt = datetime.datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return str(dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond))


class interpolateSensors(beam.DoFn):
  def process(self,sensorValues):
    (timestamp, values) =  sensorValues
    df = pd.DataFrame(values)
    df.columns = ["Sensor","Value"]
    json_string =  json.loads(df.groupby(["Sensor"]).mean().T.iloc[0].to_json())
    json_string["timestamp"] = timestamp
    return [json_string]

class convertToCsv(beam.DoFn):
  def process(self,sensorValues):
      (timestamp, values) =  sensorValues
      df = pd.DataFrame(values)
      df.columns = ["Sensor","Value"]
      csvStr =  str(timestamp)+","+",".join(str(x) for x in list(df.groupby(["Sensor"]).mean().T.iloc[0]))
      return csvStr

def run(subscription_name, output_table, output_gcs_path, interval=1.0, pipeline_args=None):
    schema = 'Timestamp:TIMESTAMP, PRESSURE_1:FLOAT, PRESSURE_2:FLOAT, PRESSURE_3:FLOAT, PRESSURE_4:FLOAT, PRESSURE_5:FLOAT'
    with beam.Pipeline(options=PipelineOptions( pipeline_args, streaming=True, save_main_session=True)) as p:
      data = (p
        | 'ReadData' >> beam.io.ReadFromPubSub(subscription=subscription_name)
        | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
        | "Convert to list" >> beam.Map(lambda x: x.split(","))
        | "to tuple" >> beam.Map(lambda x: (roundTime(datetime.datetime.strptime(x[0],'%Y-%m-%d %H:%M:%S.%f'), roundTo = interval),[x[1] , float(x[2])]))#{x.split(',')[1]:x.split(',')[2]}))
        | "Window" >> beam.WindowInto(window.FixedWindows(15))
        | "Groupby" >> beam.GroupByKey()

      )
      bq = (
        data
        | "Interpolate" >> beam.ParDo(interpolateSensors())
        | "Write to Big Query" >> beam.io.WriteToBigQuery(output_table,schema=schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND) 
      )

      gcs = (
        data
        | "convert to csv" >> beam.ParDo(convertToCsv())
        | "Write to GCS" >> fileio.WriteToFiles(output_gcs_path)
      )


if __name__ == "__main__": 
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--SUBSCRIPTION_NAME",
        help="The Cloud Pub/Sub subscription to read from.\n"
        '"projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>".',
    )
    parser.add_argument(
        "--BQ_TABLE",
        help = "Big Query Table Path.\n"
        '"<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>"')
    parser.add_argument(
        "--GCS_PATH",
        help = "Path to GCP cloud storage buket\n"
        '"<gs://<PROJECT_ID>/<BUCKET>/<PATH>"')
    parser.add_argument(
        "--AGGREGATION_INTERVAL",
        type = int,
        default = 1,
        help="Number of seconds to aggregate.\n",

    )
    args, pipeline_args = parser.parse_known_args()
    run(
        args.SUBSCRIPTION_NAME,
        args.BQ_TABLE,
        args.GCS_PATH,
        args.AGGREGATION_INTERVAL,
        pipeline_args
      )
