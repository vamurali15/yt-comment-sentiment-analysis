import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import re
from datetime import datetime
from html import unescape
import yaml

# Load configuration
with open("config.yaml") as f:
    config = yaml.safe_load(f)

PROJECT_ID = config["project_id"]
BUCKET = config["bucket"]
BQ_DATASET = config["bq_dataset"]
BQ_TABLE = config["bq_table"]
INPUT_PATH = config["input_path"]  # e.g., gs://your-bucket/comments.ndjson

# Parse each NDJSON line
class ParseNDJSON(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            print(f"[DEBUG] Parsed record: {record}")
            yield record
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse JSON line: {e}")

# Clean and standardize comments
class CleanComment(beam.DoFn):
    def process(self, record):
        try:
            text = record.get("comment", "")
            text = unescape(text).lower()
            text = re.sub(r"http\S+", "", text)
            text = re.sub(r"[^a-z\s]", "", text)
            text = re.sub(r"\s+", " ", text).strip()

            cleaned = {
                "comment_id": record.get("id"),
                "comment_text": text,
                "published_at": datetime.utcnow().isoformat()
            }
            print(f"[DEBUG] Cleaned record: {cleaned}")
            yield cleaned
        except Exception as e:
            print(f"[ERROR] Failed to clean comment: {e}")

def run(argv=None):
    options = PipelineOptions(
        project=PROJECT_ID,
        region="us-central1",
        runner="DataflowRunner",
        temp_location=f"gs://{BUCKET}/temp",
        staging_location=f"gs://{BUCKET}/staging"
    )
    options.view_as(StandardOptions).streaming = False

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadNDJSON" >> beam.io.ReadFromText(INPUT_PATH)
            | "ParseNDJSON" >> beam.ParDo(ParseNDJSON())
            | "CleanComments" >> beam.ParDo(CleanComment())
            # Optional: uncomment for debug output to logs or GCS:
            # | "PrintOutput" >> beam.Map(print)
            # | "WriteToGCS" >> beam.io.WriteToText(f"gs://{BUCKET}/output/cleaned_comments", file_name_suffix=".json")
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
                schema="comment_id:STRING, comment_text:STRING, published_at:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == "__main__":
    run()
