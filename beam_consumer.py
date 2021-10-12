import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import kafkaio

consumer_config = {"topic": "my_love",
                   "bootstrap_servers": "*.*.*.*:9092",
                   "group_id": "notification_consumer_group"}

with beam.Pipeline(options=PipelineOptions()) as p:
    events = p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(
        consumer_config=consumer_config,
        value_decoder=bytes.decode,  # optional
    )
    events | 'Writing to stdout' >> beam.Map(print)
