from __future__ import print_function
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import kafkaio

with beam.Pipeline(options=PipelineOptions()) as p:
    events = (p
                     | "Creating data" >> beam.Create([('dev_1', '{"device": "0003", status": "healthy"}')])
                     | "Pushing messages to Kafka" >> kafkaio.KafkaProduce(
                                                                            topic='my_love',
                                                                            servers="*.*.*.*:9092"
                                                                        )
                    )
    events | 'Writing to stdout' >> beam.Map(print)
