import apache_beam as beam


data = ["this is sample data", "this is yet another sample data"]

pipeline = beam.Pipeline()
counts = (pipeline | "create" >> beam.Create(data)
    | "split" >> beam.ParDo(lambda row: row.split(" "))
    | "pair" >> beam.Map(lambda w: (w, 1))
    | "group" >> beam.CombinePerKey(sum))


output = []
def collect(row):
    output.append(row)
    return True

counts | "print" >> beam.Map(collect)


result = pipeline.run()
result.wait_until_finish()

print(output)
