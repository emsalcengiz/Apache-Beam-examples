import apache_beam as beam


class LogElements(beam.PTransform):
    class _LoggingFn(beam.DoFn):

        def __init__(self, prefix=''):
            super(LogElements._LoggingFn, self).__init__()
            self.prefix = prefix

        def process(self, element, **kwargs):
            print(self.prefix + str(element))
            yield element

    def __init__(self, label=None, prefix=''):
        super(LogElements, self).__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._LoggingFn(self.prefix))

class MultiplyByTenDoFn(beam.DoFn):

    def process(self, element):
        yield element * 10

p = beam.Pipeline()

(p | beam.Create([1, 2, 3, 4, 5])
   | beam.ParDo(MultiplyByTenDoFn())
   | LogElements())

p.run()
