class AbstractTransformer:
    def __init__(self, conn, path):
        self.conn = conn
        self.path = path

    def setup(self):
        pass

    def load(self):
        pass

    def transform(self, values):
        return values

    def save(self, values):
        pass

    def cleanup(self):
        pass


class AbstractBatchTransformer:
    def __init__(self, conn, path):
        self.conn = conn
        self.path = path

    def getSize(self):
        raise RuntimeError('Batch transformer must implement getSize() method.')

    def getBatchSize(self):
        raise RuntimeError('Batch transformer must implement getBatchSize() method.')

    def setup(self):
        pass

    def load(self, offset, batchSize):
        pass

    def transform(self, values):
        return values

    def save(self, values):
        pass

    def cleanup(self):
        pass


def runTransformer(transformer, verbose=False):
    """
    :param transformer:
    :type transformer AbstractTransformer
    :param verbose
    :type verbose bool
    """
    transformer.setup()
    values = transformer.load()
    transformedValues = transformer.transform(values)
    transformer.save(transformedValues)
    transformer.cleanup()


def runBatchTransformer(transformer, verbose=False):
    """
    :param transformer:
    :type transformer AbstractBatchTransformer
    :param verbose
    :type verbose bool
    """
    transformer.setup()

    size = transformer.getSize()
    batchSize = transformer.getBatchSize()

    offset = 0
    while offset < size:
        if verbose:
            print 'offset', offset, 'out of', size

        values = transformer.load(offset, batchSize)
        transformedValues = transformer.transform(values)
        transformer.save(transformedValues)
        transformer.cleanup()
        offset += batchSize

    transformer.cleanup()
