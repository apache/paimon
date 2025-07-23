from pypaimon.pynative.options import Options


class CatalogContext:
    def __init__(self, options: Options, hadoop_conf, prefer_loader, fallback_io_loader):
        self.options = options
        self.hadoop_conf = hadoop_conf
        self.prefer_io_loader = prefer_loader
        self.fallback_io_loader = fallback_io_loader

    @staticmethod
    def create(options: Options, hadoop_conf, prefer_loader, fallback_io_loader):
        return CatalogContext(options, hadoop_conf, prefer_loader, fallback_io_loader)

    @staticmethod
    def create_from_options(options: Options):
        return CatalogContext(options, None, None, None)
