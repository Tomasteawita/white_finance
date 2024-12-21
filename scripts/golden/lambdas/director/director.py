
class Director:

    def __init__(self, source_bucket, source_key):
        self.source_bucket = source_bucket
        self.source_key = source_key
        self.builder = None
    
    def set_builder(self, builder):
        self.builder = builder
    
    def construct_model(self):
        print('Getting sources')
        sources_dict = self.builder.get_sources()
        print('Building tables')
        print(sources_dict)
        self.builder.build_tables(sources_dict)