




class Director:

    def __init__(self, source_bucket, source_key):
        self.source_bucket = source_bucket
        self.source_key = source_key
        self.builder = None
    
    def set_builder(self, builder):
        self.builder = builder
    
    def construct_model(self):
        self.builder.load_data(self.source_bucket, self.source_key)
        self.builder.transform_data()
        self.builder.load_data()