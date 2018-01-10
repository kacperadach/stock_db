class MongoIndex():

    def __init__(self, name, index, unique=False):
        self.name = name
        self.index = index
        self.unique = unique

    def get_name(self):
        return self.name

    def get_index(self):
        return self.index

    def get_unique(self):
        return self.unique
