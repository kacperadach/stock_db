class MongoIndex():

    def __init__(self, name, index, unique=False, expire_after_seconds=None):
        self.name = name
        self.index = index
        self.unique = unique
        self.expire_after_seconds = expire_after_seconds

    def get_name(self):
        return self.name

    def get_index(self):
        return self.index

    def get_unique(self):
        return self.unique

    def get_expire_after_seconds(self):
        return self.expire_after_seconds
