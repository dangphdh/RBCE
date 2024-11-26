
class Dataset():
    def __init__(self, name, data):
        self.name = name
        self.data = data
        

    def __str__(self):
        return f"Dataset: {self.name}, Data: {self.data}"