from abc import ABC, abstractclassmethod


class BaseDataConnector(ABC):
    @abstractclassmethod
    def connect(self):
        pass

    @abstractclassmethod
    def get_data(self):
        pass

    @abstractclassmethod
    def update_data(self):
        pass

    @abstractclassmethod
    def push_data(self):
        pass

    @abstractclassmethod
    def remove_data(self):
        pass

    @abstractclassmethod
    def create_table(self):
        pass
