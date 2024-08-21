
class StreamConnector():
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def push_message(self, topic, message):
        pass

    @abstractmethod
    def close(self):
        pass