import boto3
import time

class consumer():
    def __init__(self, source, storageType, destination):
        self.source = source
        self.storageType = storageType
        self.destination = destination
        self.sourceClient = boto3.client('s3')
        self.destClient = boto3.client(storageType)

    def listen(self):
        timeEnd = time.time() = 30

        # listen for 30 seconds
        while time.time() < timeEnd:
