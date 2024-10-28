import boto3
import time
import json

class consumer():
    def __init__(self, source, storageType, destination):
        self.source = source
        self.storageType = storageType
        self.destination = destination
        self.sourceClient = boto3.client('s3')
        self.destClient = boto3.client(storageType, region_name="us-east-1")

    def listen(self):
        timeEnd = time.time() + 30

        # listen for 30 seconds
        while time.time() < timeEnd:
            # check if there are any requests to process
            response = self.sourceClient.list_objects_v2(Bucket=self.source)

            if 'Contents' in response:
                # 1. find the smallest key
                smallestKey = sorted(response['Contents'], key=lambda x: x["Key"])[0]['Key']

                # 2. using the smallest key obtained in the last step, get the request
                request = self.sourceClient.get_object(Bucket=self.source, Key=smallestKey)

                # 3. decode request into a json object that we can read and work with
                request_data = json.loads(request['Body'].read().decode('utf-8'))
                
                # 4. process the request
                self.processRequest(request_data)

                # 5. delete the request from the requests bucket after it has been processed
                self.sourceClient.delete_object(Bucket=self.source, Key=smallestKey)
            else:
                # wait for 1/10 of a second before checking again if there are no requests to process
                time.sleep(0.1)

    # routes the request to a processor based on the type of request it is
    def processRequest(self, request):
        requestType = request['type']
        # see if we need to send this to 
        if requestType == "create":
            self.create(request)
        if requestType == "delete":
            pass
        if requestType == "update":
            pass

    def create(self, widget):
        if self.storageType == 's3':
            owner = widget['owner'].replace(" ", "-").lower()
            widget_key = f"widgets/{owner}/{widget['widgetId']}"
            self.destClient.put_object(Bucket=self.destination, Key=widget_key, Body=json.dumps(widget))
        else:
            item = {
                'id': {'S': widget['widgetId']},
                'owner': {'S': widget['owner']},
                'label': {'S': widget['label']},
                'description': {'S': widget['description']}
            }

            # Handle otherAttributes
            if 'otherAttributes' in widget:
                for attribute in widget['otherAttributes']:
                    item[attribute['name']] = {'S': attribute['value']}

            print(item)

            self.destClient.put_item(TableName=self.destination, Item=item)