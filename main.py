from parse import get_config
from consumer import consumer

if __name__ == "__main__":
    config = get_config()
    print(config)

    consumer1 = consumer(config["source"], config["storageType"], config["destination"])
    consumer1.printSelf()