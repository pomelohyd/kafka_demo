import json

from kafka import KafkaProducer
from kafka.errors import KafkaError


class my_producer():
    def __init__(self, kafkahost, kafkaport, kafkatopic):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
        )

        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers
                                      )

    def send_data(self, params,partition):
        try:
            parmas_message = json.dumps(params, ensure_ascii=False)
            producer = self.producer
            print(parmas_message)
            v = parmas_message.encode('utf-8')
            producer.send(self.kafkatopic, partition=partition, value=v)
            producer.flush()
        except KafkaError as e:
            print(e)
