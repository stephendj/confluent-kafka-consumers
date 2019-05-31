import os
import uuid
import json

from dotenv import load_dotenv
from datetime import datetime
from config import creditcardsettlements_fields

from util.postgres import Postgres
from util.sql import SqlTemplates
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

load_dotenv()

def sanitize_oplog_payload(message: dict):
    dictionary = {}
    for key, value in message.items():
        if (key == '_id'):
            dictionary['id'] = value['$oid']
        else:
            if (type(value) is dict and '$date' in value):
                dictionary[key] = datetime.utcfromtimestamp(value['$date'] / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
            elif (type(value) is not list):
                dictionary[key] = value
            else:
                dictionary[key] = []
                for obj in value:
                    dictionary[key].append(sanitize_oplog_payload(obj))
    return dictionary

if __name__ == '__main__':
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config = {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
        'broker.version.fallback': '0.10.0.0',
        'api.version.fallback.ms': 0,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        'group.id': str(uuid.uuid1()),
        'auto.offset.reset': 'earliest',
        'schema.registry.url': os.getenv('SCHEMA_REGISTRY_URL')
    }

    postgres = Postgres(database=os.getenv('POSTGRES_DATABASE'), hostname=os.getenv('POSTGRES_HOST'),
        username=os.getenv('POSTGRES_USERNAME'), password=os.getenv('POSTGRES_PASSWORD'))

    # Execute initial table creation query
    postgres.execute_query(SqlTemplates.get('init_creditcardsettlements_table.sql'))

    consumer = AvroConsumer(config)
    consumer.subscribe(['creditcardsettlements_test'])

    try:
        while True:
            msg = consumer.poll(10)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            try:
                message_dict = msg.value()
                operation = message_dict['op']
                # Insert operation
                if (operation == 'c' or operation == 'r'):
                    cleaned_payload = sanitize_oplog_payload(json.loads(message_dict['after']))
                    taken_fields = {}
                    for key in creditcardsettlements_fields:
                        if (key in cleaned_payload):
                            taken_fields[key] = cleaned_payload[key]
                    
                    insertSqlStatement = SqlTemplates.get('insert_creditcardsettlements_full.sql').format(
                        keys=','.join(taken_fields.keys()),
                        values=','.join([
                            f'\'{str(taken_fields[key])}\'' if (type(taken_fields[key]) is str)
                            else f'\'{json.dumps(taken_fields[key])}\'' if (type(taken_fields[key]) is list)
                            else str(taken_fields[key])
                            for key in taken_fields.keys()]))
                    postgres.execute_query(insertSqlStatement)
                # Update operation
                elif (operation == 'u'):
                    # Skip for now
                    pass
            except Exception as e:
                print(e, msg.value())
                break
    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        postgres.close_connection()
        consumer.close()
