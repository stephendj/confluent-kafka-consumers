import os
import uuid
import json

from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from config import creditcardsettlements_fields
from util.postgres import Postgres
from util.sql import SqlTemplates

load_dotenv()

def postgres_value_converter(value):
    if isinstance(value, str):
        return f'\'{str(value)}\''
    if isinstance(value, list):
        return f'\'{json.dumps(value)}\''
    return str(value)

def sanitize_oplog_payload(message: dict):
    dictionary = {}
    for key, value in message.items():
        if key == '_id':
            dictionary['id'] = value['$oid']
        else:
            if isinstance(value, dict) and '$date' in value:
                dictionary[key] = datetime.utcfromtimestamp(value['$date'] / 1000.0) \
                                          .strftime("%Y-%m-%d %H:%M:%S")
            elif not isinstance(value, list):
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
        'group.id': 'connect-sink-jdbc-xendit-deposit-service-creditcardsettlements-v2',
        'auto.offset.reset': 'earliest',
        'schema.registry.url': os.getenv('SCHEMA_REGISTRY_URL')
    }

    postgres = Postgres(
        database=os.getenv('POSTGRES_DATABASE'), hostname=os.getenv('POSTGRES_HOST'),
        username=os.getenv('POSTGRES_USERNAME'), password=os.getenv('POSTGRES_PASSWORD')
    )

    # Execute initial table creation query
    postgres.execute_query(SqlTemplates.get('init_creditcardsettlements_table.sql'))

    consumer = AvroConsumer(config)
    consumer.subscribe(['creditcardsettlements-standalone'])

    try:
        while True:
            msg = consumer.poll(10)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            try:
                message_key = msg.key()
                message_value = msg.value()
                operation = message_value['op']

                #Insert operation
                if operation in ('c', 'r'):
                    cleaned_payload = sanitize_oplog_payload(json.loads(message_value['after']))

                # Update operation
                elif operation == 'u':
                    cleaned_payload = sanitize_oplog_payload(json.loads(message_value['patch'])['$set'])
                    cleaned_payload['id'] = json.loads(message_key['id'])['$oid']

                taken_fields = {}
                for key in creditcardsettlements_fields:
                    if key in cleaned_payload:
                        taken_fields[key] = cleaned_payload[key]

                key_list = list(taken_fields.keys())
                value_list = [postgres_value_converter(value) for value in taken_fields.values()]
                update_key_list = [
                    f'{key_list[i]}=EXCLUDED.{key_list[i]}'
                    for i in range(len(key_list))
                    if (key_list[i] != 'id')
                ]
                upsert_sql_statement = SqlTemplates.get('upsert_creditcardsettlements.sql').format(
                    keys=','.join(key_list),
                    values=','.join(value_list),
                    update_keys=','.join(update_key_list))
                postgres.execute_query(upsert_sql_statement)
            except Exception as e:
                print(e, msg.key(), msg.value())
                break
    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        postgres.close_connection()
        consumer.close()
