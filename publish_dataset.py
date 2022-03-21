import pandas as pd
import pika as pk

import json
import os


def publish_message(message):
    connection = pk.BlockingConnection(pk.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    EXCHANGE = 'PRODUCTS'

    channel.exchange_declare(exchange=EXCHANGE, exchange_type='fanout')
    channel.basic_publish(exchange=EXCHANGE, routing_key='', body=message)

    print(f'SENT MESSAGE: {message}')

    connection.close()


def process_csv(filename):
    split_filename = filename.split('.')[0]
    marketplace_codename = split_filename.split('_')[2]

    df = pd.read_csv(filename)
    for row in df.iterrows():
        data = row[1]
        message_dict = {
            'oldPrices': data.get('OLD_PRICES'),
            'price': data.get('PRICES'),
            'title': data.get('TITLE'),
            'image': data.get('IMAGE'),
            'link': data.get('LINK'),
            'marketplaceCodename': marketplace_codename.upper()
        }

        publish_message(json.dumps(message_dict))


def main():
    PATH_DATASET = 'dataset/'

    list_filename = list(filter(lambda f: f.endswith('.csv'), os.listdir(PATH_DATASET)))
    [process_csv(os.path.join(PATH_DATASET, filename)) for filename in list_filename]

if __name__ == '__main__':
    main()