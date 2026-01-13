import json
import boto3
import urllib.request
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='eu-west-2')
STREAM_NAME = 'crypto-price-stream'

def lambda_handler(event, context):
    
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=8'
    
    with urllib.request.urlopen(url) as response:
        coins = json.loads(response.read().decode('utf-8'))
    
    records = []
    for coin in coins:
        record = {
            'Data': json.dumps({
                'coin_id': coin['id'],
                'symbol': coin['symbol'],
                'name': coin['name'],
                'current_price': coin['current_price'],
                'market_cap': coin['market_cap'],
                'price_change_24h': coin['price_change_24h'],
                'price_change_percentage_24h': coin.get('price_change_percentage_24h', 0),
                'timestamp': datetime.utcnow().isoformat()
            }),
            'PartitionKey': coin['id']
        }
        records.append(record)
    
    bad_record_1 = {
        'Data': json.dumps({
            'coin_id': 'test-small-cap-coin',
            'symbol': 'tiny',
            'name': 'Small Market Cap Coin',
            'current_price': 0.50,
            'market_cap': 500000,
            'price_change_24h': -10,
            'price_change_percentage_24h': -5,
            'timestamp': datetime.utcnow().isoformat()
        }),
        'PartitionKey': 'test-small-cap-coin'
    }
    records.append(bad_record_1)
    
    bad_record_2 = {
        'Data': json.dumps({
            'coin_id': 'test-crashed-coin',
            'symbol': 'crash',
            'name': 'Crashed Coin',
            'current_price': 2.00,
            'market_cap': 5000000,
            'price_change_24h': -500,
            'price_change_percentage_24h': -18.5,
            'timestamp': datetime.utcnow().isoformat()
        }),
        'PartitionKey': 'test-crashed-coin'
    }
    records.append(bad_record_2)

    surge_record = {
        'Data': json.dumps({
            'coin_id': 'test-surge-coin',
            'symbol': 'moon',
            'name': 'Mooning Coin',
            'current_price': 100.00,
            'market_cap': 20000000,
            'price_change_24h': 25.0,
            'price_change_percentage_24h': 25.0,
            'timestamp': datetime.utcnow().isoformat()
        }),
        'PartitionKey': 'test-surge-coin'
    }
    records.append(surge_record)
    
    response = kinesis.put_records(
        StreamName=STREAM_NAME,
        Records=records
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f"Sent {len(records)} records to Kinesis. Failed: {response['FailedRecordCount']}")
    }