import boto3
import json
import base64
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
sqs = boto3.client('sqs', region_name='eu-west-2')
sns = boto3.client('sns', region_name='eu-west-2')
s3 = boto3.client('s3', region_name='eu-west-2')

GOOD_TABLE = 'crypto_good_records'
BAD_TABLE = 'crypto_bad_records'
DLQ_URL = 'https://sqs.eu-west-2.amazonaws.com/430006376054/crypto-queue'
SNS_TOPIC_ARN = 'arn:aws:sns:eu-west-2:430006376054:crypto-notifications'
BAD_DATA_BUCKET = 'crypto-bad-data'
BAD_DATA_PREFIX = 'bad_records'

# Thresholds using Decimal for precise comparison
MIN_PRICE = Decimal('0.01')
MAX_PRICE_DROP_PCT = Decimal('15.0')
MIN_MARKET_CAP = Decimal('1000000')

def lambda_handler(event, context):
    good_table = dynamodb.Table(GOOD_TABLE)
    bad_table = dynamodb.Table(BAD_TABLE)
    
    good_count = 0
    bad_count = 0
    alert_count = 0
    bad_records_for_s3 = []
    
    for record in event['Records']:
        try:
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data, parse_float=Decimal)
            
            coin_id = payload.get('coin_id', 'unknown')
            name = payload.get('name', 'Unknown')
            current_price = payload.get('current_price', Decimal('0'))
            market_cap = payload.get('market_cap', Decimal('0'))
            price_change_pct = payload.get('price_change_percentage_24h', Decimal('0'))
            timestamp = payload.get('timestamp', datetime.now(timezone.utc).isoformat())
            
            error_reasons = []
            if current_price <= 0:
                error_reasons.append("Invalid or zero price")
            elif current_price < MIN_PRICE:
                error_reasons.append(f"Price below threshold")
            
            if market_cap < MIN_MARKET_CAP:
                error_reasons.append(f"Market cap below threshold")
            
            if price_change_pct < -MAX_PRICE_DROP_PCT:
                error_reasons.append(f"Extreme price drop")
            
            if error_reasons:
                bad_item = {
                    'coin_id': coin_id,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error_reason': " | ".join(error_reasons),
                    'raw_data': json.loads(json.dumps(payload, default=str)) # Clean serializable dict
                }
                bad_table.put_item(Item=bad_item)
                bad_records_for_s3.append(bad_item)
                sqs.send_message(QueueUrl=DLQ_URL, MessageBody=json.dumps(bad_item, default=str))
                bad_count += 1
            else:
                # REVERTED: Converting numbers to str() for DynamoDB "S" type storage
                good_table.put_item(
                    Item={
                        'coin_id': coin_id,
                        'timestamp': timestamp,
                        'symbol': payload.get('symbol'),
                        'name': name,
                        'current_price': str(current_price),
                        'market_cap': str(market_cap),
                        'price_change_24h': str(payload.get('price_change_24h', Decimal('0'))),
                        'price_change_percentage_24h': str(price_change_pct)
                    }
                )
                good_count += 1
                
                if price_change_pct > Decimal('10'):
                    sns.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Subject=f'Crypto Alert | {name}',
                        Message=json.dumps(payload, indent=2, default=str)
                    )
                    alert_count += 1
                    
        except Exception as e:
            error_info = {
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'raw_data': raw_data
            }
            bad_records_for_s3.append(error_info)
            sqs.send_message(QueueUrl=DLQ_URL, MessageBody=json.dumps(error_info, default=str))
            bad_count += 1

    if bad_records_for_s3:
        now = datetime.now(timezone.utc)
        s3_key = f"{BAD_DATA_PREFIX}/dt={now:%Y-%m-%d}/hour={now:%H}/bad_{uuid.uuid4().hex}.jsonl"
        s3.put_object(
            Bucket=BAD_DATA_BUCKET,
            Key=s3_key,
            Body="\n".join(json.dumps(x, default=str) for x in bad_records_for_s3).encode("utf-8")
        )

    return {'statusCode': 200, 'good': good_count, 'bad': bad_count}