import logging

import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
import urllib.request
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    dynamodb = boto3.resource('dynamodb')
    dynamodb_client = boto3.client('dynamodb')
    
    for message_event in json.loads(event['body'])['events']:
        
        url = 'https://api.line.me/v2/bot/message/reply'

        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + os.environ['ACCESSTOKEN']
        }
        
        if message_event['message']['text'] == "予約":
            
            data = {
                'replyToken':message_event['replyToken'],
                'messages':[{
                    "type":"text",
                    "text":"お名前を教えてください"
                }]
            }
            
            # テーブルを作成します
        
            table = dynamodb.create_table(
            
                TableName=message_event['source']['userId'],
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'
                    },
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'N'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1
                },
            )

        else:
            try: #テーブルが作成されている時
                response = dynamodb_client.describe_table(TableName=message_event['source']['userId'])  
                
                table = dynamodb.Table(message_event['source']['userId'])
                
                queryData = table.query(
                    KeyConditionExpression = Key("id").eq(1)
                )
                
                logger.info(queryData)
                
                # Itemsがまだ入ってなかったら
                if not queryData['Items']:
                    
                    table.put_item(
                        Item = {
                            'id': 1,
                            'name':message_event['message']['text'],
                            'people':"empty",
                            'reserve_date':"empty",
                        }
                    )
                    
                    data = {
                    'replyToken':message_event['replyToken'],
                    'messages':[{
                        "type":"text",
                        "text":"人数を教えてください",
                        "quickReply": {
                            "items": [
                                {
                                    "type": "action",
                                    "action": {
                                        "type": "message",
                                        "label": "1",
                                        "text": "1"
                                    }
                                },
                                {
                                    "type": "action",
                                    "action": {
                                        "type": "message",
                                        "label": "2",
                                        "text": "2"
                                    }
                                },
                                {
                                    "type": "action",
                                    "action": {
                                        "type": "message",
                                        "label": "3",
                                        "text": "3"
                                    }
                                },
                                {
                                    "type": "action",
                                    "action": {
                                        "type": "message",
                                        "label": "4",
                                        "text": "4"
                                    }
                                },
                            ] #quickReply
                        } #quickReply
                    }]
                }
                
                elif queryData['Items'][0]['people'] == 'empty':

                    table.update_item(
                        
                        # primary_keyのみでオッケー
                        Key = {
                            'id': 1
                        },
                        
                        UpdateExpression="set people = :people",
                
                        ExpressionAttributeValues={
                            ':people': message_event['message']['text']
                        },
                        
                    )
                    
                    data = {
                        'replyToken':message_event['replyToken'],
                        'messages':[{
                            "type":"text",
                            "text":"何時ですか？"
                        }]
                    }

                elif queryData['Items'][0]['reserve_date'] == 'empty':
                    
                    table.update_item(
                        
                        # primary_keyのみでオッケー
                        Key = {
                            'id': 1
                        },
                        
                        UpdateExpression="set reserve_date = :reserve_date",
                
                        ExpressionAttributeValues={
                            ':reserve_date': message_event['message']['text']
                        },
                        
                    )
                    
                    data = {
                            'replyToken':message_event['replyToken'],
                            'messages':[{
                                "type":"text",
                                "text":"氏名 : " + queryData['Items'][0]['name'] + '\n' +
                                    "人数 : " + queryData['Items'][0]['people'] + '\n' +
                                    "日付 : " + message_event['message']['text'] + '\n' +
                                    "💁こちらの内容で予約してもよろしいですか？",
                                "quickReply": {
                                    "items": [
                                        {
                                            "type": "action",
                                            "action": {
                                                "type": "message",
                                                "label": "はい",
                                                "text": "はい"
                                            }
                                        },
                                        {
                                            "type": "action",
                                            "action": {
                                                "type": "message",
                                                "label": "いいえ",
                                                "text": "いいえ"
                                            }
                                        },
                                    ] #quickReply
                                } #quickReply
                            }]
                        }

                elif message_event['message']['text'] == "はい":
                        
                    data = {
                        'replyToken':message_event['replyToken'],
                        'messages':[{
                            "type":"text",
                            "text":"予約完了しました。当日お待ちしております😋"
                        }]
                    }

                elif message_event['message']['text'] == "いいえ":
                    
                    table.delete()
                    
                    data = {
                        'replyToken':message_event['replyToken'],
                        'messages':[{
                            "type":"text",
                            "text":"入力内容を取り消しました。" + '\n' +
                                   "初めからやり直してください"
                        }]
                    }

                else:
                    
                    data = {
                        'replyToken':message_event['replyToken'],
                        'messages':[{
                            "type":"text",
                            "text":"氏名 : " + queryData['Items'][0]['name'] + '\n' +
                                   "人数 : " + queryData['Items'][0]['people'] + '\n' +
                                   "日付 : " + queryData['Items'][0]['reserve_date'] + '\n' +
                                   "💁こちらの内容で予約しております"+ '\n' +
                                   "キャンセルされる方は電話でお願いします📞"
                        }]
                    }
                
            except dynamodb_client.exceptions.ResourceNotFoundException: #テーブルが作成されていない時
                
                data = {
                    'replyToken':message_event['replyToken'],
                    'messages':[{
                        "type":"text",
                        "text":"予約されますか？😊"+ '\n' +
                               "する場合は「予約」と送信してください"
                    }]
                }
            
        req = urllib.request.Request(url=url, data=json.dumps(data).encode('utf-8'), method='POST', headers=headers)
            
        with urllib.request.urlopen(req) as res:
                
            logger.info(res.read().decode("utf-8"))
                