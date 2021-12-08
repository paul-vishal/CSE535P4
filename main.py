import boto3
import json

comprehend = boto3.client(service_name='comprehend', region_name='us-east-2', aws_access_key_id='AKIARECXADUDPVTLEIN2', aws_secret_access_key='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')

text = "Hi all i am very sad today"

print('Calling DetectSentiment')
print(json.dumps(comprehend.detect_sentiment(Text=text, LanguageCode='en'), sort_keys=True, indent=4))
print('End of DetectSentiment\n')
