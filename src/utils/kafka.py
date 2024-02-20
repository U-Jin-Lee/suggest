import json
from kafka import KafkaProducer

def request_collect_serp_to_kafka(keyword):
    try:
        producer = KafkaProducer(bootstrap_servers="10.10.30.51, 10.10.30.52, 10.10.30.53", 
                                 value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'))
        topic = 'DS_SERP_DOWNLOAD'
        value = {"keyword":keyword , "usage_id":"intent", "domain":"serp_intent"}
        producer.send(topic, value=value)
        producer.flush()
        producer.close()
    except Exception as e:
        print(e)
    else:
        print(f'send_message_to_kafka : keyword : {keyword}')