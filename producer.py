import paho.mqtt.client as mqtt
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def on_message(client, userdata, message):
    payload = json.loads(message.payload.decode("utf-8"))
    
    producer.send('helsinki-bus', payload)
    print('Sent data to Kafka')

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message
client.connect('mqtt.hsl.fi', 1883, 60)
client.subscribe("/hfp/v2/journey/ongoing/vp/bus/#")
client.loop_forever()
