import paho.mqtt.client as mqtt
import json
import pandas as pd

messagesReceived = []
numMessages = 0

def on_message(client,userdata,message):
    data = json.loads(message.payload)
    global numMessages
    numMessages = numMessages + 1
    global messagesReceived
    messagesReceived.append(data)
    print(f"Message Received: {message.payload}")
    print(f"Messages Received: {numMessages}")
    if (numMessages == 3000):
        print('3000 msgs received')
        jsonData = json.dumps(messagesReceived)
        data = pd.read_json(jsonData)
        data.to_csv('./sensor_data.csv', index=False, columns = ['weight', 'humidity', 'temperature', 'prod_id'])
        print("Data captured to disk.  You can exit the application with Ctrl/Cmd+C.")

mqttc = mqtt.Client()

mqttc.on_message = on_message 

mqttc.connect("Enter broker URL here.") #Change to mqtt.cisco.com or 128.107.70.30 to access Cisco's test/public MQTT broker.

mqttc.on_connect = print('Connected to MQTT Broker.')

mqttc.subscribe("devnet/sensors")

mqttc.loop_forever() #start the loop

