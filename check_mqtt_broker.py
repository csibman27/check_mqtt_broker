#!/usr/bin/env python3

import paho.mqtt.client as mqtt
from datetime import datetime
import time

debug=False
mapOut=dict()

def convertNumberToMultiple_1024(number, suffix='B'):
    number=int(number)
    for unit in ['','Ki','Mi','Gi','Ti']:
      if abs(number) < 1024.0:
        return "%3.1f%s%s" % (number, unit, suffix)
      number /= 1024.0
    return number+suffix
                          
def convertNumberToMultiple_1000(number, suffix=''):
    number=int(number)
    for unit in ['','K','M','G','T']:
      if abs(number) < 1000.0:
        return "%3.1f%s%s" % (number, unit, suffix)
      number /= 1000.0
    return number+suffix

def check_mqtt_broker(params):
    hostname = params["hostname"]
    port = int(params.get("port", 1883))
    timeout = int(params.get("timeout", 10))

    try:
        # Create an MQTT client and set timeout
        client = mqtt.Client()
        client.connect(hostname, port, timeout)

        # Set a flag to track if the broker is alive
        broker_alive = False
        message_count = 0
        subscription_count = 0
        start_time = time.time()

        # Function to handle connection success
        def on_connect(client, userdata, flags, rc):
            nonlocal broker_alive
            if rc == 0:
                broker_alive = True
            else:
                broker_alive = False

        # Function to handle new messages
        def on_message(client, userdata, msg):
            nonlocal message_count
            message_count += 1
            mapOut[msg.topic]=str(msg.payload.decode("utf-8"))
            if debug:
                print("###############################")
                print("messages received ",str(msg.payload.decode("utf-8")))
                print("message topic=",msg.topic)
                print("message qos=",msg.qos)
                print("message reatin flag=",msg.retain)
                print("")

        # Function to handle new subscriptions
        def on_subscribe(client, userdata, mid, granted_qos):
            nonlocal subscription_count
            subscription_count += 1

        # Connect the client and wait for the connection
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_subscribe = on_subscribe
        client.loop_start()
        #client.subscribe("#", qos=0)
        mqtt_QoS=0
        client.subscribe(
          [
          ('$SYS/broker/clients/connected',mqtt_QoS),
          ('$SYS/broker/bytes/sent',mqtt_QoS),
          ('$SYS/broker/bytes/received',mqtt_QoS),
          ('$SYS/broker/load/bytes/sent/+',mqtt_QoS), #3 topics
          ('$SYS/broker/load/bytes/received/+',mqtt_QoS), #3 topics
          ('$SYS/broker/messages/sent',mqtt_QoS),
          ('$SYS/broker/messages/received',mqtt_QoS),
          ('$SYS/broker/load/messages/received/+',mqtt_QoS), #3 topics
          ('$SYS/broker/load/messages/sent/+',mqtt_QoS),  #3 topics
          ('$SYS/broker/messages/stored',mqtt_QoS),
          ('$SYS/broker/retained messages/count',mqtt_QoS),
          ('$SYS/broker/subscriptions/count',mqtt_QoS),
          ('$SYS/broker/uptime',mqtt_QoS),
          ('$SYS/broker/version',mqtt_QoS)
          ])

        client.publish('$SYS/broker/clients/connected', payload='check', qos=0)

        while time.time() - start_time < timeout:
            time.sleep(0.1)

        # Disconnect and stop the client loop
        client.disconnect()
        client.loop_stop(True)

        # Get the current datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Liveness check
        ver=mapOut['$SYS/broker/version']
        if broker_alive:
            print(f"0 MQTT_Running - OK - MQTT broker is alive at {now}. Version: {ver}")
        else:
            print(f"2 MQTT_Running - CRITICAL - MQTT broker is unreachable at {now}")
        
        # Check for number of connected clients
        clientsConnected=mapOut['$SYS/broker/clients/connected']
        print(f"0 MQTT_Clients - OK - Number of clients connected: {clientsConnected} at {now}")
        
        # Check for Total messages
        totalMessagesSent=mapOut['$SYS/broker/messages/sent']
        totalMessagesReceived=mapOut['$SYS/broker/messages/received']
        print(f"0 MQTT_TotalMessages MessagesSent={totalMessagesSent}|MessagesReceived={totalMessagesReceived}"  +
          "  - Messages since last start. Sent: " + 
              convertNumberToMultiple_1000(mapOut['$SYS/broker/messages/sent']) + 
              ", Received: " + 
              convertNumberToMultiple_1000(mapOut['$SYS/broker/messages/received'])
        )

        # Check messages stored
        print("0 MQTT_Messages_ActuallyStored " + 
          " TotActuallyStored_Messages=" + mapOut['$SYS/broker/messages/stored'] +
          "|ActuallyRetained_Messages=" + mapOut['$SYS/broker/retained messages/count']  +
          "  - Num. messages actually stored : " + 
              convertNumberToMultiple_1000(mapOut['$SYS/broker/messages/stored']) + 
              " (retained+durable), Num. retained messages: " + 
              convertNumberToMultiple_1000(mapOut['$SYS/broker/retained messages/count'])
        )

        # Check avarage messages
        if subscription_count > 0:
            average_messages = message_count / subscription_count
            print(f"0 MQTT_Average_Messages - OK - Average messages per subscription: {average_messages} at {now}")
        else:
            print(f"2 MQTT_Average_Messages - CRITICAL - No active subscriptions at {now}")

    except Exception as e:
        print(f"2 MQTT_Broker_Liveness - CRITICAL - Error connecting to MQTT broker: {str(e)}")

if __name__ == "__main__":
    check_mqtt_broker({"hostname": "localhost", "port": "1883", "timeout": "10"})
