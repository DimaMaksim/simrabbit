
import numpy as np
import pika
import json
import yaml
import datetime
lagers=np.array([[1991,117],[1991,118],[2022,117],[2022,118],[2069,3]])
with open("macrotask.yaml") as file:
    macro=yaml.full_load(file)



credentials = pika.PlainCredentials(username='sim_dev_user', password='simdev')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='sftv-othe031.fz.fozzy.lan',port=5672,virtual_host='simdev',credentials=credentials))
channel = connection.channel()



enco = lambda obj: (
    obj.isoformat()
    if isinstance(obj, datetime.datetime)
    or isinstance(obj, datetime.date)
    else None
)

body=json.dumps(dict(lagers=lagers.tolist(),macro=macro),default=enco)
channel.basic_publish(exchange='', routing_key='DigitalCore.jobs.macrogen', body=body)

connection.close()
