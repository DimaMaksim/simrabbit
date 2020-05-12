# -*- coding: utf-8 -*-

import sys
import warnings
warnings.filterwarnings("ignore")
import json
import pandas as pd
import pika
import uuid
from task_lib import fifo, Gvidon


def Task(params_dict):
    gvidon=Gvidon(return_dict,params_dict)
    reply_rpc = DataTroughtRpc()
    params_in = reply_rpc.call(params_dict)
    if len(params_in)==1:
        return params_in
    else:
        gvidon.df_all=pd.read_json(params_in['frame'])
        gvidon.n=len(gvidon.df_all)
        gvidon.L=params_in['L']
        gvidon.T=params_in['T']
        gvidon.godnost=params_in['godnost']
        gvidon.z=params_in['z']
        gvidon.Rasf=params_in['Rasf']
        gvidon.InitialStock=params_in['InitialStock']
        gvidon.method_name=params_in['method_name']
        gvidon.classid=params_in['classid']
        gvidon.predict()
        details=gvidon.good_names()
        metrics=gvidon.fmetrics()
        if gvidon.DetailsShow=="Y":
            out={'1.Metrics':metrics,'2.Details':details.to_dict(orient='records')}
        elif gvidon.DetailsShow=="N":
            out=metrics
        else:
            out={'Wrong value for DetailsShow': 'valid values: Y, N'}

    return out

class DataTroughtRpc:
    def __init__(self):
        credentials = pika.PlainCredentials(username='sim_dev_user', password='simdev')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='sftv-othe031.fz.fozzy.lan'\
                                                ,port=5672,virtual_host='simdev',  credentials=credentials))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(queue=self.callback_queue,on_message_callback=self.on_response,auto_ack=True)
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
    def call(self, params):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='DigitalCore.jobs.dataprep',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(params))
        while self.response is None:
            self.connection.process_data_events()
        return json.loads(self.response)


return_dict={}
credentials = pika.PlainCredentials(username='sim_dev_user', password='simdev')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='sftv-othe031.fz.fozzy.lan',port=5672,virtual_host='simdev',credentials=credentials))
channel = connection.channel()
def callback(ch,method,properties,body):
    #return_dict={}
    params=json.loads(body)
    out=Task(params)
    print(out)

channel.basic_consume(queue='DigitalCore.jobs.atomgen', on_message_callback=callback, auto_ack=True)
channel.start_consuming()

# отдельно:
params0={
	"SalesTransformationForSimulation":"NoOutOfStockMitigation",
    "SalesTypeForForecast":"NoFiltered",
    "Mode_CheckupOrSimul": "S",
    "Articule":117,
    "Filid": 2022,
    "StartDate": "2020-01-01",
    "EndDate": "2020-02-15",
    "DaysForHistory":90,
    "PredictionMethod":{"14_prev_days":[]},
    "SafetyStockAlg": "BaseLine",
    "SafetyStock": "calculated",
    "Rasf":"BaseLine",
    "DaysForSigmaSS": 60,
    "DeliveryShedule": "BaseLine",
    "InitialStock": "BaseLine",
    "SSL": "BaseLine",
    "LeadTime": "BaseLine",
    "PromoProcessing": "Deactive",
    "DetailsShow":"N",
    "FirstSupply":1,
    "UsePrevWeekDaysforBaseline": "N",
    "Offs":"Y",
    "ApplyMPCoeff":"Y"
}

#return_dict={}
#out=Task(params0)
#print(out)
