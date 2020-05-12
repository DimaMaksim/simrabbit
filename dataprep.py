# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings("ignore")
from dataprep_lib import Dataprep,config
import pika
import json
import pandas as pd
params1={
	"SalesTransformationForSimulation":"NoOutOfStockMitigation",
    "SalesTypeForForecast":"NoFiltered",
    "Articule":117,
    "Filid": 1991,
    "StartDate": "2019-05-16",
    "EndDate": "2020-02-15",
    "DaysForHistory":90,
    "PredictionMethod":"BaseLine",
    "SafetyStock": "calculated",
    "Rasf":"BaseLine",
    "DaysForSigmaSS": 60,
    "DeliveryShedule": "BaseLine",
    "InitialStock": "BaseLine",
    "SSL": "BaseLine",
    "LeadTime": "BaseLine",
    "ApplyMPCoeff":"Y"
}


def main(params): 
    data=Dataprep(params)
    data.simulationpossibility()
    if data.return_dict=={}:
        data.order_shedule()
        data.mpcoefficients_godnost()
        data.rasfasovka()
        data.initstock_ssl()
        data.msp()
        data.choose_baseline_enginename()

    if data.return_dict=={}:
        return {'frame':data.df_all.to_json(),'method_name':data.method_name,'classid':int(data.classid)\
                ,'L':float(data.L),'T':float(data.T),'godnost':float(data.godnost)\
                ,'z':float(data.z),'SSL':float(data.SSL),'Rasf':float(data.Rasf),'InitialStock':float(data.InitialStock)}
    else:
        return data.return_dict

if __name__=='__main__' :
    credentials = pika.PlainCredentials(username=config['rabbit']['user'], password=config['rabbit']['pwd'])
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['rabbit']['host']\
                                            ,port=5672,virtual_host=config['rabbit']['vhost'],  credentials=credentials))
    channel = connection.channel()

    def on_request(ch, method, props, body):
        params=json.loads(body)
        response = main(params)
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = props.correlation_id),
                         body=json.dumps(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='DigitalCore.jobs.dataprep', on_message_callback=on_request)

    print("Awaiting RPC requests")
    channel.start_consuming()
