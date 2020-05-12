
import numpy as np
import pika
import json


credentials = pika.PlainCredentials(username='sim_dev_user', password='simdev')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='sftv-othe031.fz.fozzy.lan',port=5672,virtual_host='simdev',credentials=credentials))
channel = connection.channel()

par={
	"SalesTransformationForSimulation":"NoOutOfStockMitigation",
    "SalesTypeForForecast":"NoFiltered",
    "Mode_CheckupOrSimul": "S",
    "Articule":118,
    "Filid": 2407,
    "StartDate": "2019-10-16",
    "EndDate": "2020-02-15",
    "DaysForHistory":90,
    "PredictionMethod":{"exp":[0.1]},
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
import itertools
def atomtaskgen(macrotask):
	lagers=macrotask['lagers']
	filid_lagers=({'Filid':x[0],'Articule':x[1]} for x in lagers)

	jobs=macrotask['macro']['forexplore']
	jobs_list=[]
	for key in jobs:
	    l0=[{key:x} for x in jobs[key]]
	    jobs_list.append(l0)
	jobs_list_paired=list(itertools.product(*jobs_list))

	dates=macrotask['macro']['dates']
	par.update(dates)

	for lag in filid_lagers:

		params=par.copy()
		params.update(lag)
		for rest in jobs_list_paired:

			for r in rest:
				params.update(r)
			print(par)
			channel.basic_publish(exchange='',routing_key='DigitalCore.jobs.atomgen',body=json.dumps(params))


def callback(ch, method, properties, body):
    mes=json.loads(body)
    atomtaskgen(mes)
    ####channel.basic_publish(exchange='',routing_key='DigitalCore.jobs.atomgen',body=json.dumps(par))


channel.basic_consume(queue='DigitalCore.jobs.macrogen', on_message_callback=callback, auto_ack=True)


channel.start_consuming()
