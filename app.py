from flask import Flask,request,jsonify
import json
import traceback
import datetime
import pandas as pd
import sys
from task import Task,prediction_service_url
import multiprocessing as mp
import requests
app = Flask(__name__)
@app.route("/metrics",methods=["GET","POST"])
def metrics():
	r=out["System"]
	return jsonify(r)


@app.route("/stock",methods=["GET","POST"])
def mytask():
	global out
	try:
		params=request.get_json()

	except Exception as e:
		return jsonify({'Not Json Object':f'{e}'}),400

	#for p in params:

	 #   if not(p in all_params_list):
	  #      return jsonify(f'{p}    Next: {l}')
	   #     return jsonify({'Wrong parameter name':f"""mistake in parameter {p}.
		#    Valid parameters: {all_params_list}"""}),404
	#if datetime.datetime.strptime(params["StartDate"], '%Y-%m-%d').date()>datetime.datetime.strptime(params["EndDate"], '%Y-%m-%d').date():
	#	return jsonify({'error in dates': ' EndDate must be greater then StartDate'})


	if params["PredictionMethod"] in ("BaseLine","ideal","XGB","test"):
		pass
	elif params["PredictionMethod"] in ["AOEngine01","AOEngine02","AOEngine03","AOEngine04","AOEngine05","AOEngine03Seasonal"]:
		method_name=params["PredictionMethod"]
	else:
		try:
			methods=json.loads(requests.get(f'{prediction_service_url}/params').content)
		except:
			return jsonify("Unable to connect Apollo")
		method=params['PredictionMethod']
		method_name=next(iter(method))
		if not (method_name) in methods:
			return jsonify({'error in method name': f'Wrong method name. Possible methods: "BaseLine,AOEngine01-05" or {methods}'})
		if len(methods[method_name])!=len(method[method_name]) and method_name!="clever_exp":
			return jsonify({'error in method name':f'Wrong number of arguments in method prediction, expected: {methods[method_name]}'})



	#if ('SafetyStockAlg' in params):
	#	if params['SafetyStockAlg'] not in SS_alg_names:
	#		return jsonify({'error in  SafetyStockAlg name':f'valid algorithm names: {SS_alg_names}'})

	if ('SafetyStock' in params):
		if (params['SafetyStock']!="calculated"):
			try:
				val=float(params['SafetyStock'])
			except Exception:
				return jsonify({'ValueError':'SafetyStock must by numeric or equals to \"calculated\"'})
			if val<0:
				return jsonify({'error in SafetyStock': 'SafetyStock can not be negative'})

	if ('SSL' in params):
		if (params['SSL']!="BaseLine"):
			try:
				val=float(params['SSL'])
			except Exception:
				return jsonify({'ValueError':'SSL must by numeric from [0,1] or equals to BaseLine'})
			if (val<0) or (val>1):
				return jsonify({'error in SSL': 'SSL must by numeric from [0,1]'})
	if ('InitialStock' in params):
		if (params['InitialStock']!="BaseLine"):
			try:
				val=float(params['InitialStock'])
			except Exception:
				return jsonify({'ValueError':'InitialStock must by numeric or equals to \"BaseLine\"'})
			if val<0:
				return jsonify({'error in InitialStock': 'InitialStock can not be negative'})


	if ('Mode_CheckupOrSimul' in params):
		if params['Mode_CheckupOrSimul'] not in ["C","S"]:
			return jsonify('Error in  Mode_CheckupOrSimul value. Valid Mode_CheckupOrSimul values: C or S ')

	if ('DeliveryShedule' in params):
		if (params['DeliveryShedule']!="BaseLine"):

			if (type(params['DeliveryShedule'])!=list) :
				return jsonify({'error in DeliveryShedule':"14-elements boolean list  and # week are expected or 'BaseLine'"})
			else:
				if len(params['DeliveryShedule'])!=2:
					return jsonify({'error in DeliveryShedule':"week number (1 or 2) is required"})
				table=params['DeliveryShedule'][0]
				if len(table)!=14:
					return jsonify({'error in DeliveryShedule':f"14-elements boolean list is expected but not {len(table)}-elements"})
				elif sum(table)==0:
					return jsonify({'error in DeliveryShedule':"all zeroes for DeliveryShedule is impossible"})
				try:
					week_number=params['DeliveryShedule'][1]
				except Exception:
					return jsonify({'error in week_number':'week_number is requered, 1 or 2'})
				if week_number not in (1,2):
					return jsonify({'error in week_number':'1 or 2 is expected'})


	if ('SalesTypeForForecast' in params):
		if params["SalesTypeForForecast"] not in ["Filtered","NoFiltered"]:
			return jsonify({f'Wrong value for SalesTransformationForSimulation': 'valid values: Filtered, NoFiltered'})


	if ('SalesTransformationForSimulation' in params):
		if params["SalesTransformationForSimulation"] not in ["OutOfStockMitigation","NoOutOfStockMitigation"]:
			return jsonify({f'{params["SalesTypeForForecast"]}   Wrong value for SalesTypeForForecast': 'valid values: OutOfStockMitigation, NoOutOfStockMitigation'})


	if params["Mode_CheckupOrSimul"]=="C":
		try:
			out_=inputcontrol(**params)

		except Exception as e:
			tb = traceback.format_exc()
			return jsonify({'error': f'{e}','traceback': f'{tb}'}),500
		else:
			return jsonify(out_)
	elif params["Mode_CheckupOrSimul"]=="S":
		try:


			manager = mp.Manager()
			return_dict = manager.dict()
			proc=mp.Process(target=Task,args=(params,return_dict,))
			proc.start()
			proc.join()
			out=return_dict["out"]


			#elif params["SeparateProcessLaunch"]==0:
			#	out=Task(params,{})
			#else:
 			#	return jsonify({'error in SeparateProcessLaunch value':'1 or 0 is expected'})

		except Exception as e:
			tb = traceback.format_exc()
			return jsonify({'error': f'{e}','traceback': f'{tb}'})
		else:
			#print("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKOOOOOOOOO")
			return jsonify(out)
if __name__ == "__main__":
	app.run(debug=True,host="0.0.0.0",port=5002)
