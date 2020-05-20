# Copyright (c) 2020 OpenBlender.io
# Simplicity is key.


from urllib.request import Request, urlopen
from base64 import b64encode, b64decode
from urllib.request import urlopen
from urllib.parse import urlencode
from contextlib import closing
from datetime import datetime
import pandas as pd
import numpy as np
import traceback
import requests
import time
import math
import json
import zlib
import os

VERSION = 1.15

def dameRespuestaLlamado(url, data):
	respuesta = ''
	with closing(urlopen(url, data)) as response:
		respuesta = json.loads(response.read().decode())
		if 'base64_zip' in respuesta:
			try:
				respuesta = json.loads(zlib.decompress(b64decode(respuesta['base64_zip'])).decode('utf-8'))
			except:
				respuesta = json.loads(zlib.decompress(b64decode(respuesta['base64_zip'])))
	try:
		if 'error' in respuesta['status']:
			print("------------------------------------------------")
			print("API call error: " + str(respuesta['response']))
			print("------------------------------------------------")
			print("")
			return False
	except:
		print("--Internal error. Please upgrade OpenBlender verison via Pip.--")
		print("-----")
		#print(traceback.format_exc())
	return respuesta


def call(action, json_parametros):
	respuesta = ''
	try:
		respuesta = ''
		url = ''
		if 'oblender' in json_parametros and json_parametros['oblender'] == 1:
			url = 'http://3.16.237.62:8080/bronce'
		else:
			url = 'http://52.8.156.139/oro/'
		#print(url)
		if action == 'API_createDataset':
			respuesta = API_createDataset(json_parametros, url)
		elif action == 'API_insertObservations':
			respuesta = API_insertObservationsFromDataFrame(json_parametros, url)
		elif action == 'API_getObservationsFromDataset':
			respuesta = API_getSampleObservationsFromDataset(json_parametros, url)
		elif action == 'API_powerModel':
			respuesta = API_powerModel(json_parametros, url)
		elif action == 'API_getDataWithVectorizer':
			respuesta = API_getSampleObservationsWithVectorizer(json_parametros, url)
		elif action == 'API_getSampleObservationsWithVectorizer':
			respuesta = API_getSampleObservationsWithVectorizer(json_parametros, url)
		elif action == 'API_getOpenTextData':
			respuesta = API_getOpenTextData(json_parametros, url)
		else:
			data = urlencode({'action' : action, 'json' : json.dumps(json_parametros), 'compress' : 1}).encode()
			respuesta = dameRespuestaLlamado(url, data)
		return respuesta
	except Exception as e:
		if 'oblender' in json_parametros and json_parametros['oblender'] == 1:
			print(json.dumps({"status": "internal error", "msg": (e)}))
		else:
			print(json.dumps({"status": "internal error", "msg": str(e)}))
		return json.dumps({"status": "internal error", "msg": str(e)})
    
def API_createDataset(json_parametros, url):
	respuesta = ''
	action = 'API_createDataset'
	nom_obs = 'dataframe' if 'dataframe' in json_parametros else 'observations'
	df, valido_df, msj = comprobarJSONaDF(json_parametros[nom_obs])
	if not valido_df:
		return msj
	n_filas = df.shape[0]
	tam_pedazo_ini = 1000
	insert_observations = True
	json_particion = json_parametros.copy()
	if 'insert_observations' in json_parametros:
		insert_observations = json_parametros['insert_observations'] == 1 or json_parametros['insert_observations'] == 'on'
		
	test_call = 1 if 'test_call' in json_parametros and (json_parametros['test_call'] == 1 or json_parametros['test_call'] == 'on') else False
	
	if test_call == 1:
		print("")
		print('This is a TEST CALL, set "test_call" : "off" or remove to execute service.')
		print("")
	respuesta0 = None
    
	# Primer pedazo para crear el dataset
	if not test_call and n_filas > tam_pedazo_ini:
		if insert_observations:
			start = time.time()
			tam_pedazo_ini = tam_pedazo_ini if n_filas > tam_pedazo_ini else n_filas
			
			json_particion[nom_obs] = df.sample(n=tam_pedazo_ini).to_json()
			json_particion_molde = json_particion.copy()
			json_particion_molde['insert_observations'] = 0
			data = urlencode({'action' : action, 'json' : json.dumps(json_particion_molde), 'compress' : 1}).encode()
			respuesta = dameRespuestaLlamado(url, data)
			#print(respuesta)
			respuesta0 = respuesta
			json_particion['id_dataset'] = respuesta['id_dataset']
			print("Dataset created succesfully, id: " + str(json_particion['id_dataset']))
			print("Starting upload..")
			stop = time.time()
			segundos = math.ceil(stop - start)
			tam_pedazo = int(round(600 / segundos))
			
			action = 'API_insertObservationsFromDataFrame'
			for i in range(0, n_filas, tam_pedazo):
				json_particion[nom_obs] = df[i:i+tam_pedazo].to_json()
				data = urlencode({'action' : action, 'json' : json.dumps(json_particion), 'compress' : 1}).encode()
				respuesta = dameRespuestaLlamado(url, data)
				# Imprimir avance
				avance = round((i + tam_pedazo) / n_filas * 100, 2)
				if avance > 100:
					print('100%')
					print("Wrapping Up..")
				else:
					print(str(avance) + "%")
					time.sleep(2)
					#print("Uploading...")
		else:
			json_particion[nom_obs] = df[0:tam_pedazo_ini].to_json()
			data = urlencode({'action' : action, 'json' : json.dumps(json_particion), 'compress' : 1}).encode()
			respuesta = dameRespuestaLlamado(url, data)
			return respuesta
	else:
		tam_pedazo_ini = tam_pedazo_ini if n_filas > tam_pedazo_ini else n_filas
		json_particion[nom_obs] = df.sample(n=tam_pedazo_ini).to_json()
		data = urlencode({'action' : action, 'json' : json.dumps(json_particion), 'compress' : 1}).encode()
		respuesta = dameRespuestaLlamado(url, data)
		return respuesta
	return respuesta0

def API_insertObservationsFromDataFrame(json_parametros, url):
	action = 'API_insertObservationsFromDataFrame'
	respuesta = ''
	test_call = 1 if 'test_call' in json_parametros and (json_parametros['test_call'] == 1 or json_parametros['test_call'] == 'on') else False
	if test_call == 1:
		print("")
		print('This is a TEST CALL, set "test_call" : "off" or remove to execute service.')
		print("")
		
	nom_obs = 'dataframe' if 'dataframe' in json_parametros else 'observations'
	df, valido_df, msj = comprobarJSONaDF(json_parametros[nom_obs])
	if not valido_df:
		return msj
	n_filas = df.shape[0]
	n_columnas = df.shape[1]
	tam_pedazo_ini = 1000
	json_particion = json_parametros.copy()

	if n_filas > tam_pedazo_ini:
		# Inserta por primera vez para medir tiempos.
		start = time.time()
		json_particion[nom_obs] = df[0:tam_pedazo_ini].to_json()
		data = urlencode({'action' : action, 'json' : json.dumps(json_particion), 'compress' : 1}).encode()
		respuesta = dameRespuestaLlamado(url, data)
		stop = time.time()
		segundos = math.ceil(stop - start)
		tam_pedazo = int(round(600 / segundos))
		print("Uploading..")
		json_particion = json_parametros.copy()
		for i in range(tam_pedazo_ini, n_filas, tam_pedazo):
			try:
				json_particion[nom_obs] = df[i:i+tam_pedazo].to_json()
				data = urlencode({'action' : action, 'json' : json.dumps(json_particion), 'compress' : 1}).encode()
				respuesta = dameRespuestaLlamado(url, data)
				#print(respuesta)
				# Imprimir avance
				avance = round((i + tam_pedazo)/n_filas * 100, 2)
				if avance > 100:
					print('100%')
					print("Wrapping Up..")
				else:
					print(str(avance) + "%")
					#print("Uploading...")
					time.sleep(2)
			except:
				print("Warning: Some observations might not have been uploaded.")
	else:
		data = urlencode({'action' : action, 'json' : json.dumps(json_parametros), 'compress' : 1}).encode()
		respuesta = dameRespuestaLlamado(url, data)
	return respuesta


def API_getSampleObservationsWithVectorizer(json_parametros, url):
	global VERSION
	confirm, consumption_id = initializeTask(json_parametros, url)
	if confirm == 'y':
		print("Task confirmed. Starting download..")
		json_parametros['consumption_id'] = consumption_id
		json_parametros['python_version'] = VERSION
		return API_genericDownloadCall(json_parametros, url, 'API_getSampleObservationsWithVectorizerPlus', 5, 300)
	else:
		print("")
		print("Task cancelled. To execute tasks without prompt set 'consumption_confirmation' to 0.")
		return {'status' : 'cancelled'}
    
    
def API_getSampleObservationsFromDataset(json_parametros, url):
	global VERSION
	confirm, consumption_id = initializeTask(json_parametros, url)
	if confirm == 'y':
		print("Task confirmed. Starting download..")
		json_parametros['consumption_id'] = consumption_id
		json_parametros['python_version'] = VERSION
		return API_genericDownloadCall(json_parametros, url, 'API_getSampleObservationsFromDataset', 25, 600)
	else:
		print("")
		print("Task cancelled. To execute tasks without prompt set 'consumption_confirmation' to 'off'.")
		return {'status' : 'cancelled'}
    

def API_getOpenTextData(json_parametros, url):
	global VERSION
	confirm, consumption_id = initializeTask(json_parametros, url)
	if confirm == 'y':
		print("Task confirmed. Starting download..")
		json_parametros['consumption_id'] = consumption_id
		json_parametros['python_version'] = VERSION
		return API_genericDownloadCall(json_parametros, url, 'API_getOpenTextData', 25, 500)
	else:
		print("")
		print("Task cancelled. To execute tasks without prompt set 'consumption_confirmation' to 0.")
		return {'status' : 'cancelled'}
    
    
def initializeTask(json_parametros, url):
	json_parametros['python_version'] = VERSION
	data = urlencode({'action' : 'API_initializeTask', 'json' : json.dumps(json_parametros), 'compress' : 1}).encode()
	details_task = dameRespuestaLlamado(url, data)
	#print(details_task)
	consumption_id = details_task['consumption_id']
	print("Task ID: '" + str(consumption_id) + "'.")
	print("Total estimated consumption: " + str(round(details_task['details']['total_consumption'],2)) + " processing units.")
	consumption_confirmation = json_parametros['consumption_confirmation'] if 'consumption_confirmation' in json_parametros else 0
	time.sleep(0.5)
	confirm = input("Continue?  [y] yes \t [n] no") if consumption_confirmation == 'on' else 'y'
	return confirm, consumption_id

def API_genericDownloadCall(json_parametros, url, action, n_test_observations, slice_mult):
	respuesta = ''
	nom_archivo = str(time.time()) + '.csv'
	try:
		start = time.time()
		test_call = 1 if 'test_call' in json_parametros and (json_parametros['test_call'] == 1 or json_parametros['test_call'] == 'on') else False
		if test_call == 1:
			print("")
			print('This is a TEST CALL, set "test_call" : "off" or remove to execute service.')
			print("")
			data = urlencode({'action' : action, 'json' : json.dumps(json_parametros), 'compress' : 1}).encode()
			respuesta = dameRespuestaLlamado(url, data)
			df_resp = pd.DataFrame.from_dict(respuesta['sample'])
			df_resp = df_resp.reset_index(drop=True)
			t_universo = 0
		else:
			json_parametros['tamano_bin'] = n_test_observations
			json_parametros['skip'] = 0
			data = urlencode({'action' : action, 'json' : json.dumps(json_parametros), 'compress' : 1}).encode()
			respuesta = dameRespuestaLlamado(url, data)
			t_universo = respuesta['universe_size']
			stop = time.time()
			segundos = math.ceil(stop - start)
			tam_pedazo = int(round(slice_mult / segundos))
			num_pedazos = math.ceil(t_universo/tam_pedazo)
			num_pedazos = num_pedazos if num_pedazos > 0 else 1
			df_resp = None
			for i in range(0, num_pedazos):
				try:
					json_parametros['tamano_bin'] = tam_pedazo
					json_parametros['skip'] = tam_pedazo * i
					data = urlencode({'action' : action, 'json' : json.dumps(json_parametros), 'compress' : 1}).encode()
					respuesta = dameRespuestaLlamado(url, data)
					df = pd.DataFrame.from_dict(respuesta['sample'])
					if df_resp is None:
						df_resp = df
						try:
							if not os.path.isfile(nom_archivo):
							   df_resp.to_csv(nom_archivo)
							else: # else it exists so append without writing the header
							   df_resp.to_csv(nom_archivo, mode='a', header=False)
							print('CSV will be stored in: ' + nom_archivo)
						except:
							print('Unable to save CSV locally, please save dataframe when download completes.')
					else:
						df_resp = df_resp.append(df).reset_index(drop=True)
						try:
							if not os.path.isfile(nom_archivo):
							   df.to_csv(nom_archivo)
							else: # else it exists so append without writing the header
							   df.to_csv(nom_archivo, mode='a', header=False)
						except:
							1 + 1
					avance = round(((i + 1)/num_pedazos) * 100, 2)
					if avance >= 100:
						print(str(avance) + " % completed.")
					else:
						print(str(avance) + " %")
						#print("downloading..")
				except Exception as e:
					#print(str(e))
					print("Warning: Some observations could not be processed.")
			if 'sample_size' in json_parametros:
				if int(json_parametros['sample_size']) < df_resp.shape[0]:
				    drop_indices = np.random.choice(df_resp.index, df_resp.shape[0] - int(json_parametros['sample_size']), replace=False)
				    df_resp = df_resp.drop(drop_indices)
		if 'lag_feature' in json_parametros : 
			df_resp = agregarLagsFeatures(df_resp, json_parametros['lag_feature'])
		respuesta = json.loads(json.dumps({'universe_size' : t_universo, 'sample' : json.loads(df_resp.to_json()), 'csv_stored' : nom_archivo}))
	except:
		print("")
		print("")
		print("Generic error.")
	return respuesta


def API_powerModel(json_parametros, url):
	action = 'API_powerModel'
	data = urlencode({'action' : action, 'json' : json.dumps(json_parametros), 'compress' : 1}).encode()
	respuesta = dameRespuestaLlamado(url, data)
	return respuesta


def comprobarJSONaDF(df_json):
	valido = True
	msj = "Sucess"
	try:
		df_nuevo = pd.read_json(df_json, convert_dates=False, convert_axes=False)
	except Exception as e:
		df_nuevo = None
		valido = False
		msj = "Error transforming json: " + str(e)
	return df_nuevo, valido, msj

def agregarLagsFeatures(df, lag_feature):
	try:
		df = df.sort_values('timestamp', ascending=False)
		df.reset_index(drop=True, inplace=True)
		features = [lag_feature['feature']]
		lag_type = lag_feature['add_poc'] if 'add_poc' in lag_feature else 0
		arr_periods = lag_feature['periods'] if 'periods' in lag_feature else [1]
		numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64', np.number]
		arr_periods.sort()
		if 'timestamp' in df:
			df_res = df.copy()
			for periods in arr_periods:
				df_lag = df[features].copy()
				df_lag = df_lag.shift(periods = -periods)
				df_lag.columns = ["lag" + str(periods) + "_" + column for column in df_lag.columns]
				if lag_type == 1:
					if df[features].select_dtypes(include = numerics).shape[1] > 0:
						for column in df[features].select_dtypes(include = numerics):
							try:
								nom_col = "lagPoc" + str(periods) + "_" + column
								col_lag = "lag" + str(periods) + "_" + column
								df_lag[nom_col] =  (df[column] - df_lag[col_lag]) / [0.1 if row == 0 else row for row in df_lag[col_lag]]
							except Exception as e:
								print("Warning : Poc was not performed for lag period: " + str(periods))
					else:
						print("Warning : Poc was not performed for lag period: " + str(periods))
				df_res = pd.concat([df_res.reset_index(drop=True), df_lag.reset_index(drop=True)], axis=1)
			df = df_res
	except:
		print("Warning : Lags were not performed.")
	return df
