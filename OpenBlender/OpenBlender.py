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

def dameRespuestaLlamado(url, data):
	with closing(urlopen(url, data)) as response:
		respuesta = json.loads(response.read().decode())
		if 'base64_zip' in respuesta:
			respuesta = json.loads(zlib.decompress(b64decode(respuesta['base64_zip'])))
	try:
		if 'error' in respuesta['status']:
			print("------------------------------------------------")
			print("API call error: " + str(respuesta['response']))
			print("------------------------------------------------")
			print("")
			return False
	except:
		print(respuesta)
	return respuesta


def call(action, json_parametros):
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
			respuesta = API_getDataWithVectorizer(json_parametros, url)
		else:
			data = urlencode({'action' : action, 'json' : json.dumps(json_parametros), 'compress' : 1}).encode()
			respuesta = dameRespuestaLlamado(url, data)
		return respuesta
	except Exception as e:
		if 'oblender' in json_parametros and json_parametros['oblender'] == 1:
			print(json.dumps({"status": "internal error", "msg": traceback.format_exc()}))
		else:
			print(json.dumps({"status": "internal error", "msg": str(e)}))
		return json.dumps({"status": "internal error", "msg": str(e)})
    
def API_createDataset(json_parametros, url):
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
		with closing(urlopen(url, data)) as response:
			return json.loads(response.read().decode())
	return respuesta

def API_getDataWithVectorizer(json_parametros, url):
	return API_genericDownloadCall(json_parametros, url, 'API_getDataWithVectorizer', 5, 300)

def API_getSampleObservationsFromDataset(json_parametros, url):
	return API_genericDownloadCall(json_parametros, url, 'API_getSampleObservationsFromDataset', 25, 600)

def API_genericDownloadCall(json_parametros, url, action, n_test_observations, slice_mult):

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
				else:
					df_resp = df_resp.append(df).reset_index(drop=True)
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
	#print(df_resp)
	respuesta = json.loads(json.dumps({'universe_size' : t_universo, 'sample' : json.loads(df_resp.to_json())}))
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
