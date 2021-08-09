import urllib3
import requests
import xml.etree.ElementTree as ET
import os.path
import constant
import time

urllib3.disable_warnings()


def call_cert():
    home = os.getcwd()
    cert_file = "cert.pem"
    cert_path = os.path.join(home, cert_file)
    key_file = "key2.pem"
    key_path = os.path.join(home, key_file)
    full_cert = (cert_path, key_path)
    return full_cert


def recall_response_from_queue(queue_id):
    body = {'id': queue_id, 'method': 'get'}
    try:
        response = requests.post(constant.MOVEON_URL, data=body, cert=call_cert(), verify=False)
        XML_response = ET.fromstring(response.text)
        status = ''
        for result in XML_response.iter('status'):
            status = result.text
        while status == "processing" or status == "queued":
            print("processing")
            time_duration = 2
            time.sleep(time_duration)
            response = requests.post(constant.MOVEON_URL, data=body, cert=call_cert(), verify=True)
            XML_response = ET.fromstring(response.text)
            for result in XML_response.iter('status'):
                status = result.text
        if status != "success":
            raise ValueError("Failed from request: " + response.text)
        return response.text
    except ValueError as errordatos:
        print("error :" + errordatos)


def queue_list_request(entity, data):
    body = {
        'method': 'queue',
        'entity': entity,
        'action': 'list',
        'data': data
    }
    # HTTP Request
    try:
        response = requests.post(constant.MOVEON_URL, data=body, cert=call_cert(), verify=True)
        XML_response = ET.fromstring(response.text)
        status = ''
        for result in XML_response.iter('status'):
            status = result.text

        while status == "processing" or status == "queued":
            print("processing")
            time_duration = 2
            time.sleep(time_duration)
            response = requests.post(constant.MOVEON_URL, data=body, cert=call_cert(), verify=True)
            XML_response = ET.fromstring(response.text)
            for result in XML_response.iter('status'):
                status = result.text
        if status != "success":
            raise ValueError("Failed from request: " + response.text)
            # Partición de la respuesta para poder sacar el queueId específicamente
        divided_response = XML_response[0][0].text.partition(':')
        # QueueId
        queue_id = divided_response[2][:-1]
        return queue_id
    except ValueError as errordatos:
        print("error :" + errordatos)


def list_request(entity, data):
    queue_id = queue_list_request(entity, data)
    response = recall_response_from_queue(queue_id)
    return response

def queue_create_request(entity, data):
    body = {
        'method': 'queue',
        'entity': entity,
        'action': 'save',
        'data': data
    }
    # HTTP Request
    print("body:")
    print(body)
    try:
        response = requests.post(constant.MOVEON_URL, data=body, cert=call_cert(), verify=True)
        XML_response = ET.fromstring(response.text)
        status = ''
        for result in XML_response.iter('status'):
            status = result.text
        while status == "processing" or status == "queued":
            print("processing")
            time_duration = 2
            time.sleep(time_duration)
            response = requests.post(constant.MOVEON_URL, data=body, cert=call_cert(), verify=True)
            XML_response = ET.fromstring(response.text)
            for result in XML_response.iter('status'):
                status = result.text
        if status != "success":
            raise ValueError("Failed from request: " + response.text)
            # Partición de la respuesta para poder sacar el queueId específicamente
        divided_response = XML_response[0][0].text.partition(':')
        # QueueId
        queue_id = divided_response[2][:-1]
        return queue_id
    except ValueError as errordatos:
        print("error :" + errordatos)


def create_request(entity, data):
    queue_id = queue_create_request(entity, data)
    response = recall_response_from_queue(queue_id)
    return response


# TODO se debe verificar que data venga con el campo entity.id para que sea PUT y no POST

def queue_update_request(entity, data):
    body = {
        'method': 'queue',
        'entity': entity,
        'action': 'save',
        'data': data}
    # HTTP Request
    print("body:")
    print(body)
    try:
        response = requests.post(constant.MOVEON_URL, data=body, cert=call_cert(), verify=True)
        XML_response = ET.fromstring(response.text)
        status = ''
        for result in XML_response.iter('status'):
            status = result.text
        while status == "processing":
            print("processing")
            time_duration = 2
            time.sleep(time_duration)
            response = requests.post(constant.MOVEON_URL, data=body, cert=call_cert(), verify=True)
            XML_response = ET.fromstring(response.text)
            for result in XML_response.iter('status'):
                status = result.text
        if status != "success":
            raise ValueError("Failed from request: " + response.text)
            # Partición de la respuesta para poder sacar el queueId específicamente
        divided_response = XML_response[0][0].text.partition(':')
        # QueueId
        queue_id = divided_response[2][:-1]
        return queue_id
    except ValueError as errordatos:
        print("error :" + errordatos)


def update_request(entity, data):
    queue_id = queue_update_request(entity, data)
    response = recall_response_from_queue(queue_id)
    return response
