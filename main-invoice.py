from os import getenv
from zeep import Client

url = getenv('CTF_WEBSERVICE_URL')

client = Client(wsdl=f'{url}?wsdl')

web_service = client.create_service(binding_name=getenv('CTF_WEBSERVICE_BINDING'), address=getenv('CTF_WEBSERVICE_URL'))