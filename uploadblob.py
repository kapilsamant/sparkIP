import mysql.connector
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings



def upload_to_blob_storage(connection_string, container_name, blob_name, data):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data, blob_type="BlockBlob", content_settings=ContentSettings(content_type='application/octet-stream'))


mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'root'
mysql_database = 'jmm'

azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=<name>;AccountKey=<key>;EndpointSuffix=core.windows.net"
azure_container_name = '<container name>'
query = "select FileContent, filename from JMM.MediaContent"



conn = mysql.connector.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_database)
cursor = conn.cursor()
cursor.execute(query)
rows = cursor.fetchall()
for row in rows:
    print(row)
    data = row[0]
    filename = row[1]
    upload_to_blob_storage(azure_connection_string, azure_container_name, filename, data)

conn.close()

