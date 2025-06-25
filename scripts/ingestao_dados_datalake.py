from google.cloud import storage

client = storage.Client.from_service_account_json('/home/raissa/Downloads/teak-amphora-460722-b2-4e7c2fe25adb.json')

bucket_name = 'dados_enem-bucket'
file_path = '/home/raissa/Documentos/microdados_enem_2023/dados/MICRODADOS_ENEM_2023__amostra.csv'
destination_blob_name = 'bronze/microdados_enem.csv' 

bucket = client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(file_path)

print(f"Arquivo enviado para gs://{bucket_name}/{destination_blob_name}")