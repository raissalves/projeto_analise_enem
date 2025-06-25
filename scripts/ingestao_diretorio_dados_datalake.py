import os
from google.cloud import storage

def upload_pasta_para_bucket(
    caminho_pasta_local: str,
    bucket_name: str,
    pasta_destino_bucket: str,
    chave_json: str
):
    # Inicializa cliente do GCS
    client = storage.Client.from_service_account_json(chave_json)
    bucket = client.bucket(bucket_name)

    # Percorre todos os arquivos na pasta local
    for nome_arquivo in os.listdir(caminho_pasta_local):
        caminho_completo_arquivo = os.path.join(caminho_pasta_local, nome_arquivo)
        
        # Apenas arquivos (ignora subpastas)
        if os.path.isfile(caminho_completo_arquivo):
            # Define o nome completo do blob no bucket, dentro da pasta destino
            blob_nome = os.path.join(pasta_destino_bucket, nome_arquivo).replace("\\", "/")
            
            blob = bucket.blob(blob_nome)
            blob.upload_from_filename(caminho_completo_arquivo)
            
            print(f"Arquivo {nome_arquivo} enviado para gs://{bucket_name}/{blob_nome}")

if __name__ == "__main__":
    # pasta local com os arquivos para upload
    pasta_local = '/home/raissa/Documentos/microdados_enem_2023/scripts/parquet_chunks'
    #Nome do seu bucket
    bucket = 'dados_enem-bucket'
    # pasta dentro do bucket para onde os arquivos vão
    pasta_destino = 'bronze/parquet'  
    # Sua chave de autenticacao do servico
    caminho_chave = '/home/raissa/Downloads/teak-amphora-460722-b2-4e7c2fe25adb.json'

    upload_pasta_para_bucket(pasta_local, bucket, pasta_destino, caminho_chave)