import os

import boto3
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

def create_lakehouse_bucket():
    # Inicializa o client S3 usando boto3 com as chaves do Neon
    s3 = boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION"),
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    bucket = os.environ.get("S3_BUCKET_NAME", "lakehouse")
    
    print(f"Tentando acessar/criar o bucket: '{bucket}'...")
    
    try:
        # Cria o bucket
        s3.create_bucket(Bucket=bucket)
        print(f"Sucesso! Bucket '{bucket}' criado na plataforma Neon.")
    except Exception as e:  # noqa: BLE001
        if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
            print(f"O bucket '{bucket}' já existe e está pronto para uso.")
        else:
            print(f"Erro ao criar o bucket: {e}")

if __name__ == "__main__":
    create_lakehouse_bucket()
