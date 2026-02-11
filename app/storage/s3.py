import io
import boto3
import torchaudio
import torch
from botocore.client import Config, ClientError
import tempfile
from pathlib import Path
from typing import Any, Dict
import json

S3_BUCKET = "local-bucket"
S3_REGION = "us-east-1"
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"


s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
    config=Config(signature_version="s3v4"),
)

def generate_presigned_url(key: str, expires_in: int = 3600) -> str:
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires_in,
    )

def upload_wav_to_s3(
    wave: torch.Tensor,
    sample_rate: int,
    key: str,
):
    buffer = io.BytesIO()
    torchaudio.save(buffer, wave, sample_rate, format="wav")
    buffer.seek(0)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer,
        ContentType="audio/wav",
    )


def download_segment_from_s3(s3_client, bucket: str, key: str) -> str:
    """
    Скачивает объект S3 во временный WAV-файл и возвращает путь к нему.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
    s3_client.download_file(bucket, key, tmp_file.name)
    return tmp_file.name


def upload_mp3_to_s3(local_path: Path, s3_key: str):
    """Заливает mp3 в S3, если его там еще нет"""
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        # если объект есть — не загружаем повторно
        return
    except s3_client.exceptions.ClientError:
        # объекта нет — загружаем
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=S3_BUCKET,
            Key=s3_key,
            ExtraArgs={"ContentType": "audio/mpeg"}
        )


def upload_json_to_s3(
    data: Dict[str, Any],
    s3_key: str,
) -> None:
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def s3_object_exists(s3_key: str, bucket_name: str = "your-bucket") -> bool:
    """
    Проверяет, существует ли объект в S3.

    Args:
        s3_key: ключ объекта в S3
        bucket_name: имя bucket

    Returns:
        True, если объект существует
        False, если объекта нет
    """
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        # Любая другая ошибка — пробрасываем
        raise