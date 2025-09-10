import os
import io
import gzip
import json
import time
import random
import logging
import datetime
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings

app = func.FunctionApp()

CG_PER_PAGE = int(os.getenv("CG_PER_PAGE", "250"))
CG_PAGE = int(os.getenv("CG_PAGE", "1"))
RAW_CONTAINER = os.getenv("RAW_CONTAINER", "raw")
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

def fetch_coingecko(per_page=250, page=1, max_attempts=3):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": int(per_page),
        "page": int(page),
        "sparkline": "false",
        "price_change_percentage": "1h,24h,7d"
    }
    headers = {"User-Agent": "crypto-pipeline-ingest/1.0"}

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(COINGECKO_URL, params=params, headers=headers, timeout=15)
            if resp.status_code == 429 or (500 <= resp.status_code < 600):
                raise RuntimeError(f"Transient HTTP {resp.status_code}")
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise RuntimeError("Unexpected response format from CoinGecko")
            return data
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                logging.exception("Max retries reached fetching CoinGecko")
                raise
            backoff = (2 ** attempt) + random.random()
            time.sleep(backoff)
    raise RuntimeError("Unreachable fetching logic")

def create_ndjson_gz(records, ingested_at_iso):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for rec in records:
            rec = dict(rec)
            rec["_ingested_at_utc"] = ingested_at_iso
            line = json.dumps(rec, separators=(",", ":"), ensure_ascii=False) + "\n"
            gz.write(line.encode("utf-8"))
    buf.seek(0)
    return buf

def upload_to_blob(container_name: str, blob_path: str, stream: io.BytesIO):
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        raise RuntimeError("AzureWebJobsStorage not set in environment")
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_path)
    content_settings = ContentSettings(content_type="application/x-ndjson", content_encoding="gzip")
    blob_client.upload_blob(stream, overwrite=True, content_settings=content_settings)

def write_to_deadletter(error_msg: str, context: dict):
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        logging.error("AzureWebJobsStorage not set; cannot write to deadletter")
        return

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(
        container="deadletter",
        blob=f"failed_ingest/{datetime.datetime.utcnow().strftime('%Y/%m/%d/%H')}/error_{int(time.time())}.json"
    )
    payload = {
        "error": error_msg,
        "context": context,
        "timestamp_utc": datetime.datetime.utcnow().isoformat() + "Z"
    }

    try:
        blob_client.upload_blob(
            json.dumps(payload),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json")
        )
        logging.info("Wrote failure to deadletter container")
    except Exception as e:
        logging.exception(f"Failed to write to deadletter: {e}")

@app.function_name(name="cg_ingest_timer")
@app.schedule(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def cg_ingest_timer(mytimer: func.TimerRequest) -> None:
    logging.info("cg_ingest_timer triggered")

    now = datetime.datetime.utcnow().replace(microsecond=0)
    ts_iso = now.isoformat() + "Z"
    y, m, d, H = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"), now.strftime("%H")

    try:
        data = fetch_coingecko(per_page=CG_PER_PAGE, page=CG_PAGE)
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        write_to_deadletter(str(e), {"stage": "fetch", "page": CG_PAGE})
        return

    gz_stream = create_ndjson_gz(data, ts_iso)
    blob_name = f"coins_markets/year={y}/month={m}/day={d}/hour={H}/coins_markets_p{CG_PAGE}_{now.strftime('%Y%m%dT%H%M%SZ')}.jsonl.gz"

    try:
        upload_to_blob(RAW_CONTAINER, blob_name, gz_stream)
        logging.info(f"Uploaded {len(data)} records to {RAW_CONTAINER}/{blob_name}")
    except Exception as e:
        logging.exception(f"Upload failed: {e}")
        write_to_deadletter(str(e), {"stage": "upload", "blob_name": blob_name, "count": len(data)})
