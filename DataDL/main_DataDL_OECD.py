import os
import shutil
from datetime import datetime
from io import StringIO

import pandas as pd
import pytz
import requests
from loguru import logger
from src.common import clean_old_backups, gen_timestamp_filename

# 변수 설정
timestamp_str = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y%m%d_%H%M%S")
# OECD API URL
CLI_URL = f"https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,/USA.M.LI...NOR+AA...H?startPeriod=2010-01&dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
MAX_BACKUP_NUM = 2

# 디렉터리 설정
# (사전에 GCS 버킷 snp-project-bucket이 /bucket에 마운트 되어 있어야 함)
data_dir = "/bucket/data/raw_data"
backup_dir = os.path.join(data_dir, "backups")
log_dir = "/logs"
os.makedirs(log_dir, exist_ok=True)
bucket_log_dir = "/bucket/logs/DataDL"

# 대상 파일명 설정
oecd_all_dict = {
    "CLI": {
        "filename": "cli_data.csv",
        "url": CLI_URL,
        "MEASURE": "LI",
    }
}
log_filename = f"log_datadl_oecd_{timestamp_str}.log"

# 로그 설정 (로그 파일에 기록)
logger.add(os.path.join(log_dir, log_filename))
logger.info("Start DataDL.")

try:
    for symbol in oecd_all_dict.keys():
        # target
        logger.info(f"Start: [OECD {symbol}]")
        filename = oecd_all_dict[symbol]["filename"]
        logger.info(f"[OECD {symbol}] filename: {filename}")
        backup_filename = gen_timestamp_filename(filename, timestamp_str)
        url = oecd_all_dict[symbol]["url"]
        logger.info(f"[OECD {symbol}] url: {url}")
        measure = oecd_all_dict[symbol]["MEASURE"]
        logger.info(f"[OECD {symbol}] measure: {measure}")

        # Retrieve OECD
        response = requests.get(url)
        response.raise_for_status()  # HTTP 오류가 발생하면 예외 발생
        retrieval_df = pd.read_csv(StringIO(response.text))
        data_df = retrieval_df.loc[
            retrieval_df["MEASURE"] == measure, ["TIME_PERIOD", "OBS_VALUE"]
        ].copy()
        data_df.columns = ["datetime", symbol]
        data_df["datetime"] = pd.to_datetime(data_df["datetime"], format="%Y-%m")
        data_df = data_df.drop_duplicates(subset="datetime", keep="last")
        data_df = data_df.set_index("datetime")
        data_df = data_df.sort_index()
        logger.info(
            f"[OECD {symbol}] Successfully retrieved data: {data_df.shape[0]} rows."
        )

        # Save
        data_df.to_csv(os.path.join(data_dir, filename))
        data_df.to_csv(os.path.join(backup_dir, backup_filename))
        clean_old_backups(logger, backup_dir, filename, MAX_BACKUP_NUM)
        logger.info(f"[OECD {symbol}] Save Data: {os.path.join(data_dir, filename)}")
    logger.info("End DataDL.")

except Exception as e:
    logger.error(f"Unexpected Error: {e}")
    raise e

finally:
    logger.info(f"Copy log to GCS: {os.path.join(bucket_log_dir, log_filename)}")
    try:
        # Copy log to GCS mount path
        shutil.copy(
            os.path.join(log_dir, log_filename),
            os.path.join(bucket_log_dir, log_filename),
        )
        logger.info("Success to copy log to GCS.")
    except Exception as e:
        logger.error(f"Failed to copy log to GCS: {e}")
