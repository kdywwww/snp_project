import glob
import os
import shutil
from datetime import datetime

import pandas as pd
import pytz
from loguru import logger
from src.common import clean_old_backups, gen_timestamp_filename
from twelvedata import TDClient

# 변수 설정
timestamp_str = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y%m%d_%H%M%S")
TD_API_KEY = "45d1cfb01d4d4677a1a158bd46a97b84"
MAX_BACKUP_NUM = 2

# 디렉터리 설정
# (사전에 GCS 버킷 snp-project-bucket이 /bucket에 마운트 되어 있어야 함)
data_dir = "/bucket/data/raw_data"
backup_dir = os.path.join(data_dir, "backups")
log_dir = "/logs"
os.makedirs(log_dir, exist_ok=True)
bucket_log_dir = "/bucket/logs/DataDL"

# 대상 파일명 설정
twelve_dict = {"SPY": ["spy_data.csv"], "USD/KRW": ["usdkrw_data.csv"]}
log_filename = f"log_datadl_{timestamp_str}.log"


# 로그 설정 (로그 파일에 기록)
logger.add(os.path.join(log_dir, log_filename))
logger.info("Start DataDL.")


try:
    # TwelveData
    td = TDClient(apikey=TD_API_KEY)
    for symbol in twelve_dict.keys():
        # target
        logger.info(f"Start: [TwelveData {symbol}]")
        filename = twelve_dict[symbol][0]
        logger.info(f"[TwelveData {symbol}] filename: {filename}")
        backup_filename = gen_timestamp_filename(filename, timestamp_str)

        # Load original data
        data_df = pd.read_csv(
            os.path.join(data_dir, filename),
            parse_dates=["datetime"],
            index_col="datetime",
        )
        data_df = data_df.sort_index()
        logger.info(f"[TwelveData {symbol}] Existing Data: {data_df.shape[0]} rows.")
        last_date = data_df.index.max()
        start_date_str = last_date.strftime("%Y-%m-%d")
        logger.info(
            f"[TwelveData {symbol}] Existing Data last_date: {last_date.strftime('%Y-%m-%d')}, Download start_date: {start_date_str}"
        )

        # Retrieve TwelveData
        retrieval_df = td.time_series(
            symbol=symbol,
            interval="1day",
            start_date=start_date_str,
            outputsize=5000,
        ).as_pandas()
        retrieval_df = retrieval_df.sort_index()
        logger.info(
            f"[TwelveData {symbol}] Successfully retrieved data: {retrieval_df.shape[0]} rows."
        )

        # Concat
        data_df = pd.concat([data_df, retrieval_df], axis=0)
        data_df.reset_index(inplace=True)
        data_df = data_df.drop_duplicates(subset="datetime", keep="last")
        data_df = data_df.set_index("datetime")
        data_df = data_df.sort_index()
        logger.info(
            f"[TwelveData {symbol}] Total data after concat: {data_df.shape[0]} rows."
        )

        # Save
        data_df.to_csv(os.path.join(data_dir, filename))
        data_df.to_csv(os.path.join(backup_dir, backup_filename))
        clean_old_backups(logger, backup_dir, filename, MAX_BACKUP_NUM)
        logger.info(
            f"[TwelveData {symbol}] Save Data: {os.path.join(data_dir, filename)}"
        )
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
