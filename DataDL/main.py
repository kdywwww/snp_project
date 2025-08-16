import glob
import os
import shutil
from datetime import datetime

import pandas as pd
import pytz
from loguru import logger
from twelvedata import TDClient

# 변수 및 경로 설정
timestamp_str = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y%m%d_%H%M%S")
API_KEY = "45d1cfb01d4d4677a1a158bd46a97b84"
MAX_BACKUP_NUM = 2

data_dir = "/bucket/data/raw_data"
backup_dir = os.path.join(data_dir, "backups")
twelve_dict = {"SPY": ["spy_data.csv"], "USD/KRW": ["usdkrw_data.csv"]}
log_dir = "/logs"
bucket_log_dir = "/bucket/logs/DataDL"
log_filename = f"log_datadl_{timestamp_str}.log"


# 로그 설정 (로그 파일에 기록)
logger.add(os.path.join(log_dir, log_filename))
logger.info("Start DataDL.")


# 날짜 파일명 생성 함수
def gen_timestamp_filename(filename, timestamp_str):
    """
    원본 파일명에 타임스탬프를 추가하여 새로운 파일명을 생성합니다.

    Args:
        filename (str): 원본 파일명 (예: 'data.csv').
        timestamp_str (str): 파일명에 추가할 타임스탬프 문자열 (예: '20230815_103000').

    Returns:
        str: 타임스탬프가 추가된 새로운 파일명 (예: 'data_20230815_103000.csv').
    """
    name, ext = os.path.splitext(filename)
    return f"{name}_{timestamp_str}{ext}"


# 백업 파일 정리 함수
def clean_old_backups(file_dir, filename, max_num):
    """
    지정된 디렉터리에서 오래된 백업 파일을 정리합니다.

    Args:
        file_dir (str): 백업 파일이 있는 디렉터리 경로.
        filename (str): 백업 파일의 원본 파일명 (예: 'data.csv').
        max_num (int): 유지할 백업 파일의 최대 개수.

    Returns:
        None: 별도의 반환값은 없습니다.
    """
    name, ext = os.path.splitext(filename)
    backup_files = sorted(glob.glob(os.path.join(file_dir, f"{name}_*{ext}")))
    if len(backup_files) > max_num:
        files_to_delete = backup_files[:-max_num]
        for file in files_to_delete:
            os.remove(file)
            logger.info(f"Delete old backup: {file}")


try:
    # TwelveData
    td = TDClient(apikey=API_KEY)
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
        clean_old_backups(backup_dir, filename, MAX_BACKUP_NUM)
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
