import os
import shutil
from datetime import datetime

import pandas as pd
import pandas_ta as ta
import pytz
from loguru import logger
from src.common import clean_old_backups, gen_timestamp_filename
from src.preprocessor import MS_to_D, create_lag_feature

# 변수 설정
timestamp_str = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y%m%d_%H%M%S")
MAX_BACKUP_NUM = 2

# 디렉터리 설정
# (사전에 GCS 버킷 snp-project-bucket이 /bucket에 마운트 되어 있어야 함)
raw_data_dir = "/bucket/data/raw_data"
data_dir = "/bucket/data/processed_data"
backup_dir = os.path.join(data_dir, "backups")
log_dir = "/logs"
os.makedirs(log_dir, exist_ok=True)
bucket_log_dir = "/bucket/logs/Preprocess"

# 대상 파일명 설정
raw_filename_dict = {"SPY": ["spy_data.csv"], "CLI": ["cli_data.csv"]}
total_filename = "prep_D_data.csv"
log_filename = f"log_prep_D_{timestamp_str}.log"

# 로그 설정 (로그 파일에 기록)
logger.add(os.path.join(log_dir, log_filename))
logger.info("Start Preprocess.")

try:
    # SPY
    logger.info("Start: [SPY]")
    # 데이터 읽기
    raw_filedir = os.path.join(raw_data_dir, raw_filename_dict["SPY"][0])
    logger.info(f"[SPY] raw_filedir: {raw_filedir}")
    spy_df = pd.read_csv(raw_filedir)
    logger.info(f"[SPY] Successfully read raw data: shape {spy_df.shape}")
    # 데이터 전처리
    spy_df = spy_df[["datetime", "close", "volume"]]
    spy_df.rename(columns={"close": "spy_close", "volume": "spy_volume"}, inplace=True)
    spy_df["ds"] = pd.to_datetime(spy_df["datetime"], format="%Y-%m-%d")
    spy_df.set_index("ds", inplace=True)
    spy_df.drop(columns=["datetime"], inplace=True)
    spy_df = spy_df.sort_index()
    # 기술적 지표 추가
    spy_df.ta.sma(close="spy_close", length=60, append=True)
    spy_df.ta.sma(close="spy_close", length=120, append=True)
    spy_df.ta.bbands(close="spy_close", length=20, std=2, append=True)
    spy_df.columns = spy_df.columns.str.replace("_2.0_2.0", "", regex=False)
    spy_df.ta.macd(close="spy_close", fast=12, slow=26, signal=9, append=True)
    spy_df.ta.rsi(close="spy_close", length=14, append=True)
    # Volume Lag Feature 생성
    spy_df = create_lag_feature(
        df=spy_df,
        target="spy_volume",
        n_lags=1,
        freq="D",
        extend_rows=False,
        drop_target=True,
    )
    logger.info(f"[SPY] Preprocessing completed: shape {spy_df.shape}")
    logger.info("End: [SPY]")

    # CLI
    logger.info("Start: [CLI]")
    # 데이터 읽기
    raw_filedir = os.path.join(raw_data_dir, raw_filename_dict["CLI"][0])
    logger.info(f"[CLI] raw_filedir: {raw_filedir}")
    cli_df = pd.read_csv(raw_filedir)
    logger.info(f"[CLI] Successfully read raw data: shape {cli_df.shape}")
    # 데이터 전처리
    cli_df = cli_df[["datetime", "CLI"]]
    cli_df["ds"] = pd.to_datetime(cli_df["datetime"], format="%Y-%m-%d")
    cli_df.set_index("ds", inplace=True)
    cli_df.drop(columns=["datetime"], inplace=True)
    cli_df = cli_df.sort_index()
    # Lag Feature 생성
    cli_df = create_lag_feature(
        df=cli_df, target="CLI", n_lags=1, freq="MS", extend_rows=True, drop_target=True
    )
    # 월별 데이터를 일별로 확장 및 ffill
    cli_df = MS_to_D(cli_df, ffill=True)
    logger.info(f"[CLI] Preprocessing completed: shape {cli_df.shape}")
    logger.info("End: [CLI]")

    # 최종 데이터
    logger.info("Start: [Total]")
    # 데이터 병합
    total_df = pd.merge(spy_df, cli_df, left_index=True, right_index=True, how="left")
    logger.info(f"[Total] Merged DataFrame : shape {total_df.shape}")
    # NaN 처리
    total_df = total_df.ffill()
    total_df = total_df.dropna(how="any")
    total_df = total_df.round(6)
    logger.info(f"[Total] Preprocessing completed: shape {total_df.shape}")
    # 저장
    total_filedir = os.path.join(data_dir, total_filename)
    logger.info(f"[Total] total_filedir: {total_filedir}")
    backup_total_filename = gen_timestamp_filename(total_filename, timestamp_str)
    total_df.to_csv(total_filedir, index=True)
    total_df.to_csv(os.path.join(backup_dir, backup_total_filename), index=True)
    clean_old_backups(logger, backup_dir, total_filename, MAX_BACKUP_NUM)
    logger.info(f"[Total] Save Data: {total_filedir}")
    logger.info("End: [Total]")

    logger.info("End Preprocess.")

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
