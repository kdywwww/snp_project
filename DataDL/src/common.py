import glob
import os


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
def clean_old_backups(logger, file_dir, filename, max_num):
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
