import pandas as pd


def create_lag_feature(df, target, n_lags, freq, extend_rows=False, drop_target=False):
    """
    DataFrame의 특정 열(들)에 대해 지정된 기간만큼 Lag 변수를 생성합니다.

    Args:
        df (pd.DataFrame): 시계열 인덱스(DatetimeIndex)를 가진 원본 데이터프레임.
        target (str): Lag 변수를 생성할 열 이름.
        n_lags (int): 지연시킬 기간 (정수 시점 또는 시간 문자열).
            - int: 시계열 인덱스의 '행' 개수만큼 지연 (예: 1일, 7주).
        freq (str): df의 인덱스(datetime)의 빈도 문자열 (예: 'D', 'M', 'MS').
        extend_rows (bool, optional): True이면 데이터프레임의 마지막 날짜 이후로 n_lags 기간만큼의
            새로운 인덱스를 추가합니다. 기본값은 False입니다.
        drop_target (bool, optional): True이면 원본 target 열을 삭제합니다. 기본값은 False입니다.

    Returns:
        new_df (pd.DataFrame): Lag 변수가 추가된 새로운 데이터프레임.
    """
    new_df = df.copy()
    new_df = new_df.sort_index()
    index_name = new_df.index.name if new_df.index.name else "ds"

    if extend_rows:
        # 마지막 날짜 이후로 n_lags 기간만큼의 새로운 인덱스 생성
        start = new_df.index[-1] + pd.tseries.frequencies.to_offset(freq)
        future_dates = pd.date_range(start=start, periods=n_lags, freq=freq)
        new_df = pd.concat([new_df, pd.DataFrame(index=future_dates)], axis=0)
        new_df.index.name = index_name

    # Lag 변수명 생성 (예: 'Close_lag_1' 또는 'VIX_lag_2M')
    lag_name = f"{target}_lag_{n_lags}{freq}"
    # shift() 함수를 사용하여 Lag 변수 생성
    new_df[lag_name] = new_df[target].shift(periods=n_lags, freq=None)

    if drop_target:
        # 원본 target 열 삭제
        new_df.drop(columns=[target], inplace=True)

    return new_df


def MS_to_D(df, ffill=False):
    """
    월초(freq='MS') DatetimeIndex를 가진 DataFrame을
    일별(Daily) 빈도로 확장하고, 필요 시 NaN 값을 ffill로 채웁니다.

    Args:
        df (pd.DataFrame): 월별 DatetimeIndex를 가진 DataFrame.
        ffill (bool, optional): True이면 NaN 값을 forward fill로 채웁니다.
            기본값은 False입니다.

    Returns:
        new_df (pd.DataFrame): 일별 빈도로 확장되고 ffill 처리된 DataFrame.
    """

    new_df = df.copy()
    new_df = new_df.sort_index()

    # 1. 일별 DatetimeIndex 생성
    daily_index = pd.date_range(
        start=new_df.index.min(),
        end=new_df.index.max() + pd.tseries.frequencies.to_offset("MS"),
        freq="D",  # 일별 빈도(Daily Frequency) 지정
    )

    # 2. Reindex (일별 인덱스로 확장)
    index_name = new_df.index.name if new_df.index.name else "ds"
    new_df = new_df.reindex(daily_index)
    new_df.index.name = index_name

    # 3. ffill (Forward Fill)
    if ffill:
        new_df = new_df.ffill()

    return new_df
