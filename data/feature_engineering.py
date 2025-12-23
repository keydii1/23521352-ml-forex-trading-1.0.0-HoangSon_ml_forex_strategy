"""
Forex Feature Engineering Pipeline
===================================
Tạo đầy đủ các features cho bài toán Machine Learning Forex Trading

Features bao gồm:
1. Technical Indicators (RSI, MACD, Bollinger Bands, Moving Averages, etc.)
2. Price-based Features (Returns, Volatility, Range, etc.)
3. Lagged Features (Previous N periods)
4. Time-based Features (Day of week, Month, etc.)
5. Target Variables (Direction, Next Return)

Tác giả: Nhóm nghiên cứu Forex ML
Ngày tạo: 2025-12-23
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ============================================
# CẤU HÌNH
# ============================================

# Thư mục
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIRS = {
    'daily': os.path.join(BASE_DIR, 'daily'),
    '4h': os.path.join(BASE_DIR, '4h'),
    '1h': os.path.join(BASE_DIR, '1h')
}
OUTPUT_DIR = os.path.join(BASE_DIR, 'processed')

# Các cặp tiền
CURRENCY_PAIRS = ['EUR_USD', 'GBP_USD', 'USD_JPY', 'AUD_USD', 'USD_CHF']

# Tham số cho indicators
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
BB_STD = 2
ATR_PERIOD = 14
STOCH_PERIOD = 14
LAG_PERIODS = [1, 2, 3, 5, 7, 14, 21]  # Số ngày lag
MA_PERIODS = [7, 14, 21, 50, 100, 200]  # Moving average periods

# ============================================
# TECHNICAL INDICATORS
# ============================================

def calculate_rsi(close, period=14):
    """Tính RSI (Relative Strength Index)"""
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_macd(close, fast=12, slow=26, signal=9):
    """Tính MACD (Moving Average Convergence Divergence)"""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    macd_histogram = macd_line - signal_line
    return macd_line, signal_line, macd_histogram


def calculate_bollinger_bands(close, period=20, std_dev=2):
    """Tính Bollinger Bands"""
    sma = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    bb_width = (upper_band - lower_band) / sma
    bb_position = (close - lower_band) / (upper_band - lower_band)
    return upper_band, sma, lower_band, bb_width, bb_position


def calculate_atr(high, low, close, period=14):
    """Tính ATR (Average True Range) - Volatility Indicator"""
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr


def calculate_stochastic(high, low, close, period=14):
    """Tính Stochastic Oscillator"""
    lowest_low = low.rolling(window=period).min()
    highest_high = high.rolling(window=period).max()
    stoch_k = 100 * (close - lowest_low) / (highest_high - lowest_low)
    stoch_d = stoch_k.rolling(window=3).mean()
    return stoch_k, stoch_d


def calculate_cci(high, low, close, period=20):
    """Tính CCI (Commodity Channel Index)"""
    tp = (high + low + close) / 3
    sma_tp = tp.rolling(window=period).mean()
    mad = tp.rolling(window=period).apply(lambda x: np.mean(np.abs(x - x.mean())))
    cci = (tp - sma_tp) / (0.015 * mad)
    return cci


def calculate_williams_r(high, low, close, period=14):
    """Tính Williams %R"""
    highest_high = high.rolling(window=period).max()
    lowest_low = low.rolling(window=period).min()
    wr = -100 * (highest_high - close) / (highest_high - lowest_low)
    return wr


def calculate_momentum(close, period=10):
    """Tính Momentum"""
    return close - close.shift(period)


def calculate_roc(close, period=10):
    """Tính Rate of Change (ROC)"""
    return ((close - close.shift(period)) / close.shift(period)) * 100


# ============================================
# FEATURE ENGINEERING FUNCTIONS
# ============================================

def add_technical_indicators(df):
    """Thêm tất cả Technical Indicators"""
    print("  📈 Đang tính Technical Indicators...")
    
    # RSI
    df['RSI'] = calculate_rsi(df['Close'], RSI_PERIOD)
    df['RSI_overbought'] = (df['RSI'] > 70).astype(int)
    df['RSI_oversold'] = (df['RSI'] < 30).astype(int)
    
    # MACD
    df['MACD'], df['MACD_signal'], df['MACD_histogram'] = calculate_macd(
        df['Close'], MACD_FAST, MACD_SLOW, MACD_SIGNAL
    )
    df['MACD_crossover'] = np.where(df['MACD'] > df['MACD_signal'], 1, -1)
    
    # Bollinger Bands
    df['BB_upper'], df['BB_middle'], df['BB_lower'], df['BB_width'], df['BB_position'] = \
        calculate_bollinger_bands(df['Close'], BB_PERIOD, BB_STD)
    
    # ATR (Volatility)
    df['ATR'] = calculate_atr(df['High'], df['Low'], df['Close'], ATR_PERIOD)
    df['ATR_percent'] = df['ATR'] / df['Close'] * 100
    
    # Stochastic
    df['Stoch_K'], df['Stoch_D'] = calculate_stochastic(df['High'], df['Low'], df['Close'], STOCH_PERIOD)
    
    # CCI
    df['CCI'] = calculate_cci(df['High'], df['Low'], df['Close'])
    
    # Williams %R
    df['Williams_R'] = calculate_williams_r(df['High'], df['Low'], df['Close'])
    
    # Momentum & ROC
    df['Momentum_10'] = calculate_momentum(df['Close'], 10)
    df['ROC_10'] = calculate_roc(df['Close'], 10)
    
    # Moving Averages
    for period in MA_PERIODS:
        if len(df) > period:
            df[f'SMA_{period}'] = df['Close'].rolling(window=period).mean()
            df[f'EMA_{period}'] = df['Close'].ewm(span=period, adjust=False).mean()
            df[f'Close_vs_SMA_{period}'] = (df['Close'] / df[f'SMA_{period}'] - 1) * 100
    
    # MA Crossovers
    if 'SMA_7' in df.columns and 'SMA_21' in df.columns:
        df['MA_7_21_crossover'] = np.where(df['SMA_7'] > df['SMA_21'], 1, -1)
    if 'SMA_50' in df.columns and 'SMA_200' in df.columns:
        df['MA_50_200_crossover'] = np.where(df['SMA_50'] > df['SMA_200'], 1, -1)
    
    return df


def add_price_features(df):
    """Thêm Price-based Features"""
    print("  💰 Đang tính Price Features...")
    
    # Returns
    df['Return'] = df['Close'].pct_change()
    df['Log_Return'] = np.log(df['Close'] / df['Close'].shift(1))
    
    # Price Changes
    df['Price_Change'] = df['Close'] - df['Open']
    df['Price_Change_Percent'] = (df['Close'] - df['Open']) / df['Open'] * 100
    
    # Candle Features
    df['Range'] = df['High'] - df['Low']
    df['Body'] = abs(df['Close'] - df['Open'])
    df['Body_Percent'] = df['Body'] / df['Range']
    df['Upper_Shadow'] = df['High'] - df[['Open', 'Close']].max(axis=1)
    df['Lower_Shadow'] = df[['Open', 'Close']].min(axis=1) - df['Low']
    
    # Candle Colors
    df['Candle_Color'] = np.where(df['Close'] > df['Open'], 1, 0)  # 1=Green, 0=Red
    
    # Gap
    df['Gap'] = df['Open'] - df['Close'].shift(1)
    df['Gap_Percent'] = df['Gap'] / df['Close'].shift(1) * 100
    
    # Volatility (Rolling)
    for period in [5, 10, 20]:
        df[f'Volatility_{period}'] = df['Return'].rolling(window=period).std()
    
    # High-Low Ratio
    df['HL_Ratio'] = df['High'] / df['Low']
    
    # Close Position in Range
    df['Close_Position'] = (df['Close'] - df['Low']) / (df['High'] - df['Low'])
    
    return df


def add_lagged_features(df, lag_periods=None):
    """Thêm Lagged Features"""
    if lag_periods is None:
        lag_periods = LAG_PERIODS
    
    print(f"  ⏰ Đang tạo Lagged Features (periods: {lag_periods})...")
    
    # Lag cho Close
    for lag in lag_periods:
        if len(df) > lag:
            df[f'Close_lag_{lag}'] = df['Close'].shift(lag)
            df[f'Return_lag_{lag}'] = df['Return'].shift(lag)
    
    # Lag cho RSI
    if 'RSI' in df.columns:
        for lag in [1, 3, 5]:
            df[f'RSI_lag_{lag}'] = df['RSI'].shift(lag)
    
    # Cumulative Returns
    for period in [3, 5, 10, 20]:
        if len(df) > period:
            df[f'Cumulative_Return_{period}'] = df['Close'].pct_change(periods=period)
    
    # Rolling Stats
    for period in [5, 10, 20]:
        if len(df) > period:
            df[f'Rolling_Mean_{period}'] = df['Close'].rolling(window=period).mean()
            df[f'Rolling_Std_{period}'] = df['Close'].rolling(window=period).std()
            df[f'Rolling_Min_{period}'] = df['Close'].rolling(window=period).min()
            df[f'Rolling_Max_{period}'] = df['Close'].rolling(window=period).max()
    
    return df


def add_time_features(df):
    """Thêm Time-based Features"""
    print("  📅 Đang tạo Time Features...")
    
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Basic time features
    df['Day_of_Week'] = df['Date'].dt.dayofweek  # 0=Monday, 6=Sunday
    df['Day_of_Month'] = df['Date'].dt.day
    df['Month'] = df['Date'].dt.month
    df['Quarter'] = df['Date'].dt.quarter
    df['Year'] = df['Date'].dt.year
    df['Week_of_Year'] = df['Date'].dt.isocalendar().week.astype(int)
    
    # Binary features
    df['Is_Monday'] = (df['Day_of_Week'] == 0).astype(int)
    df['Is_Friday'] = (df['Day_of_Week'] == 4).astype(int)
    df['Is_Month_Start'] = df['Date'].dt.is_month_start.astype(int)
    df['Is_Month_End'] = df['Date'].dt.is_month_end.astype(int)
    df['Is_Quarter_Start'] = df['Date'].dt.is_quarter_start.astype(int)
    df['Is_Quarter_End'] = df['Date'].dt.is_quarter_end.astype(int)
    
    # Cyclical encoding for periodic features
    df['Day_sin'] = np.sin(2 * np.pi * df['Day_of_Week'] / 7)
    df['Day_cos'] = np.cos(2 * np.pi * df['Day_of_Week'] / 7)
    df['Month_sin'] = np.sin(2 * np.pi * df['Month'] / 12)
    df['Month_cos'] = np.cos(2 * np.pi * df['Month'] / 12)
    
    return df


def add_target_variables(df, forecast_horizon=1):
    """Tạo Target Variables cho ML"""
    print(f"  🎯 Đang tạo Target Variables (horizon={forecast_horizon})...")
    
    # Classification Target: Direction (1=Up, 0=Down)
    df['Target_Direction'] = np.where(
        df['Close'].shift(-forecast_horizon) > df['Close'], 1, 0
    )
    
    # Regression Target: Next Return
    df['Target_Return'] = df['Close'].pct_change(periods=-forecast_horizon).shift(forecast_horizon)
    df['Target_Return'] = df['Target_Return'].shift(-forecast_horizon)
    
    # Regression Target: Next Close
    df['Target_Close'] = df['Close'].shift(-forecast_horizon)
    
    # Multi-class Target: Strong Up, Up, Neutral, Down, Strong Down
    threshold = 0.001  # 0.1%
    conditions = [
        df['Target_Return'] > threshold * 2,
        df['Target_Return'] > threshold,
        df['Target_Return'] > -threshold,
        df['Target_Return'] > -threshold * 2,
    ]
    choices = [2, 1, 0, -1]  # 2=Strong Up, 1=Up, 0=Neutral, -1=Down
    df['Target_Direction_Multi'] = np.select(conditions, choices, default=-2)
    
    return df


# ============================================
# MAIN PROCESSING FUNCTION
# ============================================

def process_single_file(input_path, output_path, pair_name, timeframe):
    """Xử lý một file dữ liệu"""
    print(f"\n{'='*60}")
    print(f"📊 Đang xử lý {pair_name} ({timeframe})")
    print(f"{'='*60}")
    
    # Đọc dữ liệu
    df = pd.read_csv(input_path)
    original_rows = len(df)
    original_cols = len(df.columns)
    print(f"  📥 Đọc xong: {original_rows} rows, {original_cols} columns")
    
    # Feature Engineering
    df = add_price_features(df)
    df = add_technical_indicators(df)
    df = add_lagged_features(df)
    df = add_time_features(df)
    df = add_target_variables(df)
    
    # Loại bỏ các hàng có NaN ở đầu (do rolling calculations)
    df_clean = df.dropna()
    
    # Lưu file
    df_clean.to_csv(output_path, index=False)
    
    final_rows = len(df_clean)
    final_cols = len(df_clean.columns)
    
    print(f"  ✅ Hoàn thành!")
    print(f"  📊 Kết quả: {final_rows} rows, {final_cols} columns")
    print(f"  💾 Đã lưu: {output_path}")
    
    return df_clean


def process_all_data():
    """Xử lý tất cả dữ liệu"""
    print("=" * 70)
    print("🚀 BẮT ĐẦU FEATURE ENGINEERING CHO DỮ LIỆU FOREX")
    print("=" * 70)
    
    # Tạo thư mục output
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"✓ Đã tạo thư mục: {OUTPUT_DIR}")
    
    # Tạo sub-directories
    for timeframe in INPUT_DIRS.keys():
        tf_dir = os.path.join(OUTPUT_DIR, timeframe)
        if not os.path.exists(tf_dir):
            os.makedirs(tf_dir)
    
    results = []
    
    # Xử lý từng timeframe và pair
    for timeframe, input_dir in INPUT_DIRS.items():
        if not os.path.exists(input_dir):
            print(f"\n⚠️  Thư mục {input_dir} không tồn tại, bỏ qua...")
            continue
        
        for pair in CURRENCY_PAIRS:
            input_file = os.path.join(input_dir, f'{pair}_{timeframe}.csv')
            output_file = os.path.join(OUTPUT_DIR, timeframe, f'{pair}_{timeframe}_processed.csv')
            
            if os.path.exists(input_file):
                try:
                    df = process_single_file(input_file, output_file, pair, timeframe)
                    results.append({
                        'Pair': pair,
                        'Timeframe': timeframe,
                        'Rows': len(df),
                        'Columns': len(df.columns),
                        'Status': '✅'
                    })
                except Exception as e:
                    print(f"  ❌ Lỗi xử lý {pair} ({timeframe}): {str(e)}")
                    results.append({
                        'Pair': pair,
                        'Timeframe': timeframe,
                        'Rows': 0,
                        'Columns': 0,
                        'Status': '❌'
                    })
            else:
                print(f"\n⚠️  File không tồn tại: {input_file}")
    
    # Tổng kết
    print("\n" + "=" * 70)
    print("📊 TỔNG KẾT FEATURE ENGINEERING")
    print("=" * 70)
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    print(f"\n📁 Dữ liệu đã xử lý được lưu tại: {OUTPUT_DIR}")
    print("=" * 70)
    
    return results_df


def get_feature_list():
    """Trả về danh sách tất cả features"""
    features = {
        'Technical Indicators': [
            'RSI', 'RSI_overbought', 'RSI_oversold',
            'MACD', 'MACD_signal', 'MACD_histogram', 'MACD_crossover',
            'BB_upper', 'BB_middle', 'BB_lower', 'BB_width', 'BB_position',
            'ATR', 'ATR_percent',
            'Stoch_K', 'Stoch_D',
            'CCI', 'Williams_R',
            'Momentum_10', 'ROC_10',
            'SMA_7/14/21/50/100/200', 'EMA_7/14/21/50/100/200',
            'Close_vs_SMA_*', 'MA_crossovers'
        ],
        'Price Features': [
            'Return', 'Log_Return',
            'Price_Change', 'Price_Change_Percent',
            'Range', 'Body', 'Body_Percent',
            'Upper_Shadow', 'Lower_Shadow',
            'Candle_Color', 'Gap', 'Gap_Percent',
            'Volatility_5/10/20',
            'HL_Ratio', 'Close_Position'
        ],
        'Lagged Features': [
            'Close_lag_1/2/3/5/7/14/21',
            'Return_lag_1/2/3/5/7/14/21',
            'RSI_lag_1/3/5',
            'Cumulative_Return_3/5/10/20',
            'Rolling_Mean/Std/Min/Max_5/10/20'
        ],
        'Time Features': [
            'Day_of_Week', 'Day_of_Month', 'Month', 'Quarter', 'Year', 'Week_of_Year',
            'Is_Monday', 'Is_Friday', 'Is_Month_Start/End', 'Is_Quarter_Start/End',
            'Day_sin/cos', 'Month_sin/cos'
        ],
        'Target Variables': [
            'Target_Direction (Binary: 1=Up, 0=Down)',
            'Target_Return (Regression)',
            'Target_Close (Regression)',
            'Target_Direction_Multi (Multi-class)'
        ]
    }
    
    print("\n📋 DANH SÁCH TẤT CẢ FEATURES:")
    print("=" * 50)
    for category, feature_list in features.items():
        print(f"\n🔹 {category}:")
        for f in feature_list:
            print(f"   - {f}")
    
    return features


# ============================================
# CHẠY CHƯƠNG TRÌNH
# ============================================

if __name__ == "__main__":
    # In danh sách features
    get_feature_list()
    
    # Xử lý dữ liệu
    process_all_data()
