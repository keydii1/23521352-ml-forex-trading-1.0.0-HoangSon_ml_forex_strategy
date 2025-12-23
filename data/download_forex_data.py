"""
Forex Data Downloader for Machine Learning Research
=====================================================
Tải dữ liệu lịch sử của 5 cặp tiền chính (Major Pairs) từ 2020-2025
Các khung thời gian: Daily, 4H, 1H

Tác giả: Nhóm nghiên cứu Forex ML
Ngày tạo: 2025-12-23
Cập nhật: Mở rộng thời gian từ 2020
"""

import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
import warnings
import time
warnings.filterwarnings('ignore')

# ============================================
# CẤU HÌNH
# ============================================

# 5 cặp tiền Major đại diện
CURRENCY_PAIRS = {
    'EURUSD=X': 'EUR_USD',   # Euro vs US Dollar
    'GBPUSD=X': 'GBP_USD',   # British Pound vs US Dollar
    'USDJPY=X': 'USD_JPY',   # US Dollar vs Japanese Yen
    'AUDUSD=X': 'AUD_USD',   # Australian Dollar vs US Dollar
    'USDCHF=X': 'USD_CHF'    # US Dollar vs Swiss Franc
}

# Thời gian nghiên cứu - MỞ RỘNG TỪ 2020
START_DATE = '2020-01-01'
END_DATE = '2025-12-31'

# Thư mục lưu trữ
OUTPUT_DIRS = {
    'daily': 'daily',
    '4h': '4h',
    '1h': '1h'
}

# ============================================
# HÀM TẢI DỮ LIỆU
# ============================================

def create_directories():
    """Tạo các thư mục lưu trữ dữ liệu"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    for timeframe in OUTPUT_DIRS.values():
        dir_path = os.path.join(base_dir, timeframe)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"✓ Đã tạo thư mục: {dir_path}")
    
    return base_dir


def clean_data(data):
    """Làm sạch và chuẩn hóa dữ liệu"""
    if len(data) == 0:
        return None
    
    data = data.reset_index()
    
    # Đổi tên cột nếu là MultiIndex
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] if col[1] == '' else col[0] for col in data.columns]
    
    # Chuẩn hóa tên cột
    col_mapping = {
        'Datetime': 'Date',
        'datetime': 'Date',
        'index': 'Date'
    }
    data.rename(columns=col_mapping, inplace=True)
    
    # Sắp xếp lại cột
    expected_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    available_cols = [col for col in expected_cols if col in data.columns]
    data = data[available_cols]
    
    # Đảm bảo đúng thứ tự OHLCV
    if len(data.columns) >= 5:
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'][:len(data.columns)]
    
    return data


def download_daily_data(ticker, name, base_dir):
    """Tải dữ liệu khung Daily từ 2020"""
    print(f"  📥 Đang tải {name} Daily (2020-2025)...")
    
    try:
        data = yf.download(
            ticker, 
            start=START_DATE, 
            end=END_DATE, 
            interval='1d',
            progress=False
        )
        
        if len(data) == 0:
            print(f"  ⚠️  Không có dữ liệu cho {name}")
            return None
        
        data = clean_data(data)
        if data is None:
            return None
        
        # Lưu file
        output_path = os.path.join(base_dir, 'daily', f'{name}_daily.csv')
        data.to_csv(output_path, index=False)
        print(f"  ✅ Đã lưu {name} Daily: {len(data)} records ({data['Date'].min()} đến {data['Date'].max()})")
        
        return data
        
    except Exception as e:
        print(f"  ❌ Lỗi khi tải {name}: {str(e)}")
        return None


def download_hourly_data_chunked(ticker, name, base_dir):
    """
    Tải dữ liệu 1H theo từng chunk để vượt qua giới hạn 730 ngày
    Phương pháp: Tải nhiều đợt, mỗi đợt 59 ngày (giới hạn của yfinance cho 1h)
    """
    print(f"  📥 Đang tải {name} 1H (tải theo chunks)...")
    
    all_data = []
    end_date = datetime.now()
    start_date = datetime(2020, 1, 1)
    
    # yfinance cho phép tối đa 730 ngày với interval 1h
    # Nhưng thực tế hoạt động tốt hơn với chunk nhỏ hơn
    max_days = 59  # Giới hạn an toàn
    
    current_end = end_date
    chunks_downloaded = 0
    
    while current_end > start_date:
        current_start = max(current_end - timedelta(days=max_days), start_date)
        
        try:
            data = yf.download(
                ticker,
                start=current_start.strftime('%Y-%m-%d'),
                end=current_end.strftime('%Y-%m-%d'),
                interval='1h',
                progress=False
            )
            
            if len(data) > 0:
                all_data.append(data)
                chunks_downloaded += 1
            
            # Delay để tránh rate limit
            time.sleep(0.5)
            
        except Exception as e:
            print(f"    ⚠️ Lỗi chunk {current_start.date()} - {current_end.date()}: {str(e)}")
        
        current_end = current_start - timedelta(days=1)
    
    if not all_data:
        print(f"  ⚠️ Không có dữ liệu 1H cho {name}")
        return None
    
    # Gộp tất cả chunks
    combined_data = pd.concat(all_data)
    combined_data = combined_data.sort_index()
    combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
    
    combined_data = clean_data(combined_data)
    if combined_data is None:
        return None
    
    # Lưu file 1H
    output_path = os.path.join(base_dir, '1h', f'{name}_1h.csv')
    combined_data.to_csv(output_path, index=False)
    print(f"  ✅ Đã lưu {name} 1H: {len(combined_data)} records ({chunks_downloaded} chunks)")
    
    return combined_data


def resample_to_4h(data_1h, name, base_dir):
    """Chuyển đổi dữ liệu 1H thành 4H"""
    if data_1h is None or len(data_1h) == 0:
        return None
    
    print(f"  🔄 Đang chuyển đổi {name} sang 4H...")
    
    try:
        df = data_1h.copy()
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        # Resample sang 4H
        data_4h = df.resample('4h').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()
        
        data_4h = data_4h.reset_index()
        
        # Lưu file 4H
        output_path = os.path.join(base_dir, '4h', f'{name}_4h.csv')
        data_4h.to_csv(output_path, index=False)
        print(f"  ✅ Đã lưu {name} 4H: {len(data_4h)} records")
        
        return data_4h
        
    except Exception as e:
        print(f"  ❌ Lỗi khi chuyển đổi {name} sang 4H: {str(e)}")
        return None


def download_all_data():
    """Hàm chính để tải tất cả dữ liệu"""
    print("=" * 70)
    print("🚀 BẮT ĐẦU TẢI DỮ LIỆU FOREX (MỞ RỘNG 2020-2025)")
    print(f"📅 Thời gian: {START_DATE} đến {END_DATE}")
    print(f"💱 Số cặp tiền: {len(CURRENCY_PAIRS)}")
    print("=" * 70)
    
    # Tạo thư mục
    base_dir = create_directories()
    
    # Thống kê kết quả
    results = {
        'daily': [],
        '4h': [],
        '1h': []
    }
    
    # Tải dữ liệu cho từng cặp tiền
    for ticker, name in CURRENCY_PAIRS.items():
        print(f"\n{'='*50}")
        print(f"📊 Đang xử lý {name}...")
        print(f"{'='*50}")
        
        # Tải Daily (đầy đủ 2020-2025)
        daily_data = download_daily_data(ticker, name, base_dir)
        if daily_data is not None:
            results['daily'].append(name)
        
        # Tải 1H theo chunks
        hourly_data = download_hourly_data_chunked(ticker, name, base_dir)
        if hourly_data is not None:
            results['1h'].append(name)
            
            # Chuyển đổi sang 4H
            data_4h = resample_to_4h(hourly_data, name, base_dir)
            if data_4h is not None:
                results['4h'].append(name)
    
    # In kết quả tổng hợp
    print("\n" + "=" * 70)
    print("📊 KẾT QUẢ TẢI DỮ LIỆU")
    print("=" * 70)
    print(f"✅ Daily: {len(results['daily'])}/{len(CURRENCY_PAIRS)} cặp tiền")
    print(f"✅ 4H:    {len(results['4h'])}/{len(CURRENCY_PAIRS)} cặp tiền")
    print(f"✅ 1H:    {len(results['1h'])}/{len(CURRENCY_PAIRS)} cặp tiền")
    print("\n📁 Dữ liệu được lưu tại:")
    print(f"   {base_dir}/daily/")
    print(f"   {base_dir}/4h/")
    print(f"   {base_dir}/1h/")
    print("=" * 70)
    
    return results


# ============================================
# CHẠY CHƯƠNG TRÌNH
# ============================================

if __name__ == "__main__":
    download_all_data()
