"""
Forex Historical 1H Data Downloader from HistData.com
======================================================
Tải dữ liệu lịch sử 1H từ 2020-2023 từ HistData.com
(Bổ sung cho dữ liệu yfinance bị giới hạn 730 ngày)

Hướng dẫn sử dụng:
1. Truy cập https://www.histdata.com/download-free-forex-data/
2. Chọn "1 Hour Bars" hoặc "M1 Data" (sẽ resample)
3. Tải các file zip cho từng năm 2020, 2021, 2022, 2023
4. Giải nén và đặt vào thư mục histdata/
5. Chạy script này để xử lý

Hoặc sử dụng cách khác đơn giản hơn bên dưới.
"""

import pandas as pd
import os

# ============================================
# CÁCH 1: SỬ DỤNG FOREXSB (KHUYÊN DÙNG)
# ============================================

FOREXSB_INSTRUCTIONS = """
╔══════════════════════════════════════════════════════════════════════╗
║           HƯỚNG DẪN TẢI DỮ LIỆU 1H LỊCH SỬ (2020-2023)              ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Nguồn 1: ForexSB (Dễ nhất - File CSV sẵn)                          ║
║  https://forexsb.com/historical-forex-data                          ║
║                                                                      ║
║  1. Truy cập link trên                                              ║
║  2. Chọn cặp tiền: EURUSD, GBPUSD, USDJPY, AUDUSD, USDCHF           ║
║  3. Chọn Period: 1 Hour                                             ║
║  4. Chọn Download: CSV                                              ║
║  5. Tải về và lưu vào thư mục:                                      ║
║     data/historical/                                                 ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Nguồn 2: Dukascopy (Chất lượng cao nhất)                           ║
║  https://www.dukascopy.com/swiss/english/marketwatch/historical/    ║
║                                                                      ║
║  1. Chọn Instrument: EUR/USD, etc.                                  ║
║  2. Chọn Period: Hourly                                             ║
║  3. Chọn ngày từ 01/01/2020 đến 31/12/2023                          ║
║  4. Download CSV                                                     ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Nguồn 3: HistData.com (Tick data - Cần xử lý)                      ║
║  https://www.histdata.com/download-free-forex-data/                 ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
"""

def print_instructions():
    print(FOREXSB_INSTRUCTIONS)

# ============================================
# CÁCH 2: TẢI TỰ ĐỘNG TỪ FOREXSB
# ============================================

def download_from_forexsb():
    """
    Tải dữ liệu từ ForexSB API (nếu có)
    Hiện tại ForexSB không có API công khai, cần tải thủ công
    """
    print("\n⚠️  ForexSB không có API tự động.")
    print("Vui lòng tải thủ công theo hướng dẫn ở trên.\n")
    print_instructions()

# ============================================
# CÁCH 3: XỬ LÝ FILE ĐÃ TẢI
# ============================================

def process_histdata_files(input_dir, output_dir):
    """
    Xử lý các file đã tải từ HistData hoặc ForexSB
    Chuẩn hóa và gộp thành 1 file cho mỗi cặp tiền
    """
    if not os.path.exists(input_dir):
        print(f"❌ Thư mục {input_dir} không tồn tại!")
        print("Vui lòng tạo thư mục và đặt các file CSV đã tải vào đó.")
        return
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    pairs = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCHF']
    
    for pair in pairs:
        print(f"\n📊 Đang xử lý {pair}...")
        
        # Tìm tất cả file của cặp tiền này
        files = [f for f in os.listdir(input_dir) if pair in f.upper() and f.endswith('.csv')]
        
        if not files:
            print(f"  ⚠️  Không tìm thấy file cho {pair}")
            continue
        
        all_data = []
        for file in sorted(files):
            filepath = os.path.join(input_dir, file)
            try:
                # Thử đọc với nhiều format khác nhau
                df = pd.read_csv(filepath)
                
                # Chuẩn hóa tên cột
                df.columns = df.columns.str.strip()
                if len(df.columns) >= 6:
                    df.columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Close'][:len(df.columns)]
                    if 'Time' in df.columns:
                        df['Date'] = df['Date'].astype(str) + ' ' + df['Time'].astype(str)
                        df = df.drop('Time', axis=1)
                
                all_data.append(df)
                print(f"  ✅ Đã đọc {file}: {len(df)} records")
                
            except Exception as e:
                print(f"  ❌ Lỗi đọc {file}: {str(e)}")
        
        if all_data:
            combined = pd.concat(all_data)
            combined = combined.sort_values('Date')
            combined = combined.drop_duplicates(subset=['Date'], keep='first')
            
            output_file = os.path.join(output_dir, f'{pair}_1h_historical.csv')
            combined.to_csv(output_file, index=False)
            print(f"  ✅ Đã lưu {output_file}: {len(combined)} records")

# ============================================
# CÁCH 4: GỘP VỚI DỮ LIỆU YFINANCE
# ============================================

def merge_with_yfinance(historical_dir, yfinance_dir, output_dir):
    """
    Gộp dữ liệu lịch sử với dữ liệu yfinance
    để có dataset đầy đủ từ 2020-2025
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    pairs = ['EUR_USD', 'GBP_USD', 'USD_JPY', 'AUD_USD', 'USD_CHF']
    
    for pair in pairs:
        print(f"\n📊 Đang gộp {pair}...")
        
        # Đọc file yfinance
        yf_file = os.path.join(yfinance_dir, f'{pair}_1h.csv')
        hist_file = os.path.join(historical_dir, f'{pair.replace("_", "")}_1h_historical.csv')
        
        yf_data = None
        hist_data = None
        
        if os.path.exists(yf_file):
            yf_data = pd.read_csv(yf_file)
            yf_data['Date'] = pd.to_datetime(yf_data['Date'])
            print(f"  ✅ yfinance: {len(yf_data)} records")
        
        if os.path.exists(hist_file):
            hist_data = pd.read_csv(hist_file)
            hist_data['Date'] = pd.to_datetime(hist_data['Date'])
            print(f"  ✅ historical: {len(hist_data)} records")
        
        # Gộp dữ liệu
        if yf_data is not None and hist_data is not None:
            combined = pd.concat([hist_data, yf_data])
            combined = combined.sort_values('Date')
            combined = combined.drop_duplicates(subset=['Date'], keep='last')
            
            output_file = os.path.join(output_dir, f'{pair}_1h_full.csv')
            combined.to_csv(output_file, index=False)
            print(f"  ✅ Đã gộp: {len(combined)} records")
            
        elif yf_data is not None:
            output_file = os.path.join(output_dir, f'{pair}_1h_full.csv')
            yf_data.to_csv(output_file, index=False)
            print(f"  ⚠️  Chỉ có yfinance: {len(yf_data)} records")
        else:
            print(f"  ❌ Không có dữ liệu cho {pair}")

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("=" * 70)
    print("🔧 CÔNG CỤ XỬ LÝ DỮ LIỆU 1H LỊCH SỬ")
    print("=" * 70)
    
    # In hướng dẫn
    print_instructions()
    
    print("\n📁 CẤU TRÚC THƯ MỤC CẦN THIẾT:")
    print(f"   {base_dir}/historical/     <- Đặt file CSV tải từ ForexSB/HistData")
    print(f"   {base_dir}/1h/             <- Dữ liệu yfinance (đã có)")
    print(f"   {base_dir}/1h_full/        <- Output: Dữ liệu gộp đầy đủ")
    
    # Tạo thư mục historical nếu chưa có
    historical_dir = os.path.join(base_dir, 'historical')
    if not os.path.exists(historical_dir):
        os.makedirs(historical_dir)
        print(f"\n✓ Đã tạo thư mục: {historical_dir}")
    
    print("\n" + "=" * 70)
    print("Sau khi tải dữ liệu vào thư mục historical/, chạy lại script này")
    print("với tùy chọn process để xử lý và gộp dữ liệu.")
    print("=" * 70)
