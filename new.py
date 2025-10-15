from tvDatafeed import TvDatafeed, Interval
import pandas as pd
from datetime import datetime
import os
import time
from config import NIFTY_500_STOCKS

print("="*70)
print("NIFTY 500 DATA FETCHER - CUSTOM DATE RANGE")
print("="*70)

# ============================================================================
# CONFIGURATION - MODIFY THESE VALUES
# ============================================================================

# Define your date range here
START_DATE = '2015-01-01'  # Format: 'YYYY-MM-DD'
END_DATE = '2024-12-31'    # Format: 'YYYY-MM-DD'

# Use all Nifty 500 stocks from config.py
TEST_STOCKS = NIFTY_500_STOCKS

# Nifty 500 Index symbol
NIFTY_INDEX_SYMBOL = 'NIFTY'  # Try 'NIFTY' or 'NIFTY500'

# Configuration
EXCHANGE = 'NSE'
INTERVAL = Interval.in_daily  # Daily timeframe
DELAY_SECONDS = 0.5  # Delay between API calls

# ============================================================================
# END OF CONFIGURATION
# ============================================================================

print(f"\n📅 Date Range Requested:")
print(f"   START: {START_DATE}")
print(f"   END:   {END_DATE}")
print(f"\n📊 Stocks to fetch: {', '.join(TEST_STOCKS)}")
print(f"📊 Index: {NIFTY_INDEX_SYMBOL}")

# Convert date strings to datetime
start_dt = pd.to_datetime(START_DATE)
end_dt = pd.to_datetime(END_DATE)
date_range_days = (end_dt - start_dt).days

print(f"\n📆 Date range: {date_range_days} calendar days")
print(f"📆 Approximate trading days: {int(date_range_days * 0.7)} days")

# Calculate required bars (fetch more than needed, then filter)
# Strategy: Fetch from END_DATE backwards to ensure we have enough data
days_to_fetch = (datetime.now() - start_dt).days
bars_to_fetch = int(days_to_fetch * 0.75)  # ~75% are trading days
bars_to_fetch = min(bars_to_fetch + 500, 5000)  # Add buffer, max 5000

print(f"\n🔄 Will fetch {bars_to_fetch} bars, then filter to your date range")

# Initialize TVDataFeed
print("\n" + "="*70)
print("INITIALIZING")
print("="*70)
tv = TvDatafeed()
print("✓ TVDataFeed initialized (no-login mode)")

# Storage
all_data = {}
failed = []

# Create output folder
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_folder = f'nifty_data_{START_DATE}_to_{END_DATE}_{timestamp}'
os.makedirs(output_folder, exist_ok=True)

def fetch_and_filter_data(symbol, exchange, start_date, end_date):
    """Fetch data and filter to specific date range"""
    try:
        # Fetch data
        data = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=INTERVAL,
            n_bars=bars_to_fetch
        )
        
        if data is None or data.empty:
            return None, "No data returned"
        
        # Filter to date range
        data_filtered = data.loc[start_date:end_date]
        
        if data_filtered.empty:
            return None, f"No data in range {start_date} to {end_date}"
        
        return data_filtered, None
        
    except Exception as e:
        return None, str(e)

# ============================================================================
# FETCH NIFTY INDEX
# ============================================================================
print("\n" + "="*70)
print("FETCHING NIFTY 500 INDEX")
print("="*70)

index_fetched = False
# Try multiple variations of Nifty 500 index symbol
index_symbols_to_try = [
    ('NIFTY 500', 'NSE'),
    ('NIFTY500', 'NSE'),
    ('NIFTY_500', 'NSE'),
    ('NIFTY 500', 'INDEX'),
    ('NIFTY', 'NSE')  # Fallback to NIFTY 50 if Nifty 500 not available
]

for idx_symbol, idx_exchange in index_symbols_to_try:
    print(f"\nTrying symbol: {idx_symbol} on {idx_exchange}...", end=" ")
    try:
        data = tv.get_hist(
            symbol=idx_symbol,
            exchange=idx_exchange,
            interval=INTERVAL,
            n_bars=bars_to_fetch
        )

        if data is not None and not data.empty:
            # Filter to date range
            data_filtered = data.loc[START_DATE:END_DATE]

            if not data_filtered.empty:
                all_data[f'NIFTY500_INDEX'] = data_filtered
                print(f"✓ Success!")
                print(f"  Bars in range: {len(data_filtered)}")
                print(f"  Date range: {data_filtered.index[0].date()} to {data_filtered.index[-1].date()}")
                index_fetched = True
                break
            else:
                print(f"✗ No data in date range")
        else:
            print(f"✗ No data returned")
    except Exception as e:
        print(f"✗ {str(e)}")

if not index_fetched:
    print("\n⚠ Could not fetch Nifty 500 Index. Continuing with stocks...")

# ============================================================================
# FETCH STOCKS
# ============================================================================
# COMMENTED OUT - Only fetching index for now
# print("\n" + "="*70)
# print(f"FETCHING {len(TEST_STOCKS)} STOCKS")
# print("="*70)

# for i, stock in enumerate(TEST_STOCKS, 1):
#     print(f"\n[{i}/{len(TEST_STOCKS)}] {stock:15s} ", end="")

#     data, error = fetch_and_filter_data(stock, EXCHANGE, START_DATE, END_DATE)

#     if data is not None:
#         all_data[stock] = data
#         print(f"✓ Success!")
#         print(f"  Bars: {len(data):4d} | {data.index[0].date()} to {data.index[-1].date()}")
#     else:
#         failed.append(stock)
#         print(f"✗ Failed - {error}")

#     time.sleep(DELAY_SECONDS)

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("FETCH SUMMARY")
print("="*70)
print(f"✓ Successfully fetched: {len(all_data)} symbols")
print(f"✗ Failed: {len(failed)} symbols")

if failed:
    print(f"\nFailed symbols: {', '.join(failed)}")

# ============================================================================
# SAVE DATA
# ============================================================================
print("\n" + "="*70)
print("SAVING DATA")
print("="*70)

if len(all_data) == 0:
    print("⚠ No data to save!")
else:
    # Save individual CSV files
    print("\n1. Saving individual CSV files...")
    for symbol, data in all_data.items():
        filename = f"{output_folder}/{symbol}.csv"
        data.to_csv(filename)
        print(f"   ✓ {symbol}.csv")
    
    # Save combined Excel file
    print("\n2. Creating combined Excel file...")
    excel_file = f"{output_folder}/combined_data.xlsx"
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        for symbol, data in all_data.items():
            sheet_name = symbol[:31]  # Excel limit
            # Reset index to make datetime a column and format it properly
            data_to_save = data.reset_index()
            data_to_save['datetime'] = pd.to_datetime(data_to_save['datetime']).dt.strftime('%Y-%m-%d')
            data_to_save.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"   ✓ combined_data.xlsx")
    
    # Create summary report
    print("\n3. Creating summary report...")
    summary_data = []
    for symbol, data in all_data.items():
        summary_data.append({
            'Symbol': symbol,
            'Total_Bars': len(data),
            'Start_Date': data.index[0].date(),
            'End_Date': data.index[-1].date(),
            'First_Close': data['close'].iloc[0],
            'Last_Close': data['close'].iloc[-1],
            'Change_%': ((data['close'].iloc[-1] - data['close'].iloc[0]) / data['close'].iloc[0] * 100)
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_file = f"{output_folder}/summary_report.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"   ✓ summary_report.csv")
    
    # Display summary
    print("\n" + "="*70)
    print("DATA SUMMARY")
    print("="*70)
    print(summary_df.to_string(index=False))

# ============================================================================
# SAMPLE DATA PREVIEW
# ============================================================================
if len(all_data) > 0:
    print("\n" + "="*70)
    print("SAMPLE DATA PREVIEW (First Stock)")
    print("="*70)
    first_symbol = list(all_data.keys())[0]
    first_data = all_data[first_symbol]
    
    print(f"\n{first_symbol}:")
    print(f"Shape: {first_data.shape}")
    print(f"\nFirst 5 rows:")
    print(first_data.head())
    print(f"\nLast 5 rows:")
    print(first_data.tail())

# ============================================================================
# FINAL OUTPUT
# ============================================================================
print("\n" + "="*70)
print("✓ DATA FETCH COMPLETE!")
print("="*70)
print(f"\n📁 All files saved in folder: {output_folder}/")
print(f"\n📊 Date range: {START_DATE} to {END_DATE}")
print(f"📊 Symbols fetched: {len(all_data)}")

print("\n" + "="*70)
print("WHAT YOU GOT:")
print("="*70)
print("""
1. Individual CSV files for each stock/index
2. combined_data.xlsx with all data in separate sheets
3. summary_report.csv with overview of all symbols

You can now:
- Open CSV files in Excel, Python, or any data tool
- Analyze the data for your date range
- Use it for backtesting, analysis, or visualization
""")

print("\n" + "="*70)
print("TO FETCH MORE STOCKS:")
print("="*70)
print("""
Simply edit the TEST_STOCKS list at the top:

TEST_STOCKS = [
    'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK',
    'HINDUNILVR', 'BHARTIARTL', 'SBIN', 'BAJFINANCE', 'LT',
    # Add more stocks here...
]

Then run the script again!
""")

print("\n" + "="*70)
print("TO CHANGE DATE RANGE:")
print("="*70)
print("""
Modify these lines at the top:

START_DATE = '2020-01-01'  # Your start date
END_DATE = '2024-12-31'    # Your end date

Examples:
- Last 1 year: START_DATE = '2024-01-01', END_DATE = '2024-12-31'
- Last 5 years: START_DATE = '2020-01-01', END_DATE = '2024-12-31'
- Specific period: START_DATE = '2021-06-01', END_DATE = '2023-12-31'
""")