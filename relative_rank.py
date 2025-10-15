import pandas as pd
import numpy as np
from pathlib import Path
from dateutil.relativedelta import relativedelta

print("="*70)
print("RELATIVE STRENGTH RANKING")
print("="*70)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Folder containing stock CSV files
DATA_FOLDER = 'nifty_data_2015-01-01_to_2024-12-31_20251016_015948'

# Input file from SMA angle cut
SMA_ANGLE_CUT_FILE = 'sma_angle_2cut.csv'

# Nifty 500 Index CSV file
NIFTY_INDEX_FILE = 'nifty_data_2015-01-01_to_2024-12-31_20251016_022023/NIFTY500_INDEX.csv'

# Number of top stocks to select
TOP_N = 30

# Lookback period for relative strength (3 months)
LOOKBACK_MONTHS = 1

# Output file name
OUTPUT_FILE = 'relative_rank.csv'

# ============================================================================
# STEP 1: Read sma_angle_cut.csv
# ============================================================================
print(f"\n📁 Reading {SMA_ANGLE_CUT_FILE}...")

sma_angle_df = pd.read_csv(SMA_ANGLE_CUT_FILE)
sma_angle_df['date'] = pd.to_datetime(sma_angle_df['date'])

print(f"✓ Loaded {len(sma_angle_df)} month-end dates")
print(f"  Date range: {sma_angle_df['date'].iloc[0].date()} to {sma_angle_df['date'].iloc[-1].date()}")

# ============================================================================
# STEP 2: Read Nifty 500 Index data
# ============================================================================
print(f"\n📁 Reading Nifty 500 Index data from {NIFTY_INDEX_FILE}...")

nifty_df = pd.read_csv(NIFTY_INDEX_FILE, index_col='datetime', parse_dates=True)
print(f"✓ Loaded Nifty 500 Index data")
print(f"  Date range: {nifty_df.index[0].date()} to {nifty_df.index[-1].date()}")

# ============================================================================
# STEP 3: Read all stock CSV files
# ============================================================================
print(f"\n📁 Reading stock data from: {DATA_FOLDER}")

csv_files = list(Path(DATA_FOLDER).glob('*.csv'))

# Filter out non-stock files
stock_files = [
    f for f in csv_files
    if f.stem not in ['NIFTY500_INDEX', 'summary_report', 'combined_data']
]

print(f"✓ Found {len(stock_files)} stock CSV files")

# Dictionary to store stock data (only close prices)
stock_data = {}

print(f"\n📊 Loading stock price data...")
for i, csv_file in enumerate(stock_files, 1):
    stock_name = csv_file.stem

    try:
        # Read CSV with datetime index
        df = pd.read_csv(csv_file, index_col='datetime', parse_dates=True)

        if 'close' in df.columns and not df.empty:
            stock_data[stock_name] = df[['close']].copy()

            if i % 50 == 0:
                print(f"   Loaded {i}/{len(stock_files)} stocks...")
    except Exception as e:
        print(f"   ⚠ Error loading {stock_name}: {e}")

print(f"✓ Successfully loaded {len(stock_data)} stocks")

# ============================================================================
# STEP 4: Calculate relative strength for each month-end
# ============================================================================
print(f"\n📈 Calculating relative strength rankings...")
print(f"  Lookback period: {LOOKBACK_MONTHS} months")
print(f"  Top stocks to select: {TOP_N}")

results = []

for idx, row in sma_angle_df.iterrows():
    month_end_date = row['date']
    angle_stocks = row['stocks']

    # Skip if no stocks passed angle filter
    if pd.isna(angle_stocks) or angle_stocks == '':
        results.append({
            'date': month_end_date.strftime('%Y-%m-%d'),
            'stocks': ''
        })
        continue

    # Get list of stocks that passed angle filter
    angle_stocks_list = [s.strip() for s in angle_stocks.split(',')]

    # Calculate date 3 months back
    three_months_back = month_end_date - relativedelta(months=LOOKBACK_MONTHS)

    # Calculate Nifty 500 return over 3 months
    nifty_current_dates = nifty_df.index[nifty_df.index.date == month_end_date.date()]
    nifty_past_dates = nifty_df.index[nifty_df.index.date == three_months_back.date()]

    # Skip if we don't have Nifty data for both dates
    if len(nifty_current_dates) == 0 or len(nifty_past_dates) == 0:
        results.append({
            'date': month_end_date.strftime('%Y-%m-%d'),
            'stocks': ''
        })
        continue

    nifty_current_price = nifty_df.loc[nifty_current_dates[0], 'close']
    nifty_past_price = nifty_df.loc[nifty_past_dates[0], 'close']
    nifty_return = (nifty_current_price - nifty_past_price) / nifty_past_price

    # Skip if Nifty return is zero (to avoid division by zero)
    if nifty_return == 0:
        results.append({
            'date': month_end_date.strftime('%Y-%m-%d'),
            'stocks': ''
        })
        continue

    # Calculate relative strength for each stock
    stock_rs_data = []

    for stock_name in angle_stocks_list:
        if stock_name in stock_data:
            df = stock_data[stock_name]

            # Find matching dates for current and 3 months back
            current_dates = df.index[df.index.date == month_end_date.date()]
            past_dates = df.index[df.index.date == three_months_back.date()]

            if len(current_dates) > 0 and len(past_dates) > 0:
                current_price = df.loc[current_dates[0], 'close']
                past_price = df.loc[past_dates[0], 'close']

                # Calculate stock return
                stock_return = (current_price - past_price) / past_price

                # Calculate relative strength ratio
                rs_ratio = stock_return / nifty_return

                # Only include stocks with RS ratio >= 1
                if rs_ratio >= 1:
                    stock_rs_data.append({
                        'stock': stock_name,
                        'rs_ratio': rs_ratio
                    })

    # Sort by RS ratio (descending) and take top N
    stock_rs_data.sort(key=lambda x: x['rs_ratio'], reverse=True)
    top_stocks = stock_rs_data[:TOP_N]

    # Format output: "STOCK1 (ratio), STOCK2 (ratio), ..."
    if len(top_stocks) > 0:
        formatted_stocks = ', '.join([f"{s['stock']} ({s['rs_ratio']:.2f})" for s in top_stocks])
    else:
        formatted_stocks = ''

    results.append({
        'date': month_end_date.strftime('%Y-%m-%d'),
        'stocks': formatted_stocks
    })

    if (idx + 1) % 12 == 0:
        print(f"   Processed {idx + 1}/{len(sma_angle_df)} month-ends...")

print(f"✓ Processed all {len(sma_angle_df)} month-end dates")

# ============================================================================
# STEP 5: Generate relative_rank.csv
# ============================================================================
print(f"\n💾 Generating {OUTPUT_FILE}...")

# Create DataFrame
result_df = pd.DataFrame(results)

# Save to CSV
result_df.to_csv(OUTPUT_FILE, index=False)

print(f"✓ {OUTPUT_FILE} created successfully!")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"\n📊 Total month-end dates: {len(result_df)}")
print(f"📊 Date range: {result_df['date'].iloc[0]} to {result_df['date'].iloc[-1]}")
print(f"📊 Ranking Method: Relative Strength vs Nifty 500")
print(f"📊 Lookback Period: {LOOKBACK_MONTHS} months")
print(f"📊 Top Stocks Selected: {TOP_N} per month")

# Calculate statistics
result_df['stock_count'] = result_df['stocks'].apply(
    lambda x: len([s for s in x.split(',') if s.strip()]) if x else 0
)

print("\n📈 Stock count statistics per month-end:")
print(f"   Average stocks ranked per month: {result_df['stock_count'].mean():.1f}")
print(f"   Min stocks in a month: {result_df['stock_count'].min()}")
print(f"   Max stocks in a month: {result_df['stock_count'].max()}")

# Show sample
print("\n📋 Sample data (first 5 month-ends):")
for i in range(min(5, len(result_df))):
    date = result_df.iloc[i]['date']
    stocks = result_df.iloc[i]['stocks']
    count = result_df.iloc[i]['stock_count']

    print(f"\n  {date} ({count} stocks):")
    if stocks:
        # Show first 3 stocks for preview
        stocks_list = stocks.split(',')
        preview = ', '.join(stocks_list[:3])
        if len(stocks_list) > 3:
            preview += f", ... ({len(stocks_list) - 3} more)"
        print(f"    {preview}")
    else:
        print(f"    No stocks")

print("\n" + "="*70)
print(f"✓ DONE! Check {OUTPUT_FILE} for full results")
print("="*70)
