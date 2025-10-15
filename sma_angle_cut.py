import pandas as pd
import numpy as np
from pathlib import Path

print("="*70)
print("SMA ANGLE BUY ZONE FILTER")
print("="*70)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Folder containing stock CSV files
DATA_FOLDER = 'nifty_data_2015-01-01_to_2024-12-31_20251016_015948'

# Input file from volume cut
VOLUME_CUT_FILE = 'volume_cut_10lakh.csv'

# SMA windows
SMA_200_WINDOW = 200
SMA_50_WINDOW = 50

# Angle threshold in degrees
ANGLE_THRESHOLD = 0.2

# Output file name
OUTPUT_FILE = 'sma_angle_cut.csv'

# ============================================================================
# STEP 1: Read volume_cut_10lakh.csv
# ============================================================================
print(f"\n📁 Reading {VOLUME_CUT_FILE}...")

volume_cut_df = pd.read_csv(VOLUME_CUT_FILE)
volume_cut_df['date'] = pd.to_datetime(volume_cut_df['date'])

print(f"✓ Loaded {len(volume_cut_df)} month-end dates from volume cut file")
print(f"  Date range: {volume_cut_df['date'].iloc[0].date()} to {volume_cut_df['date'].iloc[-1].date()}")

# ============================================================================
# STEP 2: Read all stock CSV files and calculate SMAs
# ============================================================================
print(f"\n📁 Reading stock data from: {DATA_FOLDER}")

csv_files = list(Path(DATA_FOLDER).glob('*.csv'))

# Filter out non-stock files
stock_files = [
    f for f in csv_files
    if f.stem not in ['NIFTY500_INDEX', 'summary_report', 'combined_data']
]

print(f"✓ Found {len(stock_files)} stock CSV files")

# Dictionary to store stock data with SMAs and angle
stock_data = {}

print(f"\n📊 Loading stock data and calculating SMAs + Angle...")
for i, csv_file in enumerate(stock_files, 1):
    stock_name = csv_file.stem

    try:
        # Read CSV with datetime index
        df = pd.read_csv(csv_file, index_col='datetime', parse_dates=True)

        if 'close' in df.columns and not df.empty:
            # Calculate 200-day and 50-day SMAs
            df['ma200'] = df['close'].rolling(window=SMA_200_WINDOW).mean()
            df['ma50'] = df['close'].rolling(window=SMA_50_WINDOW).mean()

            # Calculate angle of 200 SMA
            # angle = arctan((ma200 - ma200[1]) / (close * 0.01)) * 180 / π
            ma200_diff = df['ma200'] - df['ma200'].shift(1)
            normalized_slope = ma200_diff / (df['close'] * 0.01)
            df['angle'] = np.arctan(normalized_slope) * 180 / np.pi

            # Store only necessary columns
            stock_data[stock_name] = df[['close', 'ma200', 'ma50', 'angle']].copy()

            if i % 50 == 0:
                print(f"   Processed {i}/{len(stock_files)} stocks...")
    except Exception as e:
        print(f"   ⚠ Error loading {stock_name}: {e}")

print(f"✓ Successfully loaded {len(stock_data)} stocks with SMA and angle data")

# ============================================================================
# STEP 3: Find the first valid month-end (after 200 days of data)
# ============================================================================
print(f"\n📅 Finding first valid month-end date (after {SMA_200_WINDOW} days)...")

# Get the earliest date across all stocks
min_date = min([df.index.min() for df in stock_data.values()])
print(f"  Earliest stock data: {min_date.date()}")

# Calculate the first date where we have 200 days of data
# This is approximately min_date + 200 trading days (accounting for weekends)
first_valid_date = min_date + pd.Timedelta(days=SMA_200_WINDOW * 1.5)

# Find the first month-end in volume_cut that's after this date
volume_cut_filtered = volume_cut_df[volume_cut_df['date'] >= first_valid_date].copy()

print(f"  First valid month-end: {volume_cut_filtered['date'].iloc[0].date()}")
print(f"  Total valid month-ends: {len(volume_cut_filtered)}")

# ============================================================================
# STEP 4: Filter stocks in BUY ZONE on month-end dates
# ============================================================================
print(f"\n🔍 Filtering Buy Zone stocks on month-ends...")
print(f"  Buy Zone Conditions:")
print(f"    - Angle > {ANGLE_THRESHOLD}°")
print(f"    - Close > 50 SMA")
print(f"    - Close > 200 SMA")

results = []

for idx, row in volume_cut_filtered.iterrows():
    month_end_date = row['date']
    volume_filtered_stocks = row['stocks']

    # Skip if no stocks passed volume filter
    if pd.isna(volume_filtered_stocks) or volume_filtered_stocks == '':
        results.append({
            'date': month_end_date.strftime('%Y-%m-%d'),
            'stocks': '',
            'count': 0
        })
        continue

    # Get list of stocks that passed volume filter
    volume_stocks_list = [s.strip() for s in volume_filtered_stocks.split(',')]

    # List to store stocks that pass buy zone conditions
    buy_zone_stocks = []

    for stock_name in volume_stocks_list:
        # Check if we have data for this stock
        if stock_name in stock_data:
            df = stock_data[stock_name]

            # Find the matching date (accounting for time component)
            matching_dates = df.index[df.index.date == month_end_date.date()]

            if len(matching_dates) > 0:
                actual_date = matching_dates[0]

                close_price = df.loc[actual_date, 'close']
                ma200 = df.loc[actual_date, 'ma200']
                ma50 = df.loc[actual_date, 'ma50']
                angle = df.loc[actual_date, 'angle']

                # Buy Zone Conditions:
                # 1. angle > angleThreshold
                # 2. close > ma50
                # 3. close > ma200
                if (pd.notna(angle) and pd.notna(ma200) and pd.notna(ma50) and
                    angle > ANGLE_THRESHOLD and
                    close_price > ma50 and
                    close_price > ma200):
                    buy_zone_stocks.append(stock_name)

    # Add to results
    results.append({
        'date': month_end_date.strftime('%Y-%m-%d'),
        'stocks': ', '.join(sorted(buy_zone_stocks)),
        'count': len(buy_zone_stocks)
    })

print(f"✓ Processed {len(volume_cut_filtered)} month-end dates")

# ============================================================================
# STEP 5: Generate sma_angle_cut.csv
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
print(f"📊 Filters Applied:")
print(f"    1. Volume > 10 lakh (20-day avg)")
print(f"    2. 200 SMA Angle > {ANGLE_THRESHOLD}°")
print(f"    3. Close > 50 SMA")
print(f"    4. Close > 200 SMA")

# Show statistics
print("\n📈 Stock count statistics per month-end:")
print(f"   Average stocks per month: {result_df['count'].mean():.1f}")
print(f"   Min stocks in a month: {result_df['count'].min()}")
print(f"   Max stocks in a month: {result_df['count'].max()}")

# Show sample
print("\n📋 Sample data (first 10 month-ends):")
print(result_df[['date', 'count']].head(10).to_string(index=False))

# Show comparison with volume cut
print("\n📊 Comparison: Volume Cut vs Angle Buy Zone")
volume_cut_comparison = volume_cut_filtered[['date', 'count']].copy()
volume_cut_comparison['date'] = volume_cut_comparison['date'].dt.strftime('%Y-%m-%d')
result_comparison = result_df[['date', 'count']].copy()

comparison_df = pd.merge(
    volume_cut_comparison.rename(columns={'count': 'volume_count'}),
    result_comparison.rename(columns={'count': 'angle_count'}),
    on='date'
)
comparison_df['reduction_%'] = ((comparison_df['volume_count'] - comparison_df['angle_count']) / comparison_df['volume_count'] * 100).round(1)

print(f"\n   Average reduction: {comparison_df['reduction_%'].mean():.1f}%")
print(f"   Avg stocks after volume filter: {comparison_df['volume_count'].mean():.1f}")
print(f"   Avg stocks in buy zone: {comparison_df['angle_count'].mean():.1f}")

print("\n" + "="*70)
print(f"✓ DONE! Check {OUTPUT_FILE} for full results")
print("="*70)
