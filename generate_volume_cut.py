import pandas as pd
import os
from pathlib import Path
from datetime import datetime

print("="*70)
print("VOLUME CUT GENERATOR")
print("="*70)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Folder containing stock CSV files
DATA_FOLDER = 'nifty_data_2015-01-01_to_2024-12-31_20251016_015948'

# Volume threshold (1 million = 10 lakh)
VOLUME_THRESHOLD = 500000

# Rolling window for average volume (20 trading days)
ROLLING_WINDOW = 20

# Output file name
OUTPUT_FILE = 'volume_cut.csv'

# ============================================================================
# STEP 1: Read all stock CSV files
# ============================================================================
print(f"\n📁 Reading stock data from: {DATA_FOLDER}")

csv_files = list(Path(DATA_FOLDER).glob('*.csv'))

# Filter out non-stock files
stock_files = [
    f for f in csv_files
    if f.stem not in ['NIFTY500_INDEX', 'summary_report', 'combined_data']
]

print(f"✓ Found {len(stock_files)} stock CSV files")

# Dictionary to store stock data
stock_data = {}

print("\n📊 Loading stock data...")
for i, csv_file in enumerate(stock_files, 1):
    stock_name = csv_file.stem

    try:
        # Read CSV with datetime index
        df = pd.read_csv(csv_file, index_col='datetime', parse_dates=True)

        if 'volume' in df.columns and not df.empty:
            stock_data[stock_name] = df[['volume']].copy()

            if i % 50 == 0:
                print(f"   Loaded {i}/{len(stock_files)} stocks...")
    except Exception as e:
        print(f"   ⚠ Error loading {stock_name}: {e}")

print(f"✓ Successfully loaded {len(stock_data)} stocks with volume data")

# ============================================================================
# STEP 2: Calculate 20-day rolling average volume for each stock
# ============================================================================
print(f"\n📈 Calculating {ROLLING_WINDOW}-day rolling average volume...")

for stock_name, df in stock_data.items():
    # Calculate 20-day rolling average
    df['avg_volume_20d'] = df['volume'].rolling(window=ROLLING_WINDOW).mean()

print("✓ Rolling averages calculated")

# ============================================================================
# STEP 3: Identify all month-end trading days
# ============================================================================
print("\n📅 Identifying month-end trading days...")

# Get all unique dates from all stocks
all_dates = set()
for df in stock_data.values():
    all_dates.update(df.index)

all_dates = sorted(all_dates)

# Convert to DataFrame for easier manipulation
date_df = pd.DataFrame({'date': all_dates})
date_df['date'] = pd.to_datetime(date_df['date'])
date_df = date_df.set_index('date')

# Group by year-month and get the last trading day of each month
month_ends = date_df.groupby(date_df.index.to_period('M')).apply(lambda x: x.index.max())
month_end_dates = sorted(month_ends.values)

print(f"✓ Found {len(month_end_dates)} month-end trading days")
print(f"  Date range: {pd.Timestamp(month_end_dates[0]).date()} to {pd.Timestamp(month_end_dates[-1]).date()}")

# ============================================================================
# STEP 4: Filter stocks with avg volume > 1,000,000 for each month-end
# ============================================================================
print(f"\n🔍 Filtering stocks with avg volume > {VOLUME_THRESHOLD:,} on month-ends...")

results = []

for i, month_end_date in enumerate(month_end_dates, 1):
    # List to store stocks that pass the filter on this date
    passing_stocks = []

    for stock_name, df in stock_data.items():
        # Check if this date exists in the stock's data
        if month_end_date in df.index:
            avg_volume = df.loc[month_end_date, 'avg_volume_20d']

            # Check if avg volume exceeds threshold (and not NaN)
            if pd.notna(avg_volume) and avg_volume > VOLUME_THRESHOLD:
                passing_stocks.append(stock_name)

    # Add to results
    results.append({
        'date': pd.Timestamp(month_end_date).strftime('%Y-%m-%d'),
        'stocks': ', '.join(sorted(passing_stocks)),
        'count': len(passing_stocks)
    })

    if i % 12 == 0:
        print(f"   Processed {i}/{len(month_end_dates)} month-ends...")

print(f"✓ Processed all {len(month_end_dates)} month-end dates")

# ============================================================================
# STEP 5: Generate volume_cut.csv
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
print(f"📊 Volume threshold: {VOLUME_THRESHOLD:,} (10 lakh)")
print(f"📊 Rolling window: {ROLLING_WINDOW} days")

# Show statistics
print("\n📈 Stock count statistics per month-end:")
print(f"   Average stocks per month: {result_df['count'].mean():.1f}")
print(f"   Min stocks in a month: {result_df['count'].min()}")
print(f"   Max stocks in a month: {result_df['count'].max()}")

# Show sample
print("\n📋 Sample data (first 10 month-ends):")
print(result_df[['date', 'count']].head(10).to_string(index=False))

print("\n" + "="*70)
print(f"✓ DONE! Check {OUTPUT_FILE} for full results")
print("="*70)
