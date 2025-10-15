import pandas as pd
import os
from pathlib import Path
from datetime import datetime

print("="*70)
print("200 SMA CUT GENERATOR")
print("="*70)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Folder containing stock CSV files
DATA_FOLDER = 'nifty_data_2015-01-01_to_2024-12-31_20251016_015948'

# Input file from volume cut
VOLUME_CUT_FILE = 'volume_cut_5lakh.csv'

# SMA window (200 trading days)
SMA_WINDOW = 200

# Output file name
OUTPUT_FILE = '200sma_cut.csv'

# ============================================================================
# STEP 1: Read volume_cut_10lakh.csv
# ============================================================================
print(f"\n📁 Reading {VOLUME_CUT_FILE}...")

volume_cut_df = pd.read_csv(VOLUME_CUT_FILE)
volume_cut_df['date'] = pd.to_datetime(volume_cut_df['date'])

print(f"✓ Loaded {len(volume_cut_df)} month-end dates from volume cut file")
print(f"  Date range: {volume_cut_df['date'].iloc[0].date()} to {volume_cut_df['date'].iloc[-1].date()}")

# ============================================================================
# STEP 2: Read all stock CSV files and calculate 200-day SMA
# ============================================================================
print(f"\n📁 Reading stock data from: {DATA_FOLDER}")

csv_files = list(Path(DATA_FOLDER).glob('*.csv'))

# Filter out non-stock files
stock_files = [
    f for f in csv_files
    if f.stem not in ['NIFTY500_INDEX', 'summary_report', 'combined_data']
]

print(f"✓ Found {len(stock_files)} stock CSV files")

# Dictionary to store stock data with SMA
stock_data = {}

print(f"\n📊 Loading stock data and calculating {SMA_WINDOW}-day SMA...")
for i, csv_file in enumerate(stock_files, 1):
    stock_name = csv_file.stem

    try:
        # Read CSV with datetime index
        df = pd.read_csv(csv_file, index_col='datetime', parse_dates=True)

        if 'close' in df.columns and not df.empty:
            # Calculate 200-day SMA
            df['sma_200'] = df['close'].rolling(window=SMA_WINDOW).mean()

            # Store only close and SMA columns
            stock_data[stock_name] = df[['close', 'sma_200']].copy()

            if i % 50 == 0:
                print(f"   Processed {i}/{len(stock_files)} stocks...")
    except Exception as e:
        print(f"   ⚠ Error loading {stock_name}: {e}")

print(f"✓ Successfully loaded {len(stock_data)} stocks with SMA data")

# ============================================================================
# STEP 3: Find the first valid month-end (after 200 days of data)
# ============================================================================
print(f"\n📅 Finding first valid month-end date (after {SMA_WINDOW} days)...")

# Get the earliest date across all stocks
min_date = min([df.index.min() for df in stock_data.values()])
print(f"  Earliest stock data: {min_date.date()}")

# Calculate the first date where we have 200 days of data
# This is approximately min_date + 200 trading days
first_valid_date = min_date + pd.Timedelta(days=SMA_WINDOW * 1.5)  # Rough estimate accounting for weekends

# Find the first month-end in volume_cut that's after this date
volume_cut_filtered = volume_cut_df[volume_cut_df['date'] >= first_valid_date].copy()

print(f"  First valid month-end: {volume_cut_filtered['date'].iloc[0].date()}")
print(f"  Total valid month-ends: {len(volume_cut_filtered)}")

# ============================================================================
# STEP 4: Filter stocks where close > 200 SMA on month-end dates
# ============================================================================
print(f"\n🔍 Filtering stocks where Close > {SMA_WINDOW}-day SMA on month-ends...")

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

    # List to store stocks that also pass SMA filter
    sma_passing_stocks = []

    for stock_name in volume_stocks_list:
        # Check if we have data for this stock
        if stock_name in stock_data:
            df = stock_data[stock_name]

            # Find the closest date in the stock's index (month-end might not be exact due to time component)
            # Look for dates on the same day as month_end_date
            matching_dates = df.index[df.index.date == month_end_date.date()]

            if len(matching_dates) > 0:
                actual_date = matching_dates[0]  # Use the first match (should only be one per day)
                close_price = df.loc[actual_date, 'close']
                sma_200 = df.loc[actual_date, 'sma_200']

                # Check if close > SMA (and SMA is not NaN)
                if pd.notna(sma_200) and close_price > sma_200:
                    sma_passing_stocks.append(stock_name)

    # Add to results
    results.append({
        'date': month_end_date.strftime('%Y-%m-%d'),
        'stocks': ', '.join(sorted(sma_passing_stocks)),
        'count': len(sma_passing_stocks)
    })

print(f"✓ Processed {len(volume_cut_filtered)} month-end dates")

# ============================================================================
# STEP 5: Generate 200sma_cut.csv
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
print(f"📊 Filter: Close > {SMA_WINDOW}-day SMA")
print(f"📊 Applied after: Volume > 10 lakh filter")

# Show statistics
print("\n📈 Stock count statistics per month-end:")
print(f"   Average stocks per month: {result_df['count'].mean():.1f}")
print(f"   Min stocks in a month: {result_df['count'].min()}")
print(f"   Max stocks in a month: {result_df['count'].max()}")

# Show sample
print("\n📋 Sample data (first 10 month-ends):")
print(result_df[['date', 'count']].head(10).to_string(index=False))

# Show comparison with volume cut
print("\n📊 Comparison: Volume Cut vs SMA Cut")
volume_cut_comparison = volume_cut_filtered[['date', 'count']].copy()
volume_cut_comparison['date'] = volume_cut_comparison['date'].dt.strftime('%Y-%m-%d')
result_comparison = result_df[['date', 'count']].copy()

comparison_df = pd.merge(
    volume_cut_comparison.rename(columns={'count': 'volume_count'}),
    result_comparison.rename(columns={'count': 'sma_count'}),
    on='date'
)
comparison_df['reduction_%'] = ((comparison_df['volume_count'] - comparison_df['sma_count']) / comparison_df['volume_count'] * 100).round(1)

print(f"\n   Average reduction: {comparison_df['reduction_%'].mean():.1f}%")
print(f"   Avg stocks after volume filter: {comparison_df['volume_count'].mean():.1f}")
print(f"   Avg stocks after SMA filter: {comparison_df['sma_count'].mean():.1f}")

print("\n" + "="*70)
print(f"✓ DONE! Check {OUTPUT_FILE} for full results")
print("="*70)
