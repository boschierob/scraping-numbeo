import pandas as pd
import glob
import os
import sys

def concatenate_city_csvs(session_folder, output_filename="ALL_DATA_CONCATENATED.csv"):
    """
    Concatenate all CSV files in a session/city folder (and subfolders) into a single CSV,
    with a unified schema (all columns present in any file, in consistent order).
    """
    # Find all CSV files recursively
    csv_files = glob.glob(os.path.join(session_folder, '**', '*.csv'), recursive=True)
    if not csv_files:
        print(f"No CSV files found in {session_folder}")
        return

    print(f"Found {len(csv_files)} CSV files. Reading and merging...")

    # Read all CSVs, collect all columns
    dfs = []
    all_columns = set()
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            df['source_file'] = os.path.basename(file)  # Optional: track origin
            dfs.append(df)
            all_columns.update(df.columns)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    # Ensure consistent column order (sorted for reproducibility)
    all_columns = sorted(all_columns)
    print(f"Unified schema columns: {all_columns}")

    # Reindex all DataFrames to have the same columns (missing columns filled with NaN)
    dfs = [df.reindex(columns=all_columns) for df in dfs]

    # Concatenate all DataFrames
    merged_df = pd.concat(dfs, ignore_index=True)

    # Save to output file
    output_path = os.path.join(session_folder, output_filename)
    merged_df.to_csv(output_path, index=False)
    print(f"✅ All CSVs merged and saved to: {output_path}")

# Example usage:
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python concatenate_city_csvs.py <session_folder>")
        sys.exit(1)
    session_folder = sys.argv[1]
    concatenate_city_csvs(session_folder) 