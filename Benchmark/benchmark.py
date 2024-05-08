import pandas as pd
import time
import os
import matplotlib.pyplot as plt


def process_and_benchmark_dataframe(csv_file, num_iterations=10, sep=';'):
    # Read CSV file into DataFrame
    print("=============================================")
    print(f"\nReading CSV file: {csv_file}")
    start_time = time.time()
    df = pd.read_csv(csv_file, low_memory=False, encoding='latin1', sep=sep)
    read_time = time.time() - start_time
    print(f"Time to read CSV: {read_time:.6f} seconds")

    # Save DataFrame to different formats
    formats = ['csv', 'pickle', 'feather', 'parquet']
    save_times = {fmt: [] for fmt in formats}
    load_times = {fmt: [] for fmt in formats}

    for _ in range(num_iterations):
        print(f"Iteration {_ + 1} of {num_iterations}")
        for fmt in formats:
            file_path = f"data.{fmt}"

            # Save DataFrame
            start_time = time.time()
            if fmt == 'csv':
                df.to_csv(file_path, index=False)
            elif fmt == 'excel':
                df.to_excel(file_path, index=False)
            elif fmt == 'pickle':
                df.to_pickle(file_path)
            elif fmt == 'feather':
                df.to_feather(file_path)
            elif fmt == 'parquet':
                df.to_parquet(file_path)
            save_times[fmt].append(time.time() - start_time)

            # Load DataFrame
            start_time = time.time()
            if fmt == 'csv':
                pd.read_csv(file_path)
            elif fmt == 'excel':
                pd.read_excel(file_path)
            elif fmt == 'pickle':
                pd.read_pickle(file_path)
            elif fmt == 'feather':
                pd.read_feather(file_path)
            elif fmt == 'parquet':
                pd.read_parquet(file_path)
            load_times[fmt].append(time.time() - start_time)

            # Clean up
            os.remove(file_path)

    # Print average times
    print("\nAverage save times:")
    for fmt, times in save_times.items():
        avg_time = sum(times) / num_iterations
        print(f"{fmt.upper()}: {avg_time:.6f} seconds")

    print("\nAverage load times:")
    for fmt, times in load_times.items():
        avg_time = sum(times) / num_iterations
        print(f"{fmt.upper()}: {avg_time:.6f} seconds")

    # Plot times
    import numpy as np

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 10))

    # Calculate average save times and plot
    avg_save_times = {fmt: np.mean(times) for fmt, times in save_times.items()}
    ax1.bar(avg_save_times.keys(), avg_save_times.values())
    ax1.set_title("Average Save Times")
    ax1.set_xlabel("Format")
    ax1.set_ylabel("Time (seconds)")

    # Calculate average load times and plot
    avg_load_times = {fmt: np.mean(times) for fmt, times in load_times.items()}
    ax2.bar(avg_load_times.keys(), avg_load_times.values())
    ax2.set_title("Average Load Times")
    ax2.set_xlabel("Format")
    ax2.set_ylabel("Time (seconds)")

    plt.savefig(
        f'benchmark_results/benchmark_{csv_file.replace(r"/","_")}.pdf')


newPath = "./benchmark_results"
if not os.path.exists(newPath):
    os.makedirs(newPath)

process_and_benchmark_dataframe('copa/INV_LOC.csv', num_iterations=10)
process_and_benchmark_dataframe('copa/FAIL_MODE.csv', num_iterations=10)
process_and_benchmark_dataframe(
    'copa/EVT_EVENT.csv', num_iterations=10, sep=',')
process_and_benchmark_dataframe(
    'copa/REQ_PART.csv', num_iterations=10, sep=',')
