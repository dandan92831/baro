import pandas as pd
import os
import re


def clean_pod_name(pod_name):
    return re.sub(r'[^a-zA-Z0-9]', '', pod_name.split('-')[1].lower())


def filter_columns_in_folder(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    for file_name in os.listdir(input_folder):
        input_file = os.path.join(input_folder, file_name)

        if os.path.isfile(input_file) and file_name.endswith('.csv'):
            df = pd.read_csv(input_file)
            selected_columns = [
                "TimeStamp",
                "PodName",
                "CpuUsage(m)",
                "MemoryUsage(Mi)",
                "PodServerLatencyP90(s)",
                "PodServerLatencyP95(s)"
            ]

            if all(col in df.columns for col in selected_columns):
                filtered_df = df[selected_columns]

                filtered_df = filtered_df.rename(columns={
                    "CpuUsage(m)": "cpu",
                    "MemoryUsage(Mi)": "mem",
                    "PodServerLatencyP90(s)": "latency-90",
                    "PodServerLatencyP95(s)": "latency-95"
                })

                output_file = os.path.join(output_folder, file_name)
                filtered_df.to_csv(output_file, index=False)
                print(f"processed {file_name}")



def merge_all_csv_in_folder(input_folder, output_file):
    csv_files = [os.path.join(input_folder, file) for file in os.listdir(input_folder) if file.endswith('.csv')]

    merged_df = pd.DataFrame()

    for file in csv_files:
        df = pd.read_csv(file)

        if "TimeStamp" in df.columns:
            df["TimeStamp"] = df["TimeStamp"].astype(str).str.replace("\.0", "", regex=False)
        if "PodName" in df.columns:
            pod_name_prefix = clean_pod_name(df["PodName"].iloc[0]) + "_"
            df = df.rename(columns={
                "cpu": pod_name_prefix + "cpu",
                "mem": pod_name_prefix + "mem",
                "latency-90": pod_name_prefix + "latency-90",
                "latency-95": pod_name_prefix + "latency-95",
                "error": pod_name_prefix + "error"
            })

        merged_df = pd.concat([merged_df, df], axis=1) if not merged_df.empty else df

    merged_df.to_csv(output_file, index=False)
    print(f"all files merged {output_file}")


def add_column_in_folder(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            input_file_path = os.path.join(input_folder, filename)
            output_file_path = os.path.join(output_folder, filename)

            df = pd.read_csv(input_file_path)
            df['error'] = 0

            df.to_csv(output_file_path, index=False)

            print(f"Updated {filename} has been saved to {output_folder}")


def process_csv(input_csv):
    df = pd.read_csv(input_csv)

    df = df.head(80)
    if 'TimeStamp' in df.columns:
        df['TimeStamp'] = df['TimeStamp'].astype(int)

    columns_to_drop = [col for col in df.columns if 'TimeStamp' in col and col != 'TimeStamp']

    columns_to_drop += [col for col in df.columns if 'PodName' in col]
    df.drop(columns=columns_to_drop, inplace=True)

    df.to_csv(input_csv, index=False)

    print(f"Processed data has been saved to {input_csv}")

##input_example
input_folder = "output"
output_folder = "output_baro"
output_merged_file = "output_baro/final_merged_output.csv"

filter_columns_in_folder(input_folder, output_folder)


add_column_in_folder(output_folder, output_folder)


merge_all_csv_in_folder(output_folder, output_merged_file)

process_csv(output_merged_file)
