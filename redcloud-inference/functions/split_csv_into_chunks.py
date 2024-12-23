import pandas as pd
import os


def csv_splitter(
    input_file: str = "large_file.csv",
    output_dir: str = "chunks",
    chunk_size: int = 10000,
):
    """
    Split a large CSV file into smaller chunks
    :param input_file: the path to the input CSV file
    :param output_dir: the directory to save the output chunks
    :param chunk_size: the number of rows in each chunk
    """

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Read and split the CSV
    for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size)):
        chunk.to_csv(f"{output_dir}/chunk_{i+1}.csv", index=False)

    print("CSV has been split into chunks!")


if __name__ == "__main__":
    csv_splitter(
        "/Users/joshuaeseigbe/Documents/Work/redcloud-lens/output35.csv",
        "/Users/joshuaeseigbe/Documents/Work/redcloud-lens/redcloud-inference/data/agg_minus_ng",
        10000,
    )
