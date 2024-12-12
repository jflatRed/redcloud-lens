import os
import pandas as pd
from openai import OpenAI

client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
)


def get_gtin_from_openai(product_name):
    """
    Queries OpenAI API to get a GTIN for the given product name.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o",  # Use the desired OpenAI model
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant that provides GTINs for products.Search any internal db or sources you have. only return the gtin number and return '' if not available",
                },
                {
                    "role": "user",
                    "content": f"What is the GTIN for the product '{product_name}'?",
                },
            ],
            max_tokens=50,
        )
        print(response)
        gtin = response.choices[0].message.content.strip()
        return gtin
    except Exception as e:
        print(f"Error fetching GTIN for {product_name}: {e}")
        return None


if __name__ == "__main__":
    # Read the CSV file
    input_csv = "/Users/joshuaeseigbe/Downloads/2024-12-05 1_56pm.csv"  # Replace with your input file path
    output_csv = "output.csv"  # Replace with your desired output file path

    # Load the CSV into a DataFrame
    df = pd.read_csv(input_csv)

    # Check if the 'Product Name' column exists
    if "Product Name" not in df.columns:
        raise ValueError("The 'Product Name' column is missing in the input CSV.")

    # Add a new column for GTIN by querying the OpenAI API
    df["GTIN"] = df["Product Name"].apply(get_gtin_from_openai)

    # Save the updated DataFrame to a new CSV file
    df.to_csv(output_csv, index=False)

    print(f"Updated CSV with GTIN column saved to {output_csv}.")
