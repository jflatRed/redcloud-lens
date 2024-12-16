import json
import os
import pandas as pd
from openai import OpenAI
from eansearch import EANSearch
import uuid

OPEN_AI_API_KEY = os.environ.get("OPENAI_API_KEY")
EAN_SEARCH_KEY = os.environ.get("EAN_API_KEY")
print(EAN_SEARCH_KEY)
client = OpenAI(
    api_key=OPEN_AI_API_KEY,  # This is the default and can be omitted
)


class EANOBJECT:
    ean: str
    name: str
    categoryId: str
    categoryName: str
    issuingCountry: str


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


def generate_gtin_from_ean(product_name: str):
    """
    Sends query to EAN service to retrieve gtin
    """
    try:
        eansearch = EANSearch(EAN_SEARCH_KEY)
        print("got here")
        eanList = eansearch.productSearch(product_name.replace(" ", "%20"))

        for product in eanList:
            if product["issuingCountry"] == "NG":
                return product["ean"]

        if len(eanList):
            return eanList[0]["ean"]
        return "null"
    except Exception as e:
        print(f"Error fetching GTIN for {product_name}: {str(e)}")
        return None


def clean_up(input_path, output_path):
    # Function to generate UUID if both fields are missing
    generated_ids = set()

    def clean_sku_string(sku_valuel: str):
        items = json.loads(sku_valuel)
        # Convert the list to a comma-separated string
        return ",".join(items)

    # A set to store previously generated identifiers

    def generate_unique_uuid():
        while True:
            # Generate a 13-character identifier
            new_id = str(uuid.uuid4()).replace("-", "")[:13]
            # Ensure uniqueness
            if new_id not in generated_ids:
                generated_ids.add(new_id)
                return new_id

    def generate_new_field(row):
        if pd.isna(row["EAN"]) or row["EAN"] == "null" or row["EAN"] == "missing":
            if pd.isna(row["GTIN"]) or row["GTIN"] == "null":
                # Both fields are missing, generate UUID
                return "'" + str(generate_unique_uuid()), "UUID"
            else:
                # Field2 is present
                return row["GTIN"], "GTIN"
        else:
            # Field1 is present
            return row["EAN"], "EAN"

    df = pd.read_csv(input_path)
    df[["Mapping", "Mapping Type"]] = df.apply(
        lambda row: pd.Series(generate_new_field(row)), axis=1
    )
    df["SKU_STRING"] = df["SKU_STRING"].apply(clean_sku_string)
    # Save the updated DataFrame to a new CSV file
    df.to_csv(output_path, index=False)

    print(f"Updated CSV with GTIN column saved to {output_path}.")


if __name__ == "__main__":
    clean_up(
        "/Users/joshuaeseigbe/Downloads/GH-4 copy.csv",
        "/Users/joshuaeseigbe/Downloads/hjk1.csv",
    )

# if __name__ == "__main__":
#     # Read the CSV file
#     input_csv = (
#         "/Users/joshuaeseigbe/Downloads/x.csv"  # Replace with your input file path
#     )
#     output_csv = "output34.csv"  # Replace with your desired output file path

#     # Load the CSV into a DataFrame
#     df = pd.read_csv(input_csv)

#     # Check if the 'Product Name' column exists
#     if "Product Name" not in df.columns:
#         raise ValueError("The 'Product Name' column is missing in the input CSV.")

#     # Add a new column for GTIN by querying the OpenAI API
#     # df["GTIN"] = df["Product Name"].apply(get_gtin_from_openai)
#     try:

#         df["GTIN_2"] = df["Product Name"].apply(generate_gtin_from_ean)
#     except:
#         print("Error Occurred")

#     # Save the updated DataFrame to a new CSV file
#     df.to_csv(output_csv, index=False)

#     print(f"Updated CSV with GTIN column saved to {output_csv}.")
