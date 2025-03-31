import json
import os
import pandas as pd
from openai import OpenAI
from eansearch import EANSearch
import uuid

from urllib.parse import quote

OPEN_AI_API_KEY = os.environ.get("OPENAI_API_KEY")
EAN_SEARCH_KEY = os.environ.get("EAN_API_KEY")

openai_client = OpenAI(
    api_key=OPEN_AI_API_KEY,  # This is the default and can be omitted
)


def get_gtin_from_openai(product_name):
    """
    Queries OpenAI API to get a GTIN for the given product name.
    """
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",  # Use the desired OpenAI model
            messages=[
                {
                    "role": "system",
                    "content": """
                    You are a helpful assistant that provides GTINs for products.Search any internal db or sources you have.
                    Return the ' + gtin number only if gtin number is available and return '' if not available""",
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


def generate_ean_from_product_name(product_name: str, country_name: str = None, country_filter: str = None, country_code: str = "NG"):
    """
    Sends query to EAN service to retrieve gtin
    """
    if country_filter and country_name != country_filter:
        return None
    try:
        eansearch = EANSearch(EAN_SEARCH_KEY)
        print("got here")
        ean_list = eansearch.productSearch(quote(product_name))

        for product in ean_list:
            if product["issuingCountry"] == country_code:
                return product["ean"]

        if len(ean_list):
            return ean_list[0]["ean"]
        return None
    except Exception as e:
        print(f"Error fetching GTIN for {product_name}: {str(e)}")
        return None


def generate_unique_uuid(generated_ids: set, length: int = 13):
    while True:
        # Generate a 13-character identifier
        new_id = str(uuid.uuid4()).replace("-", "")[:length]
        # Ensure uniqueness
        if new_id not in generated_ids:
            generated_ids.add(new_id)
            return new_id


def clean_sku_string(sku_value: str):
    items = json.loads(sku_value)
    # Convert the list to a comma-separated string
    return ",".join(items)


def generate_mapping(ean, gtin, uid):
    if ean:
        print("EAN", ean)
        return "'" + ean, "EAN"
    if gtin:
        print("GTIN", gtin)
        return "'" + gtin, "GTIN"
    if uid:
        print("UID", uid)
        return "'" + uid, "UUID"


if __name__ == "__main__":
    # A set to store previously generated identifiers
    generated_ids = set()
    search_fields = ["Product Name", "SKU_STRING", "Country"]
    # Read the CSV file
    paths = [
        "/Users/joshuaeseigbe/Downloads/product_export_except_nigeria (1).csv"
    ]
    for index, input_path in enumerate(paths):
        output_path = f"{input_path.split('.')[0]}_revised.csv"
        # Load the CSV into a DataFrame
        df = pd.read_csv(input_path)

        # Check if the 'Product Name' column exists
        if "Product Name" not in df.columns:
            raise ValueError(
                "The 'Product Name' column is missing in the input CSV.")

        # Add a new column for GTIN by querying the OpenAI API
        # df["GTIN"] = df[search_fields[0]].apply(get_gtin_from_openai)
        # df["EAN"] = df[search_fields[0], search_fields[2]].apply(
        #     lambda x: generate_ean_from_product_name(x[0], x[1], "South Africa", "ZA"))
        df["EAN"] = None
        df["GTIN"] = None
        df["UID"] = df[search_fields[0]].apply(
            lambda x: generate_unique_uuid(generated_ids, 13))
        # df["SKU_STRING"] = df[search_fields[1]].apply(clean_sku_string) : add this back
        df[["Mapping", "Mapping Type"]] = df.apply(
            lambda row: pd.Series(
                generate_mapping(row["EAN"], row["GTIN"], row["UID"])),
            axis=1)
        df['Metadata'] = None
        df['ImageUrl'] = None

        # Save the updated DataFrame to a new CSV file
        df.to_csv(output_path, index=False)

        print(f"Updated CSV with GTIN column saved to {output_path}.")
