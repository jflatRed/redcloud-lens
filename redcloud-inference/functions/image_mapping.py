import pandas as pd


def map_images(mapping_df, img_df, product_df):

    # create a table that stores the image image_mapping
    # sku, image_url

    # search for a product by gtin/ by text

    # -array of skus
    # search for product image by sku
    # for loop- 1st result with a non-null value, we escape the loop
    # we assign the image to every item in that group

    # for loop- 1st result with a non-null value, we escape the loop
    # we assign the image to every item in that group

    # pepsi_463454


if __name__ == "__main__":
    mapping_df = pd.read_csv("mapping.csv")
    img_df = pd.read_csv("image.csv")
    product_df = pd.read_csv("product.csv")
    map_images(mapping_df, img_df, product_df)
