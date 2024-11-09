####
## Magpie Technical Test Answer - Data Engineer
## by Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import pandas as pd
import json
import logging
import re

logging.basicConfig(level=logging.INFO)


# A function to safely parse the _meta column and handle missing keys (email)
def parse_meta(value):
    try:
        # Parse the value using JSON
        return json.loads(value)
    except json.JSONDecodeError:
        logging.info("--- !!Error!! JSON decoding failed, attempting to extract email and reconstruct JSON.")

        # Variables
        parsed_value = {}
        combined_data = {}

        # Extract email if it's present without a key
        email_match = re.search(r"[^@\s]+@[^@\s]+\.[a-zA-Z0-9]+", value) # Generated from ChatGPT to search email that exist in text without key `email`
        
        if email_match:
            # Extract the email and add it to parsed_value
            parsed_value['email'] = email_match.group(0)

            # Remove the email from the string to clean it up for JSON parsing
            cleaned_value = value.replace(email_match.group(0), '', 1)

            # Remove misplaced `",` at the start of the string
            cleaned_value = re.sub(r'",\s*', '', cleaned_value, 1)

            # Generated from ChatGPT - Remove trailing commas that appear before a closing brace/bracket or at the end of the string
            cleaned_value = re.sub(r",\s*(?=[}\]])|,\s*$", "", cleaned_value)

            try:
                # JSON parsing on the cleaned string
                json_cleaned = json.loads(cleaned_value)

                # Combine cleaned JSON with the parsed JSON
                combined_data = {**json_cleaned, **parsed_value}
            except json.JSONDecodeError:
                logging.info("--- !!Error!! JSON decoding failed after cleaning up. Returning dictionary with email only.")

        else:
            logging.info("--- !!Error!! No email found and JSON decoding failed entirely.")
            combined_data = {}

        return combined_data

# Tech 2; Part 1 Function - Top Contributor
def top_contributor_finder(data, working_path:str):
    try:
        logging.info("--- Checking required column...")
        if '_meta' not in data.columns:
            raise ValueError("--- !!Error!! '_meta' column is missing from the dataset.")
        elif data['_meta'].isnull().all():
            raise ValueError("--- !!Error!! '_meta' column is entirely null.")

        # Parse '_meta' column
        logging.info("--- Begin Parsing...")
        data['_meta'] = data['_meta'].apply(parse_meta)

        # Extract 'email' and 'shop_id'
        logging.info("--- Extracting required columns...")
        data['email'] = data['_meta'].apply(lambda x: x.get('email', None))

        # Checking extracted columns
        logging.info("--- Checking extraction results...")
        if data['email'].isnull().all():
            raise ValueError("--- !!Error!! 'email' field is missing in '_meta' data. This field is required for analysis.")
        elif data['merchant_id'].isnull().all():
            raise ValueError("--- !!Error!! 'shop_id' field is missing in '_meta' data. This field is required for analysis.")

        # Analysis for top contributor
        logging.info("--- Analyzing...")
        top_product_contributor = data['email'].value_counts().idxmax()
        top_distinct_shop_contributor = data.groupby('email')['merchant_id'].nunique().idxmax()

        # Generate output DataFrame
        output = pd.DataFrame({
            'qualification': ['product', 'distinct_shop'],
            'top_contributor': [top_product_contributor, top_distinct_shop_contributor]
        })
        print(output)

        # Save the output to CSV
        file_output = 'result_1'
        output.to_csv(f'{working_path}/{file_output}.csv', index=False)
        logging.info(f"Output saved to {working_path}/{file_output}.csv")

    except Exception as e:
        logging.error(f"--- Unexpected error during top contributor analysis: {e}")


# Tech 2; Part 2 Function - Top Sold Products
def top_sold_products(data, working_path: str):
    try:
        logging.info("--- Checking required columns...")

        # Verify required columns
        if 'total_volume_sold' not in data.columns or 'created_at' not in data.columns or 'product_id' not in data.columns:
            raise ValueError("--- !!Error!! Required columns ('total_volume_sold', 'created_at', 'product_id') are missing.")

        # Convert created_at to date
        data['created_at'] = pd.to_datetime(data['created_at'], errors='coerce')

        # Filter data between 10-01 and 10-07
        start_date = "2024-10-01"
        end_date = "2024-10-07"
        filtered_data = data[(data['created_at'] >= start_date) & (data['created_at'] <= end_date)]

        # Sum up the total volume sold per product within the date range
        product_sales = (
            filtered_data.groupby('product_id')['total_volume_sold']
            .sum()
            .reset_index()
            .sort_values(by='total_volume_sold', ascending=False)
        )

        # Top 10 Products
        top_10_products = product_sales.head(10)
        print(top_10_products)

        output_file = "result_2"
        top_10_products.to_csv(f"{working_path}/{output_file}.csv", index=False)
        logging.info(f"Top 10 sold products saved to {working_path}/{output_file}.csv")

    except Exception as e:
        logging.error(f"--- Unexpected error during top sold products analysis: {e}")


# Main
if __name__ == "__main__":
    working_path = "C:/Users/Mario C/Desktop/Magpie/technical/tech_2"
    raw_file_name = "results-20241107-171708.csv"

    try:
        data = pd.read_csv(f"{working_path}/{raw_file_name}")
        logging.info("--- CSV file loaded successfully")
        
        # Tech 2; Part 1
        # top_contributor_finder(data, working_path)

        # Tech 2; Part 2
        top_sold_products(data, working_path)

    except Exception as e:
        logging.error(f"--- Unexpected error: {e}")
