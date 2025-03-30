import requests
import json
from datetime import datetime
from pathlib import Path
from google.cloud import storage

def fetch_products() -> list[dict]:
    """
    Fetches a list of products from the FakeStore API.
    Returns a list of dictionaries representing the products.
    """
    url = "https://fakestoreapi.com/products"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching products: {e}")
        return []

def save_to_file(data: list[dict], filename: str) -> None:
    """
    Saves JSON data to a file.
    """
    file_path = Path(filename)
    if file_path.exists():
        print(f"Warning: Overwriting existing file {filename}")
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def convert_json_to_ndjson(input_file: str, output_file: str) -> None:
    """
    Converts a JSON file to NDJSON format.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f) 

        if not isinstance(data, list):
            raise ValueError("The JSON file must contain an array of objects.")

        with open(output_file, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")

        print(f"Conversion successful: {output_file} is ready.")

    except json.JSONDecodeError as e:
        print(f"JSON parsing error : {e}")
    except Exception as e:
        print(f"Error : {e}")

def upload_to_gcs(bucket_name: str, source_file_name: str, destination_blob_name: str) -> None:
    """
    Uploads a file to Google Cloud Storage bucket.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {bucket_name}/{destination_blob_name}")

def main() -> None:
    try:
        products = fetch_products()
        filename = "products.json"
        covertedfilename = "products.ndjson"
        time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        bucket_name = "fake-products-bucket"
        destination_blob_name = f"products/products_{time}.json"
        
        save_to_file(products, filename)
        convert_json_to_ndjson(filename, covertedfilename)
        upload_to_gcs(bucket_name, covertedfilename, destination_blob_name)
        print("Process completed successfully!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
