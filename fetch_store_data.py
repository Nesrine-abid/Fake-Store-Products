import requests
import json
from google.cloud import storage

def fetch_products():
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def save_to_file(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {bucket_name}/{destination_blob_name}")

def main():
    try:
        products = fetch_products()
        filename = "products.json"
        save_to_file(products, filename)
        
        bucket_name = "fake-products-bucket"
        destination_blob_name = "products/products.json"
        upload_to_gcs(bucket_name, filename, destination_blob_name)
        print("Process completed successfully!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
