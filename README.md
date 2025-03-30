# Fake-Store-Products

# 1. Data Ingestion: Fetching and Storing Product Data 
The fetch_store_data.py script retrieves product data from the Fake Store API and stores it in a Google Cloud Storage (GCS) bucket.

# 2. Data Transformation and Modeling: Creating a Star Schema 
The data model follows a star schema, consisting of:

* 3 fact tables (defined in facts.sql)

* 5 dimension tables (defined in dims.sql)

# 3. Analytical Tasks: Answering Advanced Business Questions 
Each analytical question is addressed by an individual SQL query, stored in a file with the corresponding name.