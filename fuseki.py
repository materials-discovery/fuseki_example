import requests
import httpx
from SPARQLWrapper import SPARQLWrapper, JSON

"""
Scripts for creating, managing, and retrieving data from Fuseki DB

 """


def retrieve_data_from_fuseki(fuseki_url, dataset_name):
    """
    Retrieve data from a dataset on a Fuseki server using a GET request.

    Args:
        fuseki_url (str): The URL of the Fuseki server.
        dataset_name (str): The name of the dataset to retrieve data from.

    Returns:
        str: The data retrieved from the dataset, or an error message.
    """
    try:
        # Construct the dataset-specific URL for querying
        query_url = f"{fuseki_url}/{dataset_name}/data"

        # Send a GET request to retrieve the data
        with httpx.Client() as client:
            response = client.get(query_url)

        # Check the response status
        if response.status_code == 200:
            # The response content contains the retrieved data
            data = response.text
            return data
        else:
            return f"Failed to retrieve data. Status code: {response.status_code}\n{response.text}"
    except Exception as e:
        return f"An error occurred: {e}"


def upload_to_dataset(fuseki_url, dataset_name, ttl_file_path):
    """
    Upload a .ttl file to an existing dataset in Fuseki.

    Args:
        fuseki_url (str): The base URL of the Fuseki server.
        dataset_name (str): The name of the existing dataset.
        ttl_file_path (str): The path to the .ttl file to be uploaded.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    try:
        # Read the .ttl file content
        ttl_data = open(ttl_file_path).read()

        # Define the URL for uploading to the dataset
        upload_url = f"{fuseki_url}/{dataset_name}/data?default"

        # Set the headers for the request
        headers = {'Content-Type': 'text/turtle;charset=utf-8'}

        # Send a POST request to upload the data
        response = requests.post(upload_url, data=ttl_data, headers=headers)

        if response.status_code == 200:
            print(f"Data uploaded to dataset '{dataset_name}' successfully.")
            return True
        else:
            print(f"Failed to upload data to dataset '{dataset_name}'. Status code: {response.status_code}")
            print(response.text)
            return False

    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def create_fuseki_dataset(fuseki_url, dataset_name):
    """
    Create a new TDB dataset in a Fuseki server.

    Args:
        fuseki_url (str): The base URL of the Fuseki server (e.g., http://localhost:3030).
        dataset_name (str): The name of the dataset to be created.

    Returns:
        bool: True if the dataset creation was successful, False otherwise.
    """
    # Define the Fuseki API endpoint for creating datasets
    api_endpoint = f"{fuseki_url}/$/datasets"

    # Define the parameters for creating a TDB dataset
    params = {
        "dbName": dataset_name,
        "dbType": "tdb",
    }

    try:
        # Send an HTTP POST request to create the dataset
        response = requests.post(api_endpoint, data=params)

        if response.status_code == 200:
            print(f"Dataset '{dataset_name}' created successfully.")
            return True
        else:
            print(f"Failed to create dataset '{dataset_name}'. Status code: {response.status_code}")
            print(response.text)
            return False

    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def update_dataset(fuseki_url, dataset_name, ttl_file_path):
    """
    Update and replace the existing data in a dataset in Fuseki with new data from a .ttl file.

    Args:
        fuseki_url (str): The base URL of the Fuseki server.
        dataset_name (str): The name of the dataset to be updated.
        ttl_file_path (str): The path to the .ttl file containing the new data.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        # Read the .ttl file content
        ttl_data = open(ttl_file_path).read()

        # Define the URL for updating the dataset (replacing ontology_manager content with new data)
        update_url = f"{fuseki_url}/{dataset_name}/data?default"

        # Set the headers for the request
        headers = {'Content-Type': 'text/turtle;charset=utf-8'}

        # Send a PUT request to update the data (replace ontology_manager content with new data)
        response = requests.put(update_url, data=ttl_data, headers=headers)

        if response.status_code == 200:
            print(f"Dataset '{dataset_name}' updated and replaced successfully.")
            return True
        else:
            print(f"Failed to update and replace dataset '{dataset_name}'. Status code: {response.status_code}")
            print(response.text)
            return False

    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def sparql_query(ontology_iri, variable_type, relationship, desired_variable):
    query = f"""
        SELECT ?{desired_variable} WHERE {{
            ?variable <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{ontology_iri}{variable_type}> .
            ?variable <{ontology_iri}{relationship}> ?value .
            ?value <{ontology_iri}quantity> ?{desired_variable} .
        }}
    """

    return query

def query_data_from_fuseki(fuseki_url, query):
    """
    Query data from a Fuseki server using SPARQL.

    Args:
        fuseki_url (str): The URL of the Fuseki server's SPARQL endpoint.
        query (str): The SPARQL query to be executed.

    Returns:
        list of dict: A list of dictionaries where each dictionary represents a row of query results.
    """
    try:
        # Initialize a SPARQLWrapper instance with the Fuseki server's SPARQL endpoint
        sparql = SPARQLWrapper(fuseki_url)

        # Set the query and request format (JSON)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        # Execute the query and parse the results
        results = sparql.query().convert()

        # Process the results and convert them into a list of dictionaries
        rows = []
        for result in results["results"]["bindings"]:
            row = {}
            for key, value in result.items():
                row[key] = value["value"]
            rows.append(row)

        return rows

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
