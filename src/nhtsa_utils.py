"""
Listed below are functions used by the NHTSA assets/functions found in assets.py
"""
import io
import json
import pandas as pd
import requests


def fetch_manufacturers(page: int) -> dict:
    """
    Fetch vehicle manufacturer information from NHTSA's vPIC api.
    """

    url = f'https://vpic.nhtsa.dot.gov/api/vehicles/getallmanufacturers?ManufacturerType=&format=json&page={str(page)}'

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()  # raise an error if status code is not ok
    except (requests.exceptions.RequestException, requests.exceptions.SSLError) as e:
        print(f"Error occurred while fetching page {page}: {str(e)}")

    json_dict = json.loads(response.text)

    return json_dict


def fetch_model_names(make_id: int, model_year: int, vehicle_type: str) -> requests.models.Response:
    """
    Fetch model name information from NHTSA's vPIC api

    Parameters
    ----------
    make_id, model_year, vehicle_type

    Returns
    -------
    requests object
    """

    url = f'https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeIdYear/makeId/{make_id}/modelyear/{model_year}/vehicletype/{vehicle_type}?format=csv'
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")

    return response


def fetch_wmi_by_manufacturer(mfr_id: int) -> pd.DataFrame:
    """
    Fetch WMI by manufacturer ID using NHTSA's vPIC api.

    Parameters
    ----------
    mfr_id

    Returns
    -------
    pandas dataframe

    """
    url = f'https://vpic.nhtsa.dot.gov/api/vehicles/GetWMIsForManufacturer/{str(mfr_id)}?format=csv'
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        return None
    except requests.exceptions.ReadTimeout:
        print("Read timeout error occurred, retrying with longer timeout...")
        response = requests.get(url, timeout=60)

    csv_file = io.StringIO(response.content.decode('utf-8'))
    df = pd.read_csv(csv_file)
    # Some WMI codes can "look" like int types and so we want to make sure they are explicitly defined as str
    # when concatenating the dataframes together.  Otherwise, will get a pyarrow error due to int/str confusion.
    # Relevant background: https://github.com/wesm/feather/issues/349
    df['wmi'] = df['wmi'].astype('str')
    return df


def fetch_wmi_data(wmi) -> pd.DataFrame:
    """
    Fetches WMI information for a single WMI code from NHTSA's vPIC api.

    Parameters
    ----------
    wmi

    Returns
    -------
    pandas dataframe
    """

    url = f'https://vpic.nhtsa.dot.gov/api/vehicles/decodewmi/{wmi}?format=json'
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()  # Raise an exception for 4xx and 5xx HTTP status codes
    except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
        print(f'Error fetching data for WMI {wmi}: {e}')
        return pd.DataFrame()
    except requests.exceptions.ReadTimeout:
        print("Read timeout error occurred, retrying with longer timeout...")
        res = requests.get(url, timeout=60)

    try:
        df = pd.json_normalize(json.loads(response.text), record_path=['Results'])
        df = df.assign(WMI=wmi)
    except json.JSONDecodeError as e:
        print(f'Error decoding JSON data for WMI {wmi}: {e}')
        return pd.DataFrame()

    return df
