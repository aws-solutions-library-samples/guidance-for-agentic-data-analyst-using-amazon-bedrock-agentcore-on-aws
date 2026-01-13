# See: https://developer.ons.gov.uk/
import json
import copy
import time
from os import path, makedirs

import requests
from tqdm import tqdm

from aws_data_analyst import DATASETS_DIR

ROOT_URL = "https://api.beta.ons.gov.uk/v1/"
EIGTH_KB = 8 * 1024


# Point fixes for datasets with malformed files. 
MALFORMED_DATA = {
    'https://download.ons.gov.uk/downloads/datasets/RM012/editions/2021/versions/2.csv': 'https://static.ons.gov.uk/datasets/RM012-2021-1.csv',
    'https://download.ons.gov.uk/downloads/datasets/RM076/editions/2021/versions/3.csv': 'https://static.ons.gov.uk/datasets/RM076-2021-2.csv',
}


def request(url, params=None, stream=None, json=False):
    while True:
        r = requests.get(url, params=params, stream=stream, timeout=30)

        if r.status_code == 429:
            retry_after = r.headers.get('Retry-After', 10)
            print(f"Too many requests. Waiting for {retry_after} seconds.")
            time.sleep(float(retry_after))
            continue
        
        r.raise_for_status()
        return r.json() if json else r


def download_file(url, destination):
    print(f"Downloading: {url}")
    response = request(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    with open(destination, 'wb') as f, tqdm(
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
    ) as pbar:
        for chunk in response.iter_content(chunk_size=EIGTH_KB):
            if not chunk: # Filter out keep-alive chunks
                continue
            f.write(chunk)
            pbar.update(len(chunk))


def ons_call(api, params=None):
    return request(ROOT_URL + api, params=params, json=True)


def ons_list_datasets():
    metadata = ons_call("datasets", {'limit': 0})
    num = metadata['total_count']
    data = ons_call("datasets", {'limit': num})
    return data['items']


def get_dataset_base_and_version(url):
    parts = url.split('/')
    base = '/'.join(parts[:-1])
    version = int(parts[-1])
    return base, version


def get_dataset_metadata(latest_version_href):
    base, version = get_dataset_base_and_version(latest_version_href)
    while version > 0:
        dataset_metadata = request(f"{base}/{version}", json=True)
        if 'downloads' not in dataset_metadata or 'csv' not in dataset_metadata['downloads']:
            print(f"The latest dataset version (v{version}) does not have a CSV link.")
            version -= 1
            print(f"Trying previous version: v{version}.")
            continue
        
        if dataset_metadata['downloads']['csv']['href'] in MALFORMED_DATA:
            dataset_metadata['downloads']['csv']['href'] = MALFORMED_DATA[dataset_metadata['downloads']['csv']['href']]
        
        return dataset_metadata


def data_exists(data_path, metdata):
    if data_path.exists():
        csv_size = int(metdata['latest_version_metadata']['downloads']['csv']['size'])
        if path.getsize(data_path) == csv_size:
            return True
    return False


def ons_download_dataset(dataset):
    dataset_dir = DATASETS_DIR / dataset['id']
    makedirs(dataset_dir, exist_ok=True)
    
    metadata_path = dataset_dir / 'metadata.json'
    data_path = dataset_dir / 'data.csv'
    current_metadata = json.load(open(metadata_path)) if metadata_path.exists() else None
    dataset_metadata = get_dataset_metadata(dataset['links']['latest_version']['href'])

    if current_metadata is not None:
        if current_metadata['latest_version_metadata']['downloads']['csv']['href'] == dataset_metadata['downloads']['csv']['href']:
            if data_exists(data_path, current_metadata):
                print("\tAlready latest version")
                return
    
    metadata = copy.deepcopy(dataset)
    metadata['latest_version_metadata'] = dataset_metadata
    json.dump(metadata, open(metadata_path, 'w'), indent=4)

    if not data_exists(data_path, metadata):
        download_file(dataset_metadata['downloads']['csv']['href'], data_path)


def ons_download_datasets():
    datasets = ons_list_datasets()
    num = len(datasets)
    for i, dataset in enumerate(datasets, 1):
        print(f"[{i}/{num}] {dataset['id']}")
        ons_download_dataset(dataset)


if __name__ == "__main__":
    ons_download_datasets()
