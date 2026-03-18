import re


DATASETS = [
    'ons',
]


def normalize_dataset_id(dataset_id: str) -> str:
    tmp_str = dataset_id.strip().lower()
    tmp_str = re.sub(r'[^a-z0-9]', '_', tmp_str)
    return re.sub(r'_+', '_', tmp_str)


def standard_dataset_decription(dataset_id, url, title, usage_description):
    return f"# ID [{dataset_id}]({url}): {title}\n{usage_description}"
