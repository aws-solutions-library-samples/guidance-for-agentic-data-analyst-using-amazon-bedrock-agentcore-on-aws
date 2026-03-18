DATASETS = [
    'ons',
]


def standard_dataset_decription(dataset_id, url, title, usage_description):
    return f"# ID [{dataset_id}]({url}): {title}\n{usage_description}"
