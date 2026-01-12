from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()

df = query_handler.query_ons_dataset('mid-year-pop-est', {
    'geography': 'ENGLAND AND WALES',
    'age': '90+',
    'sex': 'All'
})

df['time'] = df['time'].astype(int)
latest = df[df['time'] == df['time'].max()]
latest = latest.iloc[0]

print(f"As of {latest['time']}, the population aged 90 or over in England and Wales is: {latest['observation']}")
