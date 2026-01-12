from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()

df = query_handler.query_ons_dataset('mid-year-pop-est', {
    'geography': 'Cambridge',
    'age': 'Total',
    'sex': 'All'
})
df['time_int'] = df['time'].astype(int)
df_sorted = df.sort_values('time_int', ascending=False)
latest_data = df_sorted.iloc[0]

print(f"Population of Cambridge in {latest_data['time']}: {latest_data['observation']}")