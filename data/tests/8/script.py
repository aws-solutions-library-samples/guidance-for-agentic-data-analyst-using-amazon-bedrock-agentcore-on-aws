from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()

df = query_handler.query_ons_dataset('mid-year-pop-est', {
    'geography': 'Blaenau Gwent',
    'age': '30',
    'sex': 'All'
})

df['time_int'] = df['time'].astype(int)
df_sorted = df.sort_values('time_int', ascending=False)
latest = df_sorted.iloc[0]

print(f"Latest data for 30 year-olds in Blaenau Gwent ({latest['time']}): {latest['observation']}")
