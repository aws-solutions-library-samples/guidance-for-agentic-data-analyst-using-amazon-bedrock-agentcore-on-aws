from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()

df = query_handler.query_ons_dataset('cpih01', {
    'aggregate': 'Overall Index'
})

december_data = df[df['time'].str.startswith('Dec')].copy()
december_data['year'] = december_data['time'].str.extract(r'Dec-(\d+)')[0]
december_data['year'] = december_data['year'].apply(lambda x: int('20' + x) if int(x) < 50 else int('19' + x))
december_data = december_data.sort_values('year', ascending=False)
latest_december = december_data.iloc[0]

print(f"Latest December {latest_december['year']} inflation data. CPIH Index: {latest_december['observation']}:")
