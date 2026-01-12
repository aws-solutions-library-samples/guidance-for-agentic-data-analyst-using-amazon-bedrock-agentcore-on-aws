from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()

df = query_handler.query_ons_dataset('weekly-deaths-age-sex', {
    'geography': 'London',
    'agegroups': '70-74',
    'registrationoroccurrence': 'Occurrences',
    'sex': 'All'
})

df['week_num'] = df['week'].str.extract(r'Week (\d+)').astype(int)
latest = df.sort_values(['time', 'week_num']).iloc[-1]

print("The ONS does not publish weekly death statistics broken down by single year of age (72 years).")
print(f"However, the closest available data shows that in Week {latest['week_num']}, {latest['time']}, there were {latest['observation']} deaths of people aged 70-74 in London.")
