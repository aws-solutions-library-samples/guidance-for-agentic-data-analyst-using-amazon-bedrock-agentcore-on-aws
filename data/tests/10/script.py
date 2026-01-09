df = query_handler.query_ons_dataset('weekly-deaths-age-sex', {
    'agegroups': '100+',
    'registrationoroccurrence': 'Occurrences',
    'sex': 'All'
})

df['week_num'] = df['week'].str.extract('(\d+)').astype(int)

england = df[df['geography'] == 'England']
wales = df[df['geography'] == 'Wales']

england_latest = england.sort_values(['time', 'week_num']).iloc[-1]
wales_latest = wales.sort_values(['time', 'week_num']).iloc[-1]
tot = england_latest + wales_latest

print(f"According to the latest available ONS weekly deaths data (Week {england_latest['week_num']} of {england_latest['time']} there were {tot} deaths of people aged 100 or over in England and Wales.")
