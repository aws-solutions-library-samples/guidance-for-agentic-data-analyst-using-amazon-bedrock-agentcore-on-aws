df = query_handler.query_ons_dataset('labour-market', {
    'agegroups': '16-24',
    'economicactivity': 'Unemployed',
    'geography': 'United Kingdom',
    'seasonaladjustment': 'Not Seasonally Adjusted',
    'sex': 'All adults',
    'unitofmeasure': 'Levels'
})

def parse_time_period(time_str):
    """Parse time period string to get end date for sorting"""
    months = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    parts = time_str.split()
    year = int(parts[1])
    end_month = months[parts[0].split('-')[1]]
    return (year, end_month)

df['time_sort'] = df['time'].apply(parse_time_period)
df_sorted = df.sort_values('time_sort', ascending=False)

latest_row = df_sorted.iloc[0]
latest_time = latest_row['time']
latest_unemployed = latest_row['observation']

print(f"Number of unemployed 16-24 year-olds in {latest_time}: {latest_unemployed:,.0f}")