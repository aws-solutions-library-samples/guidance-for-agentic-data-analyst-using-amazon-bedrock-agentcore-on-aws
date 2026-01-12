from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()

df = query_handler.query_ons_dataset('labour-market', {
    'agegroups': '65+',
    'economicactivity': 'In Employment',
    'sex': 'All adults',
    'seasonaladjustment': 'Not Seasonally Adjusted',
    'unitofmeasure': 'Rates'
})

def parse_time_period(time_str):
    """Parse time period string to get a sortable date"""
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    # Format is like "Feb-Apr 2025"
    parts = time_str.split()
    year = int(parts[1])
    end_month = parts[0].split('-')[1]
    month = month_map[end_month]
    return (year, month)

df['sort_key'] = df['time'].apply(parse_time_period)
df = df.sort_values('sort_key', ascending=False)

latest_data = df.iloc[0]
print(f"Latest employment rate for those aged 65 and over ({latest_data['time']}): {latest_data['observation']}%")
