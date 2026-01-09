df = query_handler.query_ons_dataset('gdp-to-four-decimal-places', {
    'unofficialstandardindustrialclassification': 'A-T : Monthly GDP'
})

def parse_time(time_str):
    """Parse time string like 'Oct-25' to datetime"""
    try:
        return pd.to_datetime(time_str, format='%b-%y')
    except:
        return pd.NaT

df['parsed_time'] = df['time'].apply(parse_time)

df_sorted = df.sort_values('parsed_time', ascending=False)

latest_row = df_sorted.iloc[0]
latest_time = latest_row['time']
latest_gdp = latest_row['observation']

print(f"Latest Monthly GDP Estimate: ({latest_time}): {latest_gdp}")
