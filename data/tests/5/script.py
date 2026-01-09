df = query_handler.query_ons_dataset('gdp-to-four-decimal-places', {
    'unofficialstandardindustrialclassification': 'P : Education'
})

df['date'] = pd.to_datetime(df['time'], format='%b-%y')
df_sorted = df.sort_values('date', ascending=False)
latest = df_sorted.iloc[0]

print(f"Latest monthly GDP estimate for the Education sector ({latest['time']}): {latest['observation']}")
