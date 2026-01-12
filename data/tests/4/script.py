import pandas as pd

from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()

df = query_handler.query_ons_dataset('gdp-to-four-decimal-places', {
    'unofficialstandardindustrialclassification': 'A-T : Monthly GDP'
})

df['date'] = pd.to_datetime(df['time'], format='%b-%y')
df_sorted = df.sort_values('date', ascending=False)
latest = df_sorted.iloc[0]

print(f"Latest Monthly GDP Estimate: ({latest['time']}): {latest['observation']}")
