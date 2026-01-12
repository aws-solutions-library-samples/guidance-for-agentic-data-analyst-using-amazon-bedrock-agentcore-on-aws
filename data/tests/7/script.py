import pandas as pd

from aws_data_analyst.cloud_datasets import CloudQueryHandler

query_handler = CloudQueryHandler()

df = query_handler.query_ons_dataset('cpih01', {})

df['date'] = pd.to_datetime(df['time'], format='%b-%y')
latest_time = df['date'].max()
df_latest = df[df['date'] == latest_time]

car_aggregates = [
    '07.1.1.1 New motor cars',
    '07.1.1.2 Second-hand motor cars'
]
df_cars = df_latest[df_latest['aggregate'].isin(car_aggregates)]

print(f"Latest CPIH Index values for car-related categories ({latest_time}):")
for _, row in df_cars.iterrows():
    print(f"{row['aggregate']}: {row['observation']}")
