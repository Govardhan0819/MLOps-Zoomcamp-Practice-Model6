import pandas as pd 

year=2023
month=1
input_path = f'/workspaces/MLOps-Zoomcamp-Practice-Model6/test/{year:04d}-{month:02d}.parquet'
input = pd.read_parquet(input_path)
out_path= f'{year:04d}-{month:02d}.parquet'
input.to_parquet(
    out_path,
    engine='pyarrow',
    compression=None,
    index=False
)

df_input = pd.read_parquet(f"{year:04d}-{month:02d}.parquet")


input_file = f's3://nyc-duration/in/{year:04d}-{month:02d}.parquet'

options = {
    'client_kwargs': {
        'endpoint_url': "http://localhost:4566"
    }
}

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)