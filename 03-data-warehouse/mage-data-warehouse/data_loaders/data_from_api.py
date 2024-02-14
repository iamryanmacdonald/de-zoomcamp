import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    combined_data = []

    for month in range(1, 13):
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-%02d.parquet' % (month)
        
        data = pd.read_parquet(url)
        combined_data.append(data)

    return pd.concat(combined_data)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
