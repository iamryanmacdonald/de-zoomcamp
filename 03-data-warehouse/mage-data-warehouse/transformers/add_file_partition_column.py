import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(data.info())
    
    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_datetime'])
    data['lpep_dropoff_date'] = data['lpep_dropoff_datetime'].dt.date

    print(data.info())

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
