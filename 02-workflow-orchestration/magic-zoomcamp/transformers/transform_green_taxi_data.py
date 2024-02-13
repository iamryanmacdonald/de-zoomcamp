import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data = data.rename(columns={
        'VendorID': 'vendor_id',
        'RatecodeID': 'ratecode_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id'
    })

    return data


@test
def test_vendor_id(output, *args) -> None:
    assert 'vendor_id' in output.columns, 'Column "vendor_id" is missing'

@test
def test_passenger_count(output, *args) -> None:
    assert output[output['passenger_count'] == 0].shape[0] == 0, 'There are trips with a zero passenger count'

@test
def test_trip_distance(output, *args) -> None:
    assert output[output['trip_distance'] == 0].shape[0] == 0, 'There are trips with no trip distance'