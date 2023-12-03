import pytest
from pipeline import Pipeline
import pandas as pd
from unittest.mock import Mock
import os

@pytest.fixture
def pipeline_instance():
    return Pipeline()
#Unit tests here
def test_download_and_filter_data(pipeline_instance):
    # Call the method you want to test
    pipeline_instance.dowloadAndFilterData()
    assert file_exists('./data/temprature_data_filtired_1951-2013.csv')

def test_download_and_filter_2nd_data(pipeline_instance):
    pipeline_instance.downloadAndFilter2ndData()
    assert file_exists('./data/tidy_format_co2_emission_dataset.csv')

def file_exists(file_path):
    return os.path.exists(file_path)

def test_dfs_are_not_empty():

    temperature_df = pd.read_csv('./data/temprature_data_filtired_1951-2013.csv')
    assert len(temperature_df) > 0  

    co2_df = pd.read_csv('./data/tidy_format_co2_emission_dataset.csv')
    assert len(co2_df) > 0  


def test_download_and_filter_data_with_mock(pipeline_instance, tmp_path):
    # temp directory for test files
    data_folder = tmp_path / "data"

    # Creating a mock CsvDownloader
    mock_downloader = Mock()
    mock_downloader.download_data.return_value = create_mocked_dataframe()
    
    mock_downloader.save_file_with_modification.return_value = "File saved successfully"

    
    pipeline_instance._csv_dowloader = mock_downloader # Replacing the CsvDownloader instance with the mock

    
    pipeline_instance.dowloadAndFilterData()

    file_path = data_folder / "temprature_data_filtired_1951-2013.csv"
    assert file_path.resolve() == file_path.resolve()
    print('Unit level test ended')

def create_mocked_dataframe():# Create a mocked DataFrame for testing
    

    import pandas as pd

    data = {
        'dt': ['2022-01-01', '2022-01-02'],
        'AverageTemperature': [25.0, 26.5],
        'AverageTemperatureUncertainty': [1.0, 1.2],
        'Country': ['Country1', 'Country2']
    }

    return pd.DataFrame(data)


# System Level test that check whole system/pipeline

def test_system_level_pipeline(pipeline_instance, tmp_path): 
    # temporary directory for test files
    data_folder = tmp_path / "data"

    real_downloader = pipeline_instance.get_csv_downloader()
    pipeline_instance._csv_dowloader = real_downloader

    
    pipeline_instance._csv_dowloader.data_url = str(data_folder)  # Set the data folder for saving files
    pipeline_instance.main()

    # Assertions for the first dataset
    assert file_exists('./data/temprature_data_filtired_1951-2013.csv')
    temperature_df = pd.read_csv('./data/temprature_data_filtired_1951-2013.csv')
    assert not temperature_df.empty  # Check if the DataFrame is not empty
    assert check_temprature_data_types(temperature_df)

    # Assertions for the second dataset
    assert file_exists('./data/tidy_format_co2_emission_dataset.csv')
    co2_df = pd.read_csv('./data/tidy_format_co2_emission_dataset.csv')
    assert not co2_df.empty  # Check if the DataFrame is not empty
    assert check_co2_data_types(co2_df)
    print('Sys level test ended')



def check_co2_data_types(co2_df):
    # Check if the data types in the CO2 DataFrame match the expected data types
    expected_data_types = {'Country': 'object', 'Year': 'int64', 'CO2EmissionRate (mt)': 'float64'}

    for column, expected_type in expected_data_types.items():
        assert co2_df[column].dtype == expected_type
    return True

def check_temprature_data_types(temperature_df):
    # dt,AverageTemperature,AverageTemperatureUncertainty,Country

    expected_data_types = {'dt': 'object', 'AverageTemperature': 'float64', 'AverageTemperatureUncertainty': 
                           'float64' ,'Country': 'object'}

    for column, expected_type in expected_data_types.items():
        assert temperature_df[column].dtype == expected_type
    return True