import pytest
from pipeline import Pipeline
import pandas as pd
from Constants import urls
import os

@pytest.fixture
def pipeline_instance():
    return Pipeline()

@pytest.fixture
def test_download_mocked_files(pipeline_instance, mocker):
    # Mock the download method to avoid actual file download
    mocker.patch.object(pipeline_instance, 'downloadData', return_value='test_mocked.csv')

    data_files_path = pipeline_instance.downloadData(urls[0]) 

    # Check that the mocked file is returned for each URL
    assert data_files_path == ['test_mocked.csv'] 

    return data_files_path  # Return the value to be used in the test


@pytest.fixture
def test_download_co2_mocked_files(pipeline_instance, mocker):
    # Mock the download method to avoid actual file download
    mocker.patch.object(pipeline_instance, 'downloadCo2Data', return_value='test_mocked.csv')

    data_files_path = [pipeline_instance.downloadCo2Data() ]

    # Check that the mocked file is returned for each URL
    assert data_files_path == ['test_mocked_co2.csv'] 

    return data_files_path  # Return the value to be used in the test



def get_mocked_dataframe():# Create a mocked DataFrame for testing for 1st dataset
    
    import pandas as pd

    data = {
        'dt': ['2022-01-01', '2022-01-02'],
        'AverageTemperature': [25.0, 26.5],
        'AverageTemperatureUncertainty': [1.0, 1.2],
        'Country': ['Country1', 'Country2']
    }
    data2 = {
        'Country': ['Afghanistan', 'Albania'],
        'Year': ['2021', '2021'],
        'CO2EmissionRate (mt)': [8.35, 4.59], 
    }

    return [pd.DataFrame(data) , pd.DataFrame(data2)]



def check_co2_data_types(co2_df):

    if co2_df.empty:
        print("DataFrame is empty")
        return False
    
    # Check if the data types in the CO2 DataFrame match the expected data types
    expected_data_types = {'Country': 'object', 'Year': 'int64', 'CO2EmissionRate (mt)': 'float64'}

    for column, expected_type in expected_data_types.items():
        assert co2_df[column].dtype == expected_type
    return True

def check_temprature_data_types(temperature_df):
    # dt,AverageTemperature,AverageTemperatureUncertainty,Country

    if temperature_df.empty:
        print("DataFrame is empty")
        return False

    expected_data_types = {'Country': 'object', 'Year': 'int64', 'AverageTemperature': 
                           'float64'}

    for column, expected_type in expected_data_types.items():
        assert temperature_df[column].dtype == expected_type
    return True



#System level mocked test 
def test_system_level(pipeline_instance, mocker):
    # Mock the transform method to avoid actual transformation
    mocker.patch.object(pipeline_instance, 'dowloadAndFilterData', side_effect=lambda x: x)

    # Mock the save_file_with_modification method to avoid writing to the file system
    mocker.patch.object(pipeline_instance._csv_dowloader, 'save_file_with_modification', return_value='/path/to/mock_file.csv')        

    # Mock the read_csv method
    mocker.patch('pandas.read_csv', return_value=pd.DataFrame())

    mocked_dataframe = get_mocked_dataframe()

    # Perform the test
    for i, file_path in enumerate(urls):
        if i == 0:
            df = pipeline_instance.dowloadAndFilterData(mocked_dataframe[i])
        else:
            df = pipeline_instance.downloadAndFilter2ndData(mocked_dataframe[i])

        assert not df.isnull().values.any()

    # Debugging: print calls to save_file_with_modification
    print(pipeline_instance._csv_dowloader.save_file_with_modification.call_args_list)
 
    # Debugging: print the actual DataFrame that was passed to the method
    print("Actual DataFrame:")
    print(df)

    # Check that the save_file_with_modification method was called
    pipeline_instance._csv_dowloader.save_file_with_modification.assert_called_once()

    if 1==0:
        check_temprature_data_types(df)
        
    else:
        check_co2_data_types(df)




#def test_dfs_are_not_empty( pipeline_instance , mocker):

    # temperature_df = pd.read_csv('./data/temprature_data_filtired_1951-2013.csv')
    # assert len(temperature_df) > 0  

    # co2_df = pd.read_csv('./data/tidy_format_co2_emission_dataset.csv')
    # assert len(co2_df) > 0  



#System Level test that check whole system/pipeline

def file_exists(file_path):
    return os.path.exists(file_path)


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
