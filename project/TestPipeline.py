import pytest
import os
import pandas as pd
from pipeline import Pipeline
import Constants as const  # Assuming you have a file named Constants.py with your URLs

@pytest.fixture
def pipeline():
    return Pipeline()

@pytest.fixture
def test_data_folder(tmpdir):
    return tmpdir.mkdir("data")

def test_download_and_filter_data(pipeline, test_data_folder):
    # Replace the constant URL with a URL pointing to a test CSV file
    test_url = const.urls[0]

    # Use the test_data_folder as the destination for downloaded files
    pipeline.get_csv_downloader().set_download_folder(str(test_data_folder))

    # Call the download and filter function
    pipeline.downloadAndFilterData(test_url)

    # Check if the file is downloaded and saved
    downloaded_file_path = os.path.join(str(test_data_folder), 'temprature_data_filtired_1951-2013.csv')
    assert os.path.isfile(downloaded_file_path)

    # Check if the downloaded file can be loaded as a DataFrame
    df = pd.read_csv(downloaded_file_path)
    assert not df.empty  # Check if the DataFrame is not empty

    # Add more specific checks based on your requirements

def test_download_and_filter_2nd_data(pipeline, test_data_folder):
    # Replace the constant URL with a URL pointing to a test CSV file
    test_url = const.urls[1]

    # Use the test_data_folder as the destination for downloaded files
    pipeline.get_csv_downloader().set_download_folder(str(test_data_folder))

    # Call the download and filter function
    pipeline.downloadAndFilter2ndData(test_url)

    # Check if the file is downloaded and saved
    downloaded_file_path = os.path.join(str(test_data_folder), 'tidy_format_co2_emission_dataset.csv')
    assert os.path.isfile(downloaded_file_path)

    # Check if the downloaded file can be loaded as a DataFrame
    df = pd.read_csv(downloaded_file_path)
    assert not df.empty  # Check if the DataFrame is not empty

    # Add more specific checks based on your requirements

def test_download_and_check_data(pipeline, test_data_folder):
    # Use the test_data_folder as the destination for downloaded files
    pipeline.get_csv_downloader().set_download_folder(str(test_data_folder))

    # Call the download and check data function
    pipeline.download_and_check_data(const.urls)

    # Check if each file exists in the data folder
    for url in const.urls:
        file_name = url.split("/")[-1]
        downloaded_file_path = os.path.join(str(test_data_folder), file_name)
        assert os.path.isfile(downloaded_file_path), f"File {file_name} does not exist in the data folder."

    print("All files downloaded and saved successfully.")
