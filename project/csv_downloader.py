import pandas as pd 
import os
from kaggle.api.kaggle_api_extended import KaggleApi

class CsvDownloader:

        def __init__(self):
                self.data_url = None
                self.df = None
                

        def authenticate_kaggle(self):
                print("Authanticating Kaggle ... ")
                kaggle_api = KaggleApi()

                if 'GITHUB_ACTIONS' in os.environ:
                        # If in GitHub Actions, use GitHub Secrets
                        kaggle_username = os.environ['KAGGLE_USERNAME']
                        kaggle_key = os.environ['KAGGLE_KEY']
                        kaggle_api.authenticate(username=kaggle_username, key=kaggle_key)

                else:
                        kaggle_api.authenticate()
                
                return kaggle_api

        def download_data(self, data_url):
                self.data_url =  data_url
                self.df = pd.read_csv(data_url, sep=",", on_bad_lines='skip')
                return self.df 
        def download_co2_data(self, kaggle_api):
                #kaggle_api =self.authenticate_kaggle()
                path =  'mabdullahsajid/tracking-global-co2-emissions-1990-2023'
                kaggle_api.dataset_download_files(path, path='./data', unzip=True)

                file_name = "tidy_format_co2_emission_dataset.csv" 
                file_path = os.path.abspath(os.path.join('./data', file_name))
                directory = os.path.dirname(file_path)
                if not os.path.exists(directory):
                        print("directory does not exist cant read co2 csv file", directory)
                        return
                
                return pd.read_csv(file_path , sep=",", on_bad_lines='skip')
        #Normal case just to save it without any modification might need it later for analysis  
        def save_file(self, fileNameWithPath)  :
                if(self.df is not None ):
                        self.df.to_csv(fileNameWithPath,index=False)
                        print('File saved'+fileNameWithPath)


        def save_file_with_modification(self, filename , dataframe)  :
                file_path = os.path.abspath(os.path.join('./data', filename))
                directory = os.path.dirname(file_path)

                # Check if the directory exists
                if not os.path.exists(directory):
                        print("directory does not exist", directory)
                

                if dataframe is not None:
                        dataframe.to_csv(file_path, index=False)
                        print('File saved ' + file_path)
                        return file_path
                else:
                        return 'Dataframe is None, file not saved.'



