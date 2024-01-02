import pandas as pd 
import os
class CsvDownloader:

        def __init__(self):
                self.data_url = None
                self.df = None
                

        def download_data(self, data_url):
                self.data_url =  data_url
                self.df = pd.read_csv(data_url, sep=",", on_bad_lines='skip')
                return self.df 
        
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



