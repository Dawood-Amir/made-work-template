import pandas as pd 

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

        def save_file_with_modification(self, fileNameWithPath , dataframe)  :
                if(dataframe is not None ):
                        dataframe.to_csv(fileNameWithPath,index=False)
                        return 'File saved'+fileNameWithPath



