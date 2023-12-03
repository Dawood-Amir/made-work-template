from csv_downloader import CsvDownloader 
import pandas as pd 
import helpers 
import Constants as const


class Pipeline: 
    def __init__(self):
        self._csv_dowloader = CsvDownloader()
        

    def get_csv_downloader(self):
        return self._csv_dowloader


    def downloadData(self , url ): #return df
        return self._csv_dowloader.download_data(url)

    

    def dowloadAndFilterData(self):
        
        #df = pd.read_csv('./data/temprature.csv') # in case if dont wana download again just read the file ind data directory

        df = self.downloadData(const.urls[0])

        df['dt'] = pd.to_datetime(df['dt'])

        #Clean the data here 
        df.dropna(subset=['dt','AverageTemperature','AverageTemperatureUncertainty','Country'], inplace=True)
        #lost 32,651 rows after this so now need to filter the data with common date range in all countries

        helpers.printMinMaxData(df.copy())
        df = helpers.filterDataWithDateRange(df, "1948-12-01", "2014-01-01")
        helpers.printMissingMonthsData(df.copy())

        #After getting the missing data now changing the data range acordingly (1951-2013)
        df = helpers.filterDataWithDateRange(df, "1950-12-01" , "2014-01-01")
        path = self._csv_dowloader.save_file_with_modification('./data/temprature_data_filtired_1951-2013.csv', dataframe=df)
        print(path)


    ###################################################################################################

    #2nd Dataset

    def downloadAndFilter2ndData(self):

        df = self.downloadData(const.urls[1])
        #Clean the data here 

        # Convert 'CO2EmissionRate (mt)' column to float type Conversion bcs of failing the test!
        df['CO2EmissionRate (mt)'] = pd.to_numeric(df['CO2EmissionRate (mt)'], errors='coerce').astype(float)


        df.dropna(subset=['Country','Year','CO2EmissionRate (mt)'], inplace=True)
        df = df.drop('Unnamed: 0', axis=1)

        #save it as csv file
        path =  self._csv_dowloader.save_file_with_modification('./data/tidy_format_co2_emission_dataset.csv', df)
        print(path)
        df = pd.read_csv('./data/tidy_format_co2_emission_dataset.csv')

        #print(df)


    def main(self):
        self.dowloadAndFilterData()
        self.downloadAndFilter2ndData()


if __name__ == '__main__':

    Pipeline().main()