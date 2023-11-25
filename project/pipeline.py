from csv_downloader import CsvDownloader
import pandas as pd 
import helpers 
import Constants as const


csv_dowloader = CsvDownloader()

df = csv_dowloader.download_data(const.URL_Temprature)  
#df = pd.read_csv('./data/temprature.csv')

df['dt'] = pd.to_datetime(df['dt'])

#Clean the data here Drop the rows with null or  
df.dropna(subset=['dt','AverageTemperature','AverageTemperatureUncertainty','Country'], inplace=True)
#lost 32,651 rows after this so now need to filter the data with common date range in all countries

helpers.printMinMaxData(df.copy())
df = helpers.filterDataWithDateRange(df, "1948-12-01", "2014-01-01")
helpers.printMissingMonthsData(df.copy())

#After getting the missing data now changing the data range acordingly (1951-2013)
df = helpers.filterDataWithDateRange(df, "1950-12-01" , "2014-01-01")
csv_dowloader.save_file_with_modification('./data/temprature_data_filtired_1951-2013.csv', dataframe=df)

###################################################################################################

#2nd Dataset

csv_dowloader = CsvDownloader()
#get the df from url
df = csv_dowloader.download_data(const.URL_C02_Emission)  
#Clean the data here Drop the rows with null or None
df.dropna(subset=['Country','Year','CO2EmissionRate (mt)'], inplace=True)
df = df.drop('Unnamed: 0', axis=1)

#save it as csv file
csv_dowloader.save_file_with_modification('./data/tidy_format_co2_emission_dataset.csv', df)

df = pd.read_csv('./data/tidy_format_co2_emission_dataset.csv')

print(df)
