from csv_downloader import CsvDownloader
import pandas as pd 
import Constants as const


csv_dowloader = CsvDownloader()
#get the df from url
df = csv_dowloader.download_data(const.URL_Temprature)  

#save it as csv file
csv_dowloader.save_file_with_modification('./data/temprature.csv', df)

#df = pd.read_csv('./data/temprature.csv')
#Clean the data here Drop the rows with null or  
df.dropna(subset=['dt','AverageTemperature','AverageTemperatureUncertainty','Country'], inplace=True)
#lost 32,651 rows after this 

#/////////////////////////////////////////////////////////////////


# Filter the original DataFrame to include only the rows within the common date range
df['dt'] = pd.to_datetime(df['dt'])

#Making a copy of df here Really dont need to change the df here 

df1 = df.copy()

# Find the minimum and maximum dates for each country
min_max_dates = df1.groupby('Country')['dt'].agg(['min', 'max']).reset_index()

# Identify the common date range here
common_start_date = min_max_dates['min'].max()
common_end_date = min_max_dates['max'].min()

# Filter the original DataFrame to include only the rows within the common date range
filtered_df = df1[(df1['dt'] >= common_start_date) & (df1['dt'] <= common_end_date)]

# Display the result DataFrame with only the rows within the common date range

min_max_dates = filtered_df.groupby('Country')['dt'].agg(['min', 'max']).reset_index()
print(min_max_dates)



#Removing the data except this date range  (1949 - 2013)

#df['dt'] = pd.to_datetime(df['dt'])
df = df[(df['dt'] > "1948-12-01") & (df['dt'] < "2014-01-01")]

#csv_dowloader.save_file_with_modification('./data/temprature_new_filtered1949.csv', dataframe=df)


def getMissingMonthsData(df):
    monthCount  = 1
    lastyear = 1949
    currentyear = 0
    df2 = pd.DataFrame(columns=['dt','AverageTemperature','AverageTemperatureUncertainty','Country'])
    for i, row in df.iterrows():

        currentyear = pd.to_datetime(row['dt']).year

        if(currentyear != lastyear or monthCount >12):
            if(currentyear != lastyear and pd.to_datetime(row['dt']).month ==1):
                country = row['Country']
                #print("Next year started",currentyear, "everythings ok so far Country..."+country)
                lastyear = pd.to_datetime(row['dt']).year
                monthCount =1
                monthCount = pd.to_datetime(row['dt']).month
            else:
                print("starting month count is not 1 or something else wrong here ")
                print(row)
                df2= pd.concat([df2, df], ignore_index=True)
                #df2 = df2.append(row)

        else:
            monthCount = monthCount+1
            
            lastyear = pd.to_datetime(row['dt']).year

    return df2        




#Making a copy of df here Really dont need to change the df here 
df2 = df.copy()
missing_Data =getMissingMonthsData(df2)
print("Printing the missing data...............")
print(missing_Data)
#After getting the missing data now changing the data range acordingly (1951-2013)

df = df[(df['dt'] > "1950-12-01") & (df['dt'] < "2014-01-01")]
#remove unvanted colloumns here  

csv_dowloader.save_file_with_modification('./data/temprature_data_filtired_1951-2013.csv', dataframe=df)

###################################################################################################



#After just do the same for the C02 file first implement the func that authenticate from kagle
#and download the csv file 

csv_dowloader = CsvDownloader()
#get the df from url
df = csv_dowloader.download_data(const.URL_C02_Emission)  
#Clean the data here Drop the rows with null or  
df.dropna(subset=['Country','Year','CO2EmissionRate (mt)'], inplace=True)
df = df.drop('Unnamed: 0', axis=1)

#save it as csv file
csv_dowloader.save_file_with_modification('./data/tidy_format_co2_emission_dataset.csv', df)

df = pd.read_csv('./data/tidy_format_co2_emission_dataset.csv')

print(df)






