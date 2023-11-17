from csv_downloader import CsvDownloader
import pandas as pd 
import Constants as const

#Normal case just to save it without any modification might need it later for analysis  
# csv_dowloader = CsvDownloader()
# csv_dowloader.download_data(const.URL_CO2)
# csv_dowloader.save_file('./data/temprature.csv')


##################uncomment these  cuz have to download the file everytime####################
csv_dowloader = CsvDownloader()
#get the df from url
df = csv_dowloader.download_data(const.URL_Temprature)  

#save it as csv file
csv_dowloader.save_file_with_modification('./data/temprature.csv', df)
##################################################################

df = pd.read_csv('./data/temprature.csv')

print("df describe before : ")
print(df.info(max_cols=10))

#Clean the data here  
df.dropna(subset=['AverageTemperature','AverageTemperatureUncertainty','Country'], inplace=True)
dfd = df.head(50)
#lost 32,651 rows after this 

#print(dfd)
print("df describe after : ")
print(df.info(max_cols=10))

df['dt'] = pd.to_datetime(df['dt'])
print(df['dt'])
df.drop(df.columns[df.columns.str.contains('Unnamed: 0',case = False)],axis = 1, inplace = True)
df = df[(df['dt'] > "1866-01-01") & (df['dt'] < "1932-07-01")]

print("selected data ") 
print(df)


#this code is working jusr adjust the date as you see fit  

# csv_dowloader.save_file_with_modification('./data/temprature_new.csv', dataframe=df)
# df = pd.read_csv('./data/temprature_new.csv') # save it later somehow its not showing the dt type as datetime but object 
# print(df.describe)
# print(df.dt.dtypes)



#After just do the same for the C02 file first implement the func that authenticate from kagle
# and download the csv file 





