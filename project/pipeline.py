from csv_downloader import CsvDownloader 
import pandas as pd 
import helpers 
import Constants as const 
from sklearn.impute import SimpleImputer
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import KNNImputer

class Pipeline: 
    def __init__(self):
        self._csv_dowloader = CsvDownloader()
        

    def get_csv_downloader(self):
        return self._csv_dowloader


    def downloadData(self , url ): #return df
        return self._csv_dowloader.download_data(url)

    def dowloadAndFilterData(self,df):
        print("Working on 1st dataset ...")
        
        #df = pd.read_csv('./data/temprature.csv') # in case if dont wana download again just read the file from data directory

        df['dt'] = pd.to_datetime(df['dt'])

        #Clean the data here 
        df.dropna(subset=['dt','AverageTemperature','AverageTemperatureUncertainty','Country'], inplace=True)
        #lost 32,651 rows after this so now need to filter the data with common date range in all countries

        helpers.printMinMaxData(df.copy())
        df = helpers.filterDataWithDateRange(df, "1948-12-01", "2014-01-01")
        helpers.printMissingMonthsData(df.copy())

        #After getting the missing data now changing the data range acordingly (1951-2013)
        df = helpers.filterDataWithDateRange(df, "1950-12-01" , "2014-01-01")
         # Convert 'dt' column to datetime
        df['dt'] = pd.to_datetime(df['dt'])

        # Extract year from 'dt' and create 'Year' column
        df['Year'] = df['dt'].dt.year

        # Aggregate temperature data by taking the average temperature for each country and year
        aggregated_temperature_df = df.groupby(['Country', 'Year'], as_index=False)['AverageTemperature'].mean()
        
        self._csv_dowloader.save_file_with_modification('temprature_data_filtired_1951-2013.csv', dataframe=aggregated_temperature_df)
        
        return df



    ###################################################################################################

    #2nd Dataset

    def downloadAndFilter2ndData(self, df):

        
        #Clean the data here 
        print("Working on 2nd dataset ...")
        # Convert 'CO2EmissionRate (mt)' column to float type Conversion bcs of failing the test!
        df['CO2EmissionRate (mt)'] = pd.to_numeric(df['CO2EmissionRate (mt)'], errors='coerce').astype(float)

    
        df.dropna(subset=['Country','Year','CO2EmissionRate (mt)'], inplace=True)

        if 'Unnamed: 0' in df.columns:
            df = df.drop('Unnamed: 0', axis=1)

        #save it as csv file
        self._csv_dowloader.save_file_with_modification('tidy_format_co2_emission_dataset.csv', df)
        
        df = pd.read_csv('./data/tidy_format_co2_emission_dataset.csv')
        return df
    

    def mergeDf(self):
        temperature_df = pd.read_csv('./data/temprature_data_filtired_1951-2013.csv')
        co2_df = pd.read_csv('./data/tidy_format_co2_emission_dataset.csv')

        common_countries = set(co2_df['Country']).intersection(set(temperature_df['Country']))
        merged_dfs = []

        # Iterate over common countries
        for country in common_countries:
            # Find the range of common years for the current country
            common_years = set(co2_df[co2_df['Country'] == country]['Year']).intersection(
                set(temperature_df[temperature_df['Country'] == country]['Year'])
            )

            # Explicitly add missing years for the current country
            missing_years = set(range(1990, 2013)) - common_years
            missing_rows = pd.DataFrame({
                'Country': [country] * len(missing_years),
                'Year': list(missing_years)
            })

            # Filter datasets to include both common and missing years for the current country
            co2_df_country = pd.concat([
                co2_df[(co2_df['Country'] == country) & (co2_df['Year'].isin(common_years))],
                missing_rows
            ])

            temperature_df_country = pd.concat([
                temperature_df[(temperature_df['Country'] == country) & (temperature_df['Year'].isin(common_years))],
                missing_rows
            ])
            merged_df_country = pd.merge(co2_df_country, temperature_df_country, on=['Country', 'Year'])
            merged_dfs.append(merged_df_country)

        # Concatenate DataFrames for all common countries
        final_merged_df = pd.concat(merged_dfs, ignore_index=True)

        # Convert 'Year' column to integer
        final_merged_df['Year'] = final_merged_df['Year'].astype(int)

        final_merged_df = final_merged_df[(final_merged_df['Year'] >= 1990) & (final_merged_df['Year'] <= 2012)]

        final_merged_df =  self.imputeMean(final_merged_df) #for now ill use regression
        self.checkMissingYears(final_merged_df)
        # Sort the DataFrame by the "Country" column
        df_sorted = final_merged_df.sort_values(by='Country')

        self._csv_dowloader.save_file_with_modification('merged_df.csv', df_sorted)
        

        return final_merged_df


    def checkMissingYears(self, df):
        expected_years = set(range(1990, 2012))

        # Group the data by 'Country' and count the number of unique years for each group
        country_year_counts = df.groupby('Country')['Year'].nunique()

        # Check if each country has data for every year from 1990 to 2012
        missing_years = {}
        for country, count in country_year_counts.items():
            available_years = set(df[df['Country'] == country]['Year'])
            missing = list(expected_years - available_years)
            if missing:
                missing_years[country] = missing

        # Print the countries with missing years
        if not missing_years:
            print("All countries have data for every year from 1990 to 2012.")
        else:
            print("Countries with missing years:")
            for country, missing in missing_years.items():
                print(f"{country}: {missing}")



#Imputation methods define here 
   
    def imputeMean(self, df):
        imputed_df = df.copy()
        imputer = SimpleImputer(strategy='mean')
        imputed_df[['AverageTemperature', 'CO2EmissionRate (mt)']] = imputer.fit_transform(imputed_df[['AverageTemperature', 'CO2EmissionRate (mt)']])
        return imputed_df

    
    def imputeMedian(self, df):
        imputed_df = df.copy()
        imputer = SimpleImputer(strategy='median')
        imputed_df[['AverageTemperature', 'CO2EmissionRate (mt)']] = imputer.fit_transform(imputed_df[['AverageTemperature', 'CO2EmissionRate (mt)']])
        return imputed_df

    def imputeKNN(self, df):
        
        imputed_df = df.copy()

        # Extract features for imputation
        features = imputed_df[['Year', 'CO2EmissionRate (mt)']]

        k_neighbors = 5  
        knn_imputer = KNNImputer(n_neighbors=k_neighbors)

        imputed_values = knn_imputer.fit_transform(features)

        imputed_df[['Year', 'CO2EmissionRate (mt)']] = imputed_values

        return imputed_df
    
    def imputeRegression(self, df):
        imputed_df = df.copy()

        columns_to_impute_regression = ['AverageTemperature', 'CO2EmissionRate (mt)']

        # Group by 'Country' and perform linear interpolation for missing values
        for column in columns_to_impute_regression:
            imputed_df[column] = imputed_df.groupby('Country')[column].transform(
                lambda group: group.interpolate(method='linear', limit_direction='both')
            )

        return imputed_df


    def main(self):
        df = self.downloadData(const.urls[0])
        self.dowloadAndFilterData(df)
        df2 = self.downloadData(const.urls[1])
        self.downloadAndFilter2ndData(df2)
        merged_df = self.mergeDf()

        return merged_df # returning bcs possible use in Report 

if __name__ == '__main__':

    Pipeline().main()