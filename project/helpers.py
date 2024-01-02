import pandas as pd


def filterDataWithDateRange(df,startingDate, endingDate):
    return df[(df['dt'] > startingDate) & (df['dt'] < endingDate)]


#Print if theres any month is missing in an year in the dataset 
def printMissingMonthsData(df):
    print("Working on missing data...")

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
                #print("starting month count is not 1 or something else wrong here ")
                #print(row)
                df2= pd.concat([df2, df], ignore_index=True)
                #df2 = df2.append(row)

        else:
            monthCount = monthCount+1
            
            lastyear = pd.to_datetime(row['dt']).year

    #print("Printing the missing data...............")
    #print(df2)
    df2= None
    

#Print The Min Max data according the countries and date
def printMinMaxData(df):
    print("Working on MinMax data...")
    # Find the minimum and maximum dates for each country
    min_max_dates = df.groupby('Country')['dt'].agg(['min', 'max']).reset_index()
    # print("Min & Max date of all countries ")
    # print(min_max_dates)
    # print("\n")
    # Identify the common date range here
    common_start_date = min_max_dates['min'].max()
    common_end_date = min_max_dates['max'].min()

    filtered_df = filterDataWithDateRange(df, str(common_start_date) , str(common_end_date))

    min_max_dates = filtered_df.groupby('Country')['dt'].agg(['min', 'max']).reset_index()

    common_start_date = min_max_dates['min'].max()
    common_end_date = min_max_dates['max'].min()

    df =None
    #print("Common start date in all Countries " + str(common_start_date) +"\n")
    #print("Common End date in all Countries "+str(common_end_date))




