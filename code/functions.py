'''
This module provides access to the functions used in notebooks 1, 2, and 3.
'''

import requests 
import pandas as pd
import json
import os
from sqlalchemy import create_engine
import geopandas as gpd
from shapely import *
import shapefile
import pycountry
from shapely.geometry import shape


def fetch_data(country, indicator, start_year, end_year):
    """
    extracts the data from the wold bank API for a given country, indicator, and time range. 
    """
    url = f"http://api.worldbank.org/v2/country/{country}/indicator/{indicator}?date={start_year}:{end_year}&format=json"
    response = requests.get(url)
    return response.json()


def collect_data(countries, indicators, start_year, end_year, save_path):
    """
    saves world bank API data for a given country, indicator, and time range to an inputted file_path. 
    """
    all_data = []
    for country in countries:
        for indicator_name, indicator_code in indicators.items():
            print(f"Fetching data for {country} - {indicator_name}...")
            country_data = fetch_data(country, indicator_code, start_year, end_year)

            filename = os.path.join(save_path, f"{country}_{indicator_name}.json")
            with open(filename, 'w') as f:
                json.dump(country_data, f, indent=4)
                
            all_data.append({
                "country": country,
                "indicator": indicator_name,
                "data": country_data
            })

    return all_data


def process_file(file_path):
    """
    reads in a JSON file from an inputted path, normalizes it and saves it as a pandas dataframe
    designed specifically to read in the World Bank raw data. 
    """
    with open(file_path, 'r') as file:
        data = json.load(file)
    records = data[1]
    return pd.json_normalize(records)


def process_gdelt_data(bucket, countries, output_dir):
    """
    Extract the GDELT data from google cloud storage and saves each country and year as a JSON.
    For large datasets (over the github 50MB storage capacity), chunks the data into separate files. 
    """
    if countries is None:
        countries = ['US', 'FR', 'ZA', 'IR', 'BR', 'IN']
    
    for file in bucket.list_blobs():
        year = file.name.split('_')[1].split('.')[0]
        file_content = file.download_as_text()
        df = pd.read_json(file_content, lines=True)
        df.drop(columns=['ActionGeo_FullName'], inplace=True)

        for country in countries:
            country_df = df[df['ActionGeo_CountryCode'] == country]
            json_data = country_df.to_json()

            if len(json_data.encode('utf-8')) > 52428800:  # 50MB size limit
                num_parts = (len(json_data.encode('utf-8')) // 52428800) + 1
                chunk_size = len(country_df) // num_parts
                for i in range(num_parts):
                    chunk = country_df.iloc[i * chunk_size: (i + 1) * chunk_size]
                    part_filename = f"{output_dir}{country}_{year}({i + 1}).json"
                    chunk.to_json(part_filename)
            else:
                output_filename = f"{output_dir}{country}_{year}.json"
                country_df.to_json(output_filename)


def process_gdelt_json(data_directory):
    """
    Takes the large GDELT data from the raw data folder and aggregates by event type, country, and year.
    Calculates the mean, min, and max of the goldstein score and the journalistic tone for each event type, country, and year.
    """
    data_files = [os.path.join(data_directory, f) for f in os.listdir(data_directory) if f.endswith(".json")]
    raw_results = []

    for file_path in data_files:
        with open(file_path, 'r') as file:
            data = json.load(file)
            df = pd.DataFrame(data)
            df = df[['SQLDATE', 'EventCode', 'NumMentions', 'GoldsteinScale', 'AvgTone', 'ActionGeo_CountryCode']]
            df.rename(columns={'ActionGeo_CountryCode': 'country_code'}, inplace=True)
            df['year'] = df['SQLDATE'].astype(str).str[:4].astype(int)

            grouped = df.groupby(['country_code', 'year', 'EventCode']).agg(
                num_occurrences=('EventCode', 'size'),
                num_mentions=('NumMentions', 'sum'),
                avg_goldstein=('GoldsteinScale', 'mean'),
                min_goldstein=('GoldsteinScale', 'min'),
                max_goldstein=('GoldsteinScale', 'max'),
                avg_tone=('AvgTone', 'mean'),
                min_tone=('AvgTone', 'min'),
                max_tone=('AvgTone', 'max'),
            ).reset_index()

            raw_results.append(grouped)

    raw_df = pd.concat(raw_results, ignore_index=True)

    final_df = raw_df.groupby(['country_code', 'year', 'EventCode']).agg(
        num_occurrences=('num_occurrences', 'sum'),
        num_mentions=('num_mentions', 'sum'),

        # weighted averages:
        avg_goldstein=('avg_goldstein', lambda x: (x * raw_df.loc[x.index, 'num_occurrences']).sum() / raw_df.loc[x.index, 'num_occurrences'].sum()),
        avg_tone=('avg_tone', lambda x: (x * raw_df.loc[x.index, 'num_occurrences']).sum() / raw_df.loc[x.index, 'num_occurrences'].sum()),
        
        # absolute mins and maxes: 
        min_goldstein=('min_goldstein', 'min'),
        max_goldstein=('max_goldstein', 'max'),
        min_tone=('min_tone', 'min'),
        max_tone=('max_tone', 'max'),
    ).reset_index()

    final_df = final_df[
        ['country_code', 'year', 'EventCode', 'num_occurrences', 'num_mentions',
         'avg_goldstein', 'min_goldstein', 'max_goldstein', 
         'avg_tone', 'min_tone', 'max_tone']
    ]

    return final_df


def categorize_event(event_code):
    """
    Creates a broad category for the GDELT event type using the codebook's definitions. 
    Used as .apply with pandas. 
    """
    if str(event_code).startswith(('10', '11', '13', '14', '15', '18', '20')):
        return str(event_code)[:2]  # Use the first two digits as the category
    else:
        return 'Other'


def act_on_democracy(event_code):
    """
    Categorizes events as democratic or anti-democratic using the codebook's definitions, descriptions, and examples. 
    Used as .apply with pandas. 
    """
    promoting_democracy = [1041, # demand leadership change
               1042, # demand policy change
               1043, # demand rights
               1044, # demand change in institutions or regime 
               133, # threaten political dissent (by civilians)
               141, # demonstrate or rally 
               1411, # demonstrate for leadership change
               1412, # demonstrate for policy change
               1414, # demonstrate for change in institutions 
               1413, # demonstrate for human rights 
               142, # general hunger strike
               1423, # hunger strike for human rights
               1421, # hunger strike for leadership change
               1422, # hunger strike for policy change
               1423, # hunger strike for human rights
               1424, # hunger strike for change in institutions 
               143, # general strike 
               1431, #strike for leadership change
               1432, # strike for policy change
               1434, # strike for change in institutions
               1433, # strike or boycott for human rights
               1441, # obstruction for leadership change
               1442, # obstruction for policy change
               1444, # obstruction for institutional change 
               1443, # obstruction for human rights 
               1451, # riot for leadership change
               1452, # riot for policy change
               1454, # riot for institutional change 
               1453, # riot for human rights
               1033, # demand humanitarian aid
               104, # demand political reform
               1122, # accuse of humans rights abuses 
               1124, # accuse of war crimes
               113, # rally opposition against
               1312, # threaten a boycott or strike
               ]
    
    limiting_democracy = [1052, # demand easing of political dissent
                          1321, # threaten w/ restrictions on politcal freedoms
                          1322, # threaten to ban political parties or politicians
                          1323, # threaten curfew
                          1324, # threaten martial law
                          137,  # threaten w/ repression
                          1382, # theaten occupation 
                          1385, # threaten unconventional mass violence
                          153, # increase and/or mobilize military and/or police power (upon civilians)
                          1822, # torture 
                          201, # mass expulsion
                          202, # mass killings
                          203, # ethnic cleansing
                          ]
    if isinstance(event_code, str):
        if int(event_code) in promoting_democracy:
            return 1
        elif int(event_code) in limiting_democracy:
            return -1
        else:
            return 0
    else:
        if event_code in promoting_democracy:
            return 1
        elif event_code in limiting_democracy:
            return -1
        else:
            return 0


def economic_stability_score(country_id, year):
    """
    This function assigns an economic stability score from 1-5 for a given country and year based on the data from the World Bank API.
    
    Parameters
    ----------
    country_id: str, the ID of the country with length 2, for example 'US' or 'ZA'
    year: int, the year to analyse

    Returns
    -------
    int, the economic stability score ranging from 0-27, totaling the scores of each country and year 
    """
    
    engine = create_engine('sqlite:///../data/processed/processed_database.db', echo=False, isolation_level="AUTOCOMMIT")
    
    query = f"""
        SELECT *
        FROM world_bank
        WHERE country_id = '{country_id}' AND year = {year}
    """

    filtered_world_bank = pd.read_sql_query(query, engine)
    
    score = 0
    
    # GDP Growth (annual %)
    gdp_growth = filtered_world_bank[filtered_world_bank['indicator_id'] == 'NY.GDP.MKTP.KD.ZG']['value']

    if float(gdp_growth.iloc[0]) < 1.2:   # < Q1
        score += 0
    elif 1.2 <= float(gdp_growth.iloc[0]) < 2.9:  # Q1 - Q2
        score += 1
    elif 2.9 <= float(gdp_growth.iloc[0]) < 4.8:  # Q2 - Q3
        score += 2
    else:  # > Q3
        score += 3

    # Inflation, consumer prices (annual %)
    inflation = filtered_world_bank[filtered_world_bank['indicator_id'] == 'FP.CPI.TOTL']['value']
    if float(inflation.iloc[0]) < 128.05:
        score += 3
    elif 128.05 <= float(inflation.iloc[0]) < 144:
        score += 2
    elif 144 <= float(inflation.iloc[0]) < 199.6:
        score += 1
    else:
        score += 0
    
    # Unemployment (% of total labor force)
    unemployment = filtered_world_bank[filtered_world_bank['indicator_id'] == 'SL.UEM.TOTL.ZS']['value']
    if float(unemployment.iloc[0]) < 3.6:
        score += 3
    elif 3.6 <= float(unemployment.iloc[0]) < 5.5:
        score += 2
    elif 5.5 <= float(unemployment.iloc[0]) < 8.4:
        score += 1
    else:
        score += 0
    
    # Income Share Held by Lowest 20%
    income_lowest_20 = filtered_world_bank[filtered_world_bank['indicator_id'] == 'SI.DST.FRST.20']['value']
    if float(income_lowest_20.iloc[0]) < 6.0:
        score += 0
    elif 6.0 <= float(income_lowest_20.iloc[0]) < 7.0:
        score += 1
    elif 7.0 <= float(income_lowest_20.iloc[0]) < 7.7:
        score += 2
    else:
        score += 3

    # GINI Index (World Bank estimate)
    gini_index = filtered_world_bank[filtered_world_bank['indicator_id'] == 'SI.POV.GINI']['value']
    if float(gini_index.iloc[0]) < 31.45:
        score += 3
    elif 31.45 <= float(gini_index.iloc[0]) < 35.1:
        score += 2
    elif 35.1 <= float(gini_index.iloc[0]) < 40.85:
        score += 1
    else:
        score += 0

    # Poverty Headcount Ratio at $2.15 a Day (2017 PPP) (% of population)
    poverty_headcount = filtered_world_bank[filtered_world_bank['indicator_id'] == 'SI.POV.DDAY']['value']
    if float(poverty_headcount.iloc[0]) < 0.3:
        score += 3
    elif 0.3 <= float(poverty_headcount.iloc[0]) < 1.7:
        score += 2
    elif 1.7 <= float(poverty_headcount.iloc[0]) < 14.6:
        score += 1
    else:
        score += 0

    # Education Expenditure (% of GDP)
    education_expenditure = filtered_world_bank[filtered_world_bank['indicator_id'] == 'SE.XPD.TOTL.GD.ZS']['value']
    if float(education_expenditure.iloc[0]) < 2.9:
        score += 0
    elif 2.9 <= float(education_expenditure.iloc[0]) < 4.15:
        score += 1
    elif 4.15 <= float(education_expenditure.iloc[0]) < 5.4:
        score += 2
    else:
        score += 3
    
    # Government Effectiveness: Estimate
    government_effectiveness = filtered_world_bank[filtered_world_bank['indicator_id'] == 'GE.EST']['value']
    if float(government_effectiveness.iloc[0]) < -0.25:
        score += 0
    elif -0.25 <= float(government_effectiveness.iloc[0]) < 0.094:
        score += 1
    elif 0.094 <= float(government_effectiveness.iloc[0]) < 1.24:
        score += 2
    else:
        score += 3
    
    # Control of Corruption: Estimate
    control_corruption = filtered_world_bank[filtered_world_bank['indicator_id'] == 'CC.EST']['value']
    if float(control_corruption.iloc[0]) < -1:
        score += 0
    elif -1 <= float(control_corruption.iloc[0]) < 0:
        score += 1
    elif 0 <= float(control_corruption.iloc[0]) < 1:
        score += 2
    else:
        score += 3
    
    return score 


def calculate_economic_index(df):
    """
    Assigns an economic stability index (1-5) based on total economic scores.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing a column 'total_economic_score'.

    Returns
    -------
    DataFrame
        The DataFrame with an additional column 'economic_index' containing values from 1 to 5.
    """
    df['economic_index'] = pd.qcut(df['total_economic_score'], q=5, labels=range(1, 6))
    df['economic_index'] = df['economic_index'].astype(int)
    
    return df


def iso3_to_iso2(iso_3):
    """
    This functions map """
    country = pycountry.countries.get(alpha_3=iso_3)
    return country.alpha_2 if country else None


def get_naturalearth_data(data_type="admin_0_countries", columns=["NAME", "geometry"]):
    """
    This function is copied from an online Jupyter Notebook and is used to plot the world map with GeoPandas. Refer to the sources section in the README.md file to learn more about this function.
    """
    naturalearth_url = "https://raw.githubusercontent.com/JetBrains/lets-plot-docs/master/" + \
                       "data/naturalearth/{0}/data.shp?raw=true".format(data_type)
    sf = shapefile.Reader(naturalearth_url)

    gdf = gpd.GeoDataFrame(
        [
            dict(zip([field[0] for field in sf.fields[1:]], record))
            for record in sf.records()
        ],
        geometry=[shape(s) for s in sf.shapes()]
    )[columns]
    gdf.columns = [col.lower() for col in gdf.columns]

    return gdf
