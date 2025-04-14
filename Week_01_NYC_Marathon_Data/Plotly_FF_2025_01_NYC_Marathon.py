'''
Plotly Figure Friday - 2025 week 01 - NYC Marathon 
The dataset has results of runners who finished the race. This notebook 
produces plots of median pace by age group, for 6 of the world's 7 continents. 
'''
import plotly.express as px
import polars as pl
import pycountry_convert as pc

def country_to_continent(country_name):
    ''' Use this function to get name of continent from country code'''
    country_alpha2 = pc.country_name_to_country_alpha2(country_name)
    country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
    country_continent_name = (
        pc.convert_continent_code_to_continent_name(country_continent_code)
    )
    return country_continent_name

df = (
    # pl.scan_csv produces a lazy frame
    pl.scan_csv('NYC Marathon Results, 2024 - Marathon Runner Results.csv')
    .select(pl.col(['age', 'gender', 'countryCode', 'pace']))
    .with_columns(
        CONTINENT = pl.col('countryCode')
            .map_elements(
                lambda x: country_to_continent(x),return_dtype=pl.String
                )
    )
    .drop_nulls('CONTINENT')
    .with_columns(
        AGE_GROUP = (pl.col('age')/10).cast(pl.UInt16).cast(pl.String) +'0s'
    )
    .with_row_index()
    .with_columns(PACE_SPLIT = pl.col('pace').str.split(':'))
    .with_columns(
        PACE_MINUTES = (pl.col('PACE_SPLIT').list.first().cast(pl.Float32))
    )
    .with_columns(
        PACE_SECONDS = (
            pl.col('PACE_SPLIT').list.slice(1).list.first().cast(pl.Float32) 
            )
    )
    .with_columns(
        PACE_FLOAT = (pl.col('PACE_MINUTES') + pl.col('PACE_SECONDS')/60.0)
    )
    .with_columns(
        PACE_MEDIAN = 
            pl.col('PACE_FLOAT')
            .median()
            .over(['AGE_GROUP', 'CONTINENT', 'gender'])
    )
    .select(pl.col(['CONTINENT', 'AGE_GROUP','PACE_MEDIAN', 'gender' ]))
    .unique(['CONTINENT', 'AGE_GROUP','gender'])
    .collect() # convert lazy frame to dataframe before the pivot
    .pivot(
        on = 'CONTINENT',
        index = ['AGE_GROUP', 'gender']
    )
    .sort('AGE_GROUP')
)

for gender in ['M', 'W', 'X']:
    df_gender = df.filter(pl.col('gender') == gender).drop('gender')
    fig =px.scatter(
        df_gender,
        'AGE_GROUP',
        ['Africa', 'Asia','Europe', 'North America','Oceania','South America'],
        template='simple_white',
        height=400, width=600
    )
    fig.update_layout(
        title=f'Average Pace, gender {gender}'.upper(),
        yaxis_title='Pace (Minutes per Mile)'.upper(),
        xaxis_title='Age group'.upper(),
        legend_title='Continent'
    )
    fig.update_traces(
        mode='lines+markers'
    )
    fig.show()
