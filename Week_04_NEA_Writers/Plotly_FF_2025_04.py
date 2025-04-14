import polars as pl
import plotly.express as px
import plotly
print(f'{plotly.__version__ = }')

#-------------------------------------------------------------------------------
#  91.4% of data set comes from the United States; 7.5% listed as "No state or
#  country". This visualization uses USA data to show the median age of writers
#  by US State. 
#-------------------------------------------------------------------------------

# split long path name, PEP-8
source_file = 'https://raw.githubusercontent.com/plotly/Figure-Friday/refs/'
source_file += 'heads/main/2025/week-4/Post45_NEAData_Final.csv'

df = (
    pl.scan_csv(source_file)  # lazy frame
        .rename(
        {
            'family_name': 'LAST_NAME',
            'given_name_middle': 'FIRST_NAME',
            'us_state': 'STATE'
            }
        )
    .filter(pl.col('STATE').str.len_chars() == 2)
    .filter(pl.col('birth_year').is_not_null())
    .filter(pl.col('country') == 'USA')
    .with_columns(
        WRITERS_AGE = (pl.col('nea_grant_year') - pl.col('birth_year'))
    )
    .with_columns(
        STATE_COUNT = pl.col('STATE').count().over('STATE'),
        STATE_MEDIAN_AGE = pl.col('WRITERS_AGE').median().over('STATE'),
    )
    .select(
        pl.col(
            [
                'FIRST_NAME', 'LAST_NAME', 'WRITERS_AGE', 
                'STATE', 
                'STATE_COUNT', 
                'STATE_MEDIAN_AGE',
            ]
        )
    )
    .collect()   # lazy frame to polars dataframe
)

fig = px.scatter(
    df.sort('STATE_MEDIAN_AGE', descending=True),
    x='STATE',
    y='STATE_MEDIAN_AGE', 
    color='STATE',
    template='simple_white',
    height=500, width=1200,
    title='NEA WRITERS - MEDIAN AGE BY US STATES AND TERRITORIES',
   ) 
fig.update_traces(
    mode='lines+markers',
    line=dict(color='blue', width=5)
    )
fig.update_layout(showlegend=False)

# annotation positioned by 'paper', better to position by data or 'graph'
fig.add_annotation(
    text = 'North Dakota (median age - 57)', 
    x = 0.06, xref='paper', 
    y = 0.97, yref='paper',
    showarrow=False, align='left', 
)
# annotation positioned by 'paper', better to position by data or 'graph'
fig.add_annotation(
    text = 'Puerto Rico (median age - 36)', 
    x = 0.94, xref='paper', 
    y = 0.02, yref='paper',
    showarrow=False, align='left', 
)

fig.show()