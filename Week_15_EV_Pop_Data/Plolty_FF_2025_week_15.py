import polars as pl
import polars.selectors as cs
import plotly.express as px

# dataset issue:
#   99.8% of dataset is from Washington State. I dropped all other locations.
#   Base MSRP is 0 for 99.5% of the entries, dropped this too
#   Utility company info is long and messy, dropped it to
#   Vehicle Indentification Numbers (VIN) dropped, no interest
#   Range data is not credible with 2/3 of values as 0, dropped it too

df = (
    pl.scan_csv('Electric_Vehicle_Population_Data.csv')  # lazyframe
    .select(
        STATE = pl.col('State'),
        YEAR = pl.col('Model Year'),
        MAKE = pl.col('Make'),
    )
    .filter(pl.col('STATE') == 'WA')
    .filter(pl.col('MAKE') != 'TESLA')
    .select(
        'YEAR', 'MAKE', # 'MODEL',
        YEAR_MAKE_TOT = pl.col('STATE').count().over(['YEAR', 'MAKE']),
        YEAR_TOT = pl.col('STATE').count().over('YEAR'),
    )
    .unique(['YEAR', 'MAKE'])
    .with_columns(
        PCT_SHARE = 100*(pl.col('YEAR_MAKE_TOT')/pl.col('YEAR_TOT'))
    )
    .collect()  # convert to dataframe for pivot
    .pivot(
        on='MAKE',
        index='YEAR',
        values='PCT_SHARE'
    )
    .lazy()     # back to lazyframe
    .filter(pl.col('YEAR') > 2020)
    .sort('YEAR')
    .with_columns(
        PCT_TOT = pl.sum_horizontal(cs.exclude(pl.col('YEAR')))
    )
    .with_columns(
        PCT_TOT = pl.when(pl.col('PCT_TOT') >= 5)
                    .then('PCT_TOT')
                    .otherwise(pl.lit(0.0))
    )
    .with_columns(
        pl.col('YEAR').cast(pl.String)
    )
    .collect()  # lazyframe to data frame from here to the end of script
)

#----- EXCLUDE MAKES WITH 1 OR MORE NULL VALUES, ALPHABETIC SORT ---------------
sorted_cols_null_filter = sorted(
    [
        c for c in df.columns 
        if (df[c].is_null().sum() <= 1) and
        (c not in ['YEAR', 'PCT_TOT'])
    ]
)

#----- MAKE LIST OF TOP 5 MAKES in 2025 ----------------------------------------
top_5_makes = (
    df
    .select(['YEAR'] + sorted_cols_null_filter + ['PCT_TOT'] )
    .transpose(
        include_header=True,
        header_name='MAKE',
        column_names='YEAR',
    )
    .sort('2025', descending=True)
    .head(6)    # make a list of the top 6 makes
    .tail(5)    # then exclude the top brand
    .select('MAKE')
    .to_series()
    .to_list()
)

#----- COLOR DICTIONARY FOR THE GRAPHS AND ANNOTATED LABELS --------------------
my_color_dict = dict(
    zip(top_5_makes,px.colors.qualitative.Vivid[:5] )
)

#----- PLOT SHARE OF TOP 5 ALTERNATIVE EVS--------------------------------------
my_subtitle = (
    'Companies not led by ' +
    '<b>M</b>ostly <b>U</b>nhinged <b>S</b>pace <b>K</b>ings'
)
fig=px.line(
    df,
    'YEAR',
    top_5_makes,
    # Just found out from plotly youtube that markers can be enabled in px.line
    markers=True, 
    title="WASHINGTON STATE'S TOP 5 ALTERNATE ELECTRIC VEHICLES",
    subtitle=my_subtitle,
    template='simple_white',
    line_shape='spline',
    color_discrete_map=my_color_dict,
    height=500, width=800
)
for make in top_5_makes:
    fig.add_annotation(
            x=0.96, xref='paper',
            y=df[make].to_list()[-1], yref='y',
            text=f'<b>{make}</b>',
            showarrow=False,
            xanchor='left',
            font=dict(color=my_color_dict.get(make))
        )
    
data_source = 'https://catalog.data.gov/dataset/electric-vehicle-population-data'
fig.update_layout(
    showlegend=False,
    xaxis=dict(
        title=dict(text=f'Data source: {data_source}', font=dict(color='gray')),
    ),
    yaxis=dict(
        title=dict(
            text='ALTERNATE EV SHARE', 
            font=dict(color='gray'),
        ),
        ticksuffix = "%",
    ),
    hovermode='x unified'
)
fig.show()