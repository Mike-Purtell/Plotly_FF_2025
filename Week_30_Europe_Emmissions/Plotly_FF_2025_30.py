import polars as pl
pl.Config().set_tbl_cols(20)
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
import os
dash._dash_renderer._set_react_version('18.2.0')

#----- GLOBALS ------------- ---------------------------------------------------


style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal'}
map_styles = ['basic', 'carto-darkmatter', 'carto-darkmatter-nolabels', 
    'carto-positron', 'carto-positron-nolabels', 'carto-voyager', 
    'carto-voyager-nolabels', 'dark', 'light', 'open-street-map', 
    'outdoors', 'satellite', 'satellite-streets', 'streets', 'white-bg'
]

legend_font_size = 20
date_fmt ='%m/%d/%Y'

#----- GATHER AND CLEAN DATA ---------------------------------------------------
parquet_data_source = 'df.parquet'
if os.path.exists(parquet_data_source): # use pre-cleaned parquet file
    print(f'Reading data from {parquet_data_source}')
    df = pl.read_parquet(parquet_data_source)

else:  # read data from csv and clean
    csv_data_source = 'europe_monthly_electricity.csv' 
    print(f'Reading data from {csv_data_source}')
    df = (
        pl.read_csv(
            csv_data_source,
            )
        .select(
            COUNTRY = pl.col('Area'),
            ISO_3_CODE = pl.col('ISO 3 code'),
            YEAR = pl.col('Date')
                .str.to_date(format=date_fmt)
                .dt.year(),
            MONTH = pl.col('Date')
                .str.to_date(format=date_fmt)
                .dt.strftime("%b"),
            MONTH_NUM = pl.col('Date')
                .str.to_date(format=date_fmt)
                .dt.month(),
            DATE = pl.col('Date')
                .str.to_date(format=date_fmt),
            EU = pl.col('EU').cast(pl.Boolean),
            OECD = pl.col('OECD').cast(pl.Boolean),
            G20 = pl.col('G20').cast(pl.Boolean),
            G7 = pl.col('G7').cast(pl.Boolean),
            CAT = pl.col('Category').cast(pl.Categorical),
            SUBCAT = pl.col('Subcategory').cast(pl.Categorical),
            EMISSION = pl.col('Variable').cast(pl.Categorical),
            UNIT = pl.col('Unit').cast(pl.Categorical),
            VALUE = pl.col('Value'),
        )
        .drop_nulls(subset='ISO_3_CODE') 
        .filter(pl.col('YEAR') > 2014)   # data is parse prior to 2015
    )

#----- GLOBAL LISTS ------------------------------------------------------------
country_list = df.get_column('COUNTRY').unique().sort().to_list()
emission_list = df.get_column('EMISSION').unique().sort().to_list()

#----- FUNCTIONS----------------------------------------------------------------

def get_px_line(country, emission):
    df_local = (
        df
        .filter(pl.col('COUNTRY') == country)
        .filter(pl.col('EMISSION') == emission)
        .pivot(
            on='YEAR',
            values='VALUE',
            index=['MONTH', 'MONTH_NUM'],
            aggregate_function='sum'
        )
    )

    year_cols = df_local.columns[2:]
    fig = px.line(
        df_local,
        x='MONTH',
        y=year_cols,
        template='plotly_white',
        title=f'MONTHLY EMISSION OF {emission} from {country}'
    )
    return fig

#----- DASHBOARD COMPONENTS ----------------------------------------------------
dmc_select_country = (
    dmc.Select(
        label='Select County',
        placeholder="Select one",
        id='country',    # default value
        data=country_list,
        value='Austria',
        size='xl',
    ),
)
dmc_select_emission = (
    dmc.Select(
        label='Select Emission',
        placeholder="Select one",
        id='emission',    # default value
        data=emission_list,
        value='Demand',
        size='xl',
    ),
)

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('European Emissions Dashboard', ta='center', style=style_h2),
    dmc.Text('', ta='center', style=style_h3, id='zip_code'),
    html.Hr(style=style_horiz_line),
    dmc.Space(h=30),
    dmc.Grid(
        children = [
            dmc.GridCol(dmc_select_country, span=3, offset = 1),
            dmc.GridCol(dmc_select_emission, span=3, offset = 1),
        ]
    ),
    dmc.Space(h=10),
    dmc.Grid(  
        children = [dmc.GridCol(dcc.Graph(id='px_line'), span=8, offset=2),
        ]
    ),
])

# callback update px_scatter with selected country and emission
@app.callback(
    Output('px_line', 'figure'),
    Input('country', 'value'),
    Input('emission', 'value'),
)
def update(country, emission):
    px_line = get_px_line(country, emission)
    return px_line

if __name__ == '__main__':
    app.run(debug=True)