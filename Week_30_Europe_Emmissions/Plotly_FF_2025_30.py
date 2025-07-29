import polars as pl
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
from dash_ag_grid import AgGrid
import os
dash._dash_renderer._set_react_version('18.2.0')

#----- GLOBALS ------------- ---------------------------------------------------
parquet_data_source = 'df.parquet'
csv_data_source = 'europe_monthly_electricity.csv' 
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
#----- GATHER AND CLEAN DATA ---------------------------------------------------
if os.path.exists(parquet_data_source): # use pre-cleaned parquet file
    print(f'Reading data from {parquet_data_source}')
    df = pl.read_parquet(parquet_data_source)

else:  # read data from csv and clean
    print(f'Reading data from {csv_data_source}')
    df = (
        pl.read_csv(
            csv_data_source,
            try_parse_dates=True
            )
        .select(
            COUNTRY = pl.col('Area'),
            ISO_3_CODE = pl.col('ISO 3 code'),
            YEAR = pl.col('Date').dt.year(),
            EU = pl.col('EU').cast(pl.Boolean),
            OECD = pl.col('OECD').cast(pl.Boolean),
            G20 = pl.col('G20').cast(pl.Boolean),
            G7 = pl.col('G7').cast(pl.Boolean),
            CAT = pl.col('Category').cast(pl.Categorical),
            SUBCAT = pl.col('Subcategory').cast(pl.Categorical),
            VARIABLE = pl.col('Variable').cast(pl.Categorical),
            UNIT = pl.col('Unit').cast(pl.Categorical),
            VALUE = pl.col('Value'),
        )
        .drop_nulls(subset='ISO_3_CODE') 
        .filter(pl.col('YEAR') > 2014)   # data is parse prior to 2015
    )

print(df)

#----- GLOBAL LISTS ------------------------------------------------------------
COUNTRIES = df.get_column('COUNTRY').unique().sort().to_list()
G7_COUNTRIES = (
    df
    .filter(pl.col('G7'))
    .get_column('COUNTRY')
    .unique()
    .sort()
    .to_list()
)
G20_COUNTRIES = (
    df
    .filter(pl.col('G20'))
    .get_column('COUNTRY')
    .unique()
    .sort()
    .to_list()
)
print(f'{COUNTRIES = }')
print(f'{G7_COUNTRIES = }')
print(f'{G20_COUNTRIES = }')

#----- FUNCTIONS----------------------------------------------------------------
def get_info_table(country):
    return (
        df
        .filter(pl.col('COUNTRY') ==  country)
        .transpose(
            include_header=True, header_name='ITEM'
        )
        .rename({'column_0':'VALUE'})
        .sort('ITEM')
    )

# def get_zip_info(zip_code):
#     return df_zip.filter(pl.col('ZIP')== zip_code)['ZIP_INFO'].item()

def get_zip_table():
    df_zip_codes =pl.DataFrame({
        'ZIP CODE:' : zip_code_list
    })
    row_data = df_zip_codes.to_dicts()
    column_defs = [{"headerName": col, "field": col} for col in df_zip_codes.columns]
    return (
        AgGrid(
            id='zip_code_table',
            rowData=row_data,
            columnDefs=column_defs,
            defaultColDef={"filter": True},
            columnSize="sizeToFit",
            getRowId='params.data.State',
            # HEIGHT IS SET TO HIGH VALUE TO SHOW MORE ROWS
            style={'height': '600px', 'width': '150%'},
        )
    )
def get_country_table(country):
    select_cols = [
        'COUNTRY', 'YEAR', 'EU', 'OECD', 'G20', 'G7', 
        'CAT', 'SUBCAT', 'VARIABLE', 'UNIT', 'VALUE'
    ]
    df_info = (
        df
        .select(select_cols)
        .filter(pl.col('COUNTRY') ==  country)
        .transpose(
            include_header=True, header_name='ITEM',
        )
        .rename({'column_0':'VALUE'})
    )

    # set specific column width, 1st col narrow, 2nd column wide
    column_defs = [
        {
            'field': 'ITEM', 
            'headerName': 'ITEM', 
            'width': 100, 
            'wrapText': True,
            'cellStyle': {
                'wordBreak': 'normal',  # ensures wrapping at word boundaries
                'lineHeight': 'unset',   # optional: removes extra spacing
            }
        },
        {
            'field': 'VALUE', 
            'headerName': 'VALUE', 
            'width': 300, 
            'wrapText': True,
            'cellStyle': {
                'wordBreak': 'normal',  # ensures wrapping at word boundaries
                'lineHeight': 'unset',   # optional: removes extra spacing
            }
        },
    ]
    return (
        AgGrid(
            id='info_table',
            rowData=df_info.to_dicts(),
            columnDefs=column_defs,
            defaultColDef={'sortable': False, 'filter': False, 'resizable': False},
            columnSize="sizeToFit",
            style={'height': '700px'},
        )
    )

def get_px_choropleth_map(country, this_map_style):
    ''' returns plotly map_libre, type streets, magenta_r sequential  '''
    # color_dict = {
    #         'Non-Residential'    : 'green',
    #         'Residential'       : 'blue'
    # }
    # df_map = ( # set COLOR TYPE, and filter by residential or non-residential
    #     df
    #     .filter(pl.col('COUNTRY') == country)
    # )

    fig = px.choropleth(
        df.unique('COUNTRY'),
        # opacity=0.5,
        locations='ISO_3_CODE',
        locationmode='ISO-3',
        scope='europe',
        # map_style = this_map_style,

        # color='TYPE',
        # color_discrete_map= color_dict,
        # color_continuous_scale='Magenta_r',
        # zoom=10,
        # custom_data=[
        #     'PROJECT_ID',         #  customdata[0]
        #     'ZIP',                #  customdata[1]
        #     'TYPE',               #  customdata[2]
        #     'EXIST_OR_NEW',       #  customdata[3]
        # ],
        height=800, width =800
    )
    fig.update_geos(showcountries=True)
    #----- APPLY HOVER TEMPLATE ------------------------------------------------
    # fig.update_traces(
    #     # hovertemplate =
    #     #     '<b>PROJECT_ID:</b> %{customdata[0]}<br>' + 
    #     #     '<b>ZIP:</b> %{customdata[1]}<br>' + 
    #     #     '<b>TYPE:</b> %{customdata[2]},<br>%{customdata[3]}<br>' + 
    #     #     '<extra></extra>',
    #     marker=dict(size=15)
    # )
    fig.update_layout(
        # hoverlabel=dict(
        #     bgcolor="white",
        #     font_size=16,
        #     font_family='courier',
        # ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5,
            font=dict(family='Arial', size=legend_font_size, color='black'),
            title=dict(
                text=f'<b>EMISSIONS OF {country}</b>',
                font=dict(family='Arial', size=legend_font_size, color='black'),
            )
        )
    )
    return fig

#----- DASHBOARD COMPONENTS ----------------------------------------------------
dmc_select_map_type = (
    dmc.Select(
        label='Map Style',
        placeholder="Select one",
        id='map_style',    # default value
        value='open-street-map',
        data=[{'value' :i, 'label':i} for i in map_styles],
        maxDropdownHeight=600,
        w=400,
        mb=10, 
        size='xl',
        style={'display': 'flex', 'alignItems': 'left', 'gap': '10px'},
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
            dmc.GridCol(dmc_select_map_type, span=4, offset = 4),
        ]
    ),
    dmc.Space(h=30),
    dmc.Grid(  
        children = [ 
            #  dmc.GridCol(get_country_table('Austria'), span=1, offset=1),
            dmc.GridCol(dcc.Graph(id='px_choropleth'), span=4, offset=1),
            #  dmc.GridCol(get_info_table('Austria'), span=3, offset=1)
        ]
    ),
])

# callback #1 update scatter_map, filtered with selected zip code
@app.callback(
    Output('px_choropleth', 'figure'),
    # Output('country', 'children'),
    # Input('country_table', 'cellClicked'),
    #  Input('zip_code_table', 'cellDoubleClicked'),
    Input('map_style', 'value'),
)
def update_map(map_style):
    country = COUNTRIES[0]  # default
    # if country is not None: # replace default if zip_code has data
    #     country = country['value']
    px_choropleth_map = get_px_choropleth_map(country, map_style)
    return px_choropleth_map # , f'{country}' # : {get_country_info(country)}'

# callback #2 update info table using hover data
# @app.callback(
#     Output('info_table', 'rowData'),
#     Input('px_choropleth_map', 'hoverData'),
# )
# def update_info_table(hover_data):
#     selected_id = df.sort('PROJECT_ID').item(0,'PROJECT_ID') # default
#     if hover_data is not None:  # replace default if hover_data has data
#         selected_id = hover_data['points'][0]['customdata'][0]
#     info_table_df = get_info_table_df(df, selected_id)
#     return info_table_df.to_dicts()

if __name__ == '__main__':
    app.run(debug=True)