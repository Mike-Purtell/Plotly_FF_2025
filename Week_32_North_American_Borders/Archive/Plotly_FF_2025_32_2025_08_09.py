import polars as pl
pl.Config().set_tbl_cols(10)
import polars.selectors as cs
import plotly.express as px
import dash_ag_grid as dag
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
import os
dash._dash_renderer._set_react_version('18.2.0')

#----- GLOBALS -----------------------------------------------------------------
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_horizontal_thin_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 12}

style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold'}
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal'}

parquet_data_source = 'Border_Crossing_Entry_Data.parquet'
csv_data_source = 'Border_Crossing_Entry_Data.csv' 
date_fmt ='%b-%y'

#----- Make dataframe with STATE name and abbreviation -------------------------
state_list = [
    'Alaska',  'Arizona', 'California', 'Idaho', 'Maine', 'Michigan',
    'Minnesota', 'Montana', 'New Mexico', 'New York', 'North Dakota', 'Texas',
    'Vermont', 'Washington'
    ]
state_abbr_list = [
    'AK', 'AZ', 'CA', 'ID', 'ME', 'MI',
    'MN', 'MT', 'NM', 'NY', 'ND', 'TX', 
    'VT', 'WA'
]
df_states = pl.DataFrame({  # used in join to add state abbreviations
    'STATE' :state_list,
    'STATE_ABBR' : state_abbr_list
})

zoom_level = {   # zoom level gives best view of all ports
    'Alaska'       : 3,
    'Arizona'      : 6, 
    'California'   : 7, 
    'Idaho'        : 9,  # large state, where ports are close to each other
    'Maine'        : 5, 
    'Michigan'     : 4,
    'Minnesota'    : 5, 
    'Montana'      : 5, 
    'New Mexico'   : 8, 
    'New York'     : 5, 
    'North Dakota' : 6, 
    'Texas'        : 4, # large state, where ports spread out
    'Vermont'      : 7, 
    'Washington'   : 5
}

#----- GATHER AND CLEAN DATA ---------------------------------------------------
if os.path.exists(parquet_data_source): # use pre-cleaned parquet if exists
    print(f'Reading data from {parquet_data_source}')
    df = (
        pl.scan_parquet(parquet_data_source)
        .with_columns(pl.col('LAT', 'LON').cast(pl.Float64))
        .collect()
    )

else:  # read data from csv and clean, and save to parquet
    print(f'Reading data from {csv_data_source}')
    df = (
        pl.scan_csv(csv_data_source,try_parse_dates=True)
        .rename(lambda c: c.upper()) # col names to upper case
        .select(
            PORT = pl.col('PORT NAME'),
            STATE = pl.col('STATE'),
            BORDER = pl.col('BORDER')
                .str.replace(' Border', ''),
            DATE = pl.col('DATE').str.to_date(format=date_fmt),
            VALUE = (pl.col('VALUE')* 1e-6).cast(pl.Float32),
            POINT = pl.col('POINT')   # get rid or parens and word POINT
                .str.replace_all('POINT ', '')
                .str.strip_chars('(')
                .str.strip_chars(')')
        )
        .filter(pl.col('VALUE') > 0.0)
        .with_columns(  # LONG and LAT in point column have better resolution
            LON = pl.col('POINT').str.split(' ').list.first().cast(pl.Float64),
            LAT = pl.col('POINT').str.split(' ').list.last().cast(pl.Float64),        
        )
        .drop('POINT')
        .drop_nulls(subset='STATE')
        .collect()
        .join(
            df_states,
            on='STATE',
            how='left'
        )
        .group_by(
            'BORDER', 'STATE', 'STATE_ABBR', 'PORT', 
            'DATE', 'LAT', 'LON', 
            )
        .agg(pl.col('VALUE').sum())
        .sort(['BORDER', 'STATE','PORT', 'DATE'])  
    )

    # define enum types, and apply to dataframe columns
    port_list = df.get_column('PORT').unique().sort()
    port_enum = pl.Enum(port_list)
    state_enum = pl.Enum(state_list)
    state_abbr_enum = pl.Enum(state_abbr_list)
    border_enum = pl.Enum(['US-Canada', 'US-Mexico'])
    df = (
        df
        .lazy()
        .with_columns(
            STATE=pl.col('STATE').cast(state_enum),
            STATE_ABBR=pl.col('STATE_ABBR').cast(state_abbr_enum),
            PORT=pl.col('PORT').cast(port_enum),
            BORDER = pl.col('BORDER').cast(border_enum)
        )
        .collect()
    )
    df.write_parquet(parquet_data_source)
    df.write_excel('df.xlsx')

# #----- DASH COMPONENTS -------------------------------------------------------
dmc_select_group_by = (
    dmc.Select(
        label='Group By',
        placeholder="Select one",
        id='group-by',
        data=['BORDER', 'STATE', 'PORT'],
        value='BORDER',
        size='xl',
    ),
)

fig_us_choro = px.choropleth(
    df
        .group_by('STATE', 'STATE_ABBR')
        .agg(pl.col('VALUE')
        .sum()),
    locations='STATE_ABBR', 
    locationmode='USA-states', 
    scope='usa', # 'north america',
    color='VALUE',
    template='plotly_white',
    color_continuous_scale='Viridis_r',
    title='Total Value of Goods',
    custom_data=[
            'STATE',         # customdata[0]
            'VALUE',         # customdata[1]
    ],
   
)
fig_us_choro.update_traces(
    hovertemplate =
        '%{customdata[0]}<br>' + 
        '%{customdata[1]:.3f} M$<br>' + 
        '<extra></extra>'
)
fig_us_choro.update(layout_coloraxis_showscale=False)
# fig_us_choro.update_layout(coloraxis_colorbar_x=0,)
# fig_us_choro.update_coloraxes(colorbar_ticklabelposition='outside left')

# #----- FUNCTIONS ---------------------------------------------------------------
def get_state_port_map(selected_state):
    ''' returns scatter_map of selected state with ports '''
    df_state = (
        df
        .filter(pl.col('STATE') == selected_state)
        .group_by('STATE', 'STATE_ABBR', 'PORT', 'LAT', 'LON')
        .agg(pl.col('VALUE').sum())
        .sort('PORT')
    )

    # Calculate bounding box
    min_lat, max_lat = df_state['LAT'].min(), df_state['LAT'].max()
    min_lon, max_lon = df_state['LON'].min(), df_state['LON'].max()

    # Calculate center
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2

    # Create a scatter map
    fig_state_port_map = px.scatter_map(
        df_state,
        lat='LAT',
        lon='LON',
        text='PORT',
        center={'lat':center_lat, 'lon':center_lon},
        zoom=zoom_level[selected_state],
        # color='VALUE',
        # color_continuous_scale='Viridis_r',
        # color_continuous_scale='algae',
        title=f'Border Crossings in {selected_state}',
        map_style='basic',  # Use MapLibre-compatible tile style
        opacity=0.75,
        # height=600, width=600
    )
    fig_state_port_map.update_traces(marker={'size': 20})
    fig_state_port_map.update(layout_coloraxis_showscale=False)
    return fig_state_port_map

# #----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server
app.title = 'US Border Crossings, Value of Goods'

app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('US Border Crossings, Value of Goods', ta='center', style=style_h2),
    # dmc.Text('', ta='center', style=style_h3, id='zip_code'),
    html.Hr(style=style_horizontal_thick_line),
#     dmc.Space(h=30),
#     dmc.Grid(children = [
#         dmc.GridCol(dmc.Text('Dashboard Control Panel', 
#             fw=500, # semi-bold
#             style={'fontSize': 28},
#             ),
#             span=3, offset=1
#         )]
#     ),
#     dmc.Space(h=30),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_group_by, span=2, offset = 1),
    ]),  
    dmc.Grid(  
        children = [
            dmc.GridCol(
                dcc.Graph(
                    figure=fig_us_choro,
                    id='us-choro'
                ), 
                span=6, offset=0
            ),
            # dmc.GridCol(dcc.Graph(id='histogram'), span=4, offset=0), 
            # dmc.GridCol(dag.AgGrid(id='ag-grid'),span=3, offset=0),           
        ]
    ),
    dmc.Grid(  
        children = [
            dmc.GridCol(dcc.Graph(id='state-port-map'), span=6, offset=0),
            # dmc.GridCol(dcc.Graph(id='histogram'), span=4, offset=0), 
            # dmc.GridCol(dag.AgGrid(id='ag-grid'),span=3, offset=0),           
        ]
    ),
])

@app.callback(
    Output('state-port-map', 'figure'),
    Input('us-choro', 'hoverData'),
)
def update(selected_state):
    if selected_state is None:
        selected_state = 'California'
    else:
        selected_state = selected_state['points'][-1]['customdata'][0]
    state_port_map = get_state_port_map(selected_state)
    return (
        state_port_map 
    )

if __name__ == '__main__':
    app.run_server(debug=True)