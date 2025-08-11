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
# ---- NOTES ABOUT THIS DATASET ------------------------------------------------
# California ports Calexico and Calexico East are at the same location. I merged
#     these to be noted as Calexico
# Maine ports Portland and Bar Harbor are not at or near the Canadien Border
# Seasonal variation is much greater with Canada than with Mexico
# Effect of Covid pandemic is very easy to see in these graphs.
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
fig_template = 'presentation'

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
    'Idaho'        : 9, # large state, where ports close to each other
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
# if False: # use this to force csv file as the data source
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
            PORT = pl.col('PORT NAME').str.replace('Calexico East', 'Calexico'),
            STATE = pl.col('STATE'),
            BORDER = pl.col('BORDER')
                .str.replace(' Border', ''),
            DATE = pl.col('DATE').str.to_date(format=date_fmt),
            VALUE = (pl.col('VALUE').cast(pl.Float32)* 1e-6),
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
        .with_columns(
            PORT_STATE = pl.col('PORT') + 
                pl.lit(', ') + 
                pl.col('STATE_ABBR')
        )
        .group_by(
            'BORDER', 'STATE', 'STATE_ABBR', 'PORT', 'PORT_STATE',
            'DATE', 'LAT', 'LON', 
            )
        .agg(pl.col('VALUE').sum())
        .sort(['BORDER', 'STATE','PORT', 'DATE'])  
    )

    # define enum types, and apply to dataframe columns
    port_list = df.get_column('PORT').unique().sort()
    port_enum = pl.Enum(port_list)
    port_state_list = df.get_column('PORT_STATE').unique().sort()
    port_state_enum = pl.Enum(port_state_list)
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
            PORT_STATE=pl.col('PORT_STATE').cast(port_state_enum),
            BORDER = pl.col('BORDER').cast(border_enum)
        )
        .collect()
    )
    df.write_parquet(parquet_data_source)
    df.write_excel('df.xlsx')
    print(df)

# #----- DASH COMPONENTS -------------------------------------------------------
dmc_select_group_by = (
    dmc.Select(
        label='Group By',
        placeholder='Select one',
        id='group-by',
        data=['BORDER', 'STATE', 'PORT (TOP 10)', 'PORT (BOTTOM 10)'],
        value='BORDER',
        size='xl',
    ),
)

fig_choro = px.choropleth(
    df
        .group_by('STATE', 'STATE_ABBR')
        .agg(pl.col('VALUE')
        .sum()),
    locations='STATE_ABBR', 
    locationmode='USA-states', 
    scope='usa', # 'north america',
    color='VALUE',
    template=fig_template,
    color_continuous_scale='Viridis_r',
    title=(
        'Total Value of Goods<br>' +
        '<sup>Hover over state filters Port Map</sup>'
    ),
    custom_data=[
        'STATE',         # customdata[0]
        'VALUE',         # customdata[1]
    ],
)
fig_choro.update_traces(
    hovertemplate =
        '%{customdata[0]}<br>' + 
        'Value(M$): %{customdata[1]:,.2f}<br>' + 
        '<extra></extra>'
)
fig_choro.update(layout_coloraxis_showscale=False)

# #----- FUNCTIONS ---------------------------------------------------------------
def get_state_port_map(selected_state):
    ''' returns scatter_map of selected state with ports '''
    df_state = (
        df
        .filter(pl.col('STATE') == selected_state)
        .group_by('STATE', 'STATE_ABBR', 'PORT', 'PORT_STATE', 'LAT', 'LON')
        .agg(pl.col('VALUE').sum())
        .sort('PORT')
    )
    print('df_state')
    print(df_state)
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
        text='PORT_STATE',
        center={'lat':center_lat, 'lon':center_lon},
        zoom=zoom_level[selected_state],
        title=(f'Port Map of {selected_state}'),
        map_style='basic',  # Map Libre
        opacity=0.75,
        template=fig_template,
        custom_data=[
            'PORT_STATE',    # customdata[0]
            'VALUE',         # customdata[0]
        ],
    )
    fig_state_port_map.update_traces(
        hovertemplate =
            '%{customdata[0]}<br>' + 
            'Total Value (M$): %{customdata[1]:.2f}<br>' + 
            '<extra></extra>'
    )
    fig_state_port_map.update_traces(marker={'size': 20})
    fig_state_port_map.update(layout_coloraxis_showscale=False)
    return fig_state_port_map

def get_line_group_by(group_by):
    ''' returns px.line plot of value by selected group_by '''
    if group_by in ['BORDER', 'STATE']:
        df_group_by = (
            df
            .group_by('DATE', group_by)
            .agg(pl.col('VALUE').sum())
            .sort(group_by)
        )
        custom_col = group_by
    else: # by port, top 10 or bottom 10
        custom_col='PORT_STATE'
        if 'TOP' in group_by:
            ten_port_list = (
                df
                .group_by('PORT_STATE')
                .agg(pl.col('VALUE'))
                .sort('VALUE', descending=True)
                .head(10)
                .get_column('PORT_STATE')
                .to_list()
            )
        if 'BOTTOM' in group_by:
            ten_port_list = (
                df
                .group_by('PORT_STATE')
                .agg(pl.col('VALUE'))
                .sort('VALUE', descending=True)
                .tail(10)
                .get_column('PORT_STATE')
                .to_list()
            )
        df_group_by = (
            df
            .filter(pl.col('PORT_STATE').is_in(ten_port_list))
            .group_by('DATE', 'PORT_STATE')
            .agg(pl.col('VALUE').sum())
            .sort('PORT_STATE')
        )
        print(f'{ten_port_list = }')
   
    color_by = group_by
    if 'PORT' in group_by:
        color_by = 'PORT_STATE'
    fig_line_group_by = px.line(
        df_group_by.sort('DATE'),
        x='DATE',
        y='VALUE',
        color=color_by,
        template=fig_template,
        markers=True,
        line_shape='spline',
        custom_data=[
             custom_col,  # customdata[0]
            'DATE',       # customdata[1]
            'VALUE',      # customdata[2]
        ],
    )
    fig_line_group_by.update_traces(
        hovertemplate =
            '%{customdata[0]}<br>' + 
            'Date: %{customdata[1]}<br>' + 
            'Value (M$): %{customdata[2]:.2f}<br>' + 
            '<extra></extra>',
        line={'width': 1},
        marker={'size': 2},
    )
    legend_title=group_by
    if 'PORT' in group_by:
        legend_title = 'PORT'
    fig_line_group_by.update_layout(
        title= f'Value of Goods by {group_by}',
        xaxis_title='',  # x-axis is for date, label is obvious, keep blank
        yaxis_title='VALUE (M$)',
        legend_title_text=f'{legend_title}'
    )
    return fig_line_group_by

def get_state_ports(selected_state):
    ''' returns px.line plot of value by selected group_by '''
    df_state = (
        df
        .filter(pl.col('STATE') == selected_state)
        .group_by('PORT_STATE','PORT', 'DATE')
        .agg(pl.col('VALUE').sum())
        .sort('PORT_STATE', 'DATE')
    )

    fig_state_ports = px.line(
        df_state,
        x='DATE',
        y='VALUE',
        color='PORT',
        template=fig_template,
        markers=True,
        line_shape='spline',
        custom_data=[
            'PORT_STATE',   # customdata[0]
            'DATE',         # customdata[1]
            'VALUE',        # customdata[2]
        ],
    )
    fig_state_ports.update_traces(
        hovertemplate =
            '%{customdata[0]}<br>' + 
            'Date: %{customdata[1]}<br>' + 
            'Value (M$): %{customdata[2]:.2f}<br>' + 
            '<extra></extra>',
        line={'width': 1},
        marker={'size': 2},
    )
    fig_state_ports.update_traces(
        line={'width': 1},
        marker={'size': 2},
    )
    fig_state_ports.update_layout(
        title= f'Value of Goods, {selected_state}',
        xaxis_title='',  # x-axis is for date, label is obvious, keep blank
        yaxis_title='VALUE (M$)',
        legend_title_text=f'{selected_state} Port'
    )
    return fig_state_ports

# #----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server
app.title = 'US Border Crossings, Value of Goods'

app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('US Border Crossings, Value of Goods', ta='center', style=style_h2),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_group_by, span=3, offset = 7),
    ]),  
    dmc.Grid(  
        children = [
            dmc.GridCol(
                dcc.Graph(
                    figure=fig_choro,
                    id='choro'
                ), 
                span=6, offset=0
            ),
            dmc.GridCol(
                dcc.Graph(
                    id='line-group-by'
                ), 
                span=6, offset=0
            ),          
        ]
    ),
    dmc.Grid(  
        children = [
            dmc.GridCol(dcc.Graph(id='state-port-map'), span=6, offset=0),
            dmc.GridCol(
                dcc.Graph(
                    id='state-port-data'
                ), 
                span=6, offset=0
            ),          
        ]
    ),
])

@app.callback(
    Output('state-port-map', 'figure'),
    Output('line-group-by', 'figure'),
    Output('state-port-data', 'figure'),
    Input('choro', 'hoverData'),
    Input('group-by', 'value'),
)
def update(selected_state, group_by):
    print(f'{selected_state = }')
    print(f'{group_by = }')

    if selected_state is None:
        selected_state = 'California'
    else:
        selected_state = selected_state['points'][-1]['customdata'][0]
    state_port_map = get_state_port_map(selected_state)
    line_group_by = get_line_group_by(group_by)
    px_line_state_ports = get_state_ports(selected_state)
    return state_port_map, line_group_by, px_line_state_ports

if __name__ == '__main__':
    app.run_server(debug=True)