import polars as pl
import polars.selectors as cs
import os
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
import dash_ag_grid as dag
dash._dash_renderer._set_react_version('18.2.0')
#  Dataset has 10 unique customers & locations, 92 unique customer/locatio pairs
#  dropped the HOUR and MINUTE fields, data grouped by ID, DATE, LONG/LAT
#----- LOAD AND CLEAN THE DATASET

#----- GLOBALS -----------------------------------------------------------------
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_horizontal_thin_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 12}

style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold', 'color': 'gray'}

viz_template = 'plotly_dark'

borough_list = ['Brooklyn', 'Manhattan', 'Queens']
borough_color_map = {
    'Brooklyn'    :  'red',     # 'CornflowerBlue',
    'Manhattan'   :  'navy',    # 'crimson',
    'Queens'      :  'green',   # 'Chartreuse',
}

size_color_map = {
    'Small'        :  '#00FFFF',  
    'Medium'       :  '#FF007F',  
    'Large'        :  '#00FF00',  
    'Extra Large'  :  '#FFD700', 
}
map_types = [
    'basic', 'carto-darkmatter', 'carto-darkmatter-nolabels', 'carto-positron', 
    'carto-positron-nolabels', 'carto-voyager', 'carto-voyager-nolabels', 
    'dark', 'light', 'open-street-map', 'outdoors', 'satellite', 
    'satellite-streets', 'streets', 'white-bg']


#----- DASH COMPONENTS------ ---------------------------------------------------
dmc_select_map_style = (
    dmc.Select(
        label='Select map style',
        id='map-style',
        data=map_types,
        value=map_types[0],
        searchable=True,  # Enables search functionality
        clearable=True,    # Allows clearing the selection
        size='sm',
    ),
)


df_locations = pl.read_excel('df_locations.xlsx')

if False: # 'df.parquet' in os.listdir('.'):
    print('reading dataset from parquet file')
    df = pl.read_parquet('df.parquet')
else:
    print('reading dataset from csv file')
    df  = (
        pl.scan_csv('pistes-cyclables-2024.csv')
        .select(
            ID = pl.col('id_compteur').cast(pl.UInt32),
            DATE = pl.col('date').str.to_date(format='%m/%d/%Y'),
            LON = pl.col('longitude').mean().over('id_compteur'),  # east-west location,   X
            LAT = pl.col('latitude').mean().over('id_compteur'),   # north-south location, Y
            PASSAGES = pl.col('nb_passages'),
        )
        .filter(pl.col('ID').is_not_null())
        .group_by(['ID', 'DATE','LON', 'LAT']).agg(pl.col('PASSAGES').sum())
        .with_columns(PASSAGES_BY_ID = pl.col('PASSAGES').sum().over('ID'))
        .with_columns(pl.col('PASSAGES').cast(pl.UInt16)) 
        .with_columns(pl.col('PASSAGES_BY_ID').cast(pl.UInt32)) 
        .sort(['ID', 'DATE'])
        .collect()
        .join(
            df_locations.select('ID', 'LOC', 'NEARBY'),
            on='ID',
            how='left'
        )
    )
    df.write_parquet('df.parquet')
print(df)
# create a dashboard to show:
#   slider to filter minimum passages value
# # Convert Polars DataFrame to a dictionary for Plotly
# heatmap_data = df.to_dict(as_series=False)

def get_scatter_map(map_style):
    # Create the scatter map
    # replaced midpoints of lat, long with median values to suppress outliers
    median_lat = df['LAT'].median()
    median_lon = df['LON'].median()
    fig = px.scatter_map(
        df.unique('ID'),
        lat='LAT', lon='LON',
        size='PASSAGES_BY_ID',
        color='PASSAGES_BY_ID', 
        zoom=11,
        center={'lat':median_lat, 'lon':median_lon},  
        map_style=map_style,
        opacity=0.75,
        custom_data=['LOC', 'NEARBY', 'PASSAGES_BY_ID', 'ID'],
    )
    fig.update_traces(
        hovertemplate =
            '%{customdata[0]}<br>' +
            'Nearby: %{customdata[1]}<br>' +
            'Passages: %{customdata[2]:,d}<br>' +
            'ID: %{customdata[3]}<br>' +
            '<extra></extra>'
    )
    fig.update_layout(
        title=dict(text='Bicycle traffic by location')
    )
    fig.update(layout_coloraxis_showscale=False)
    return fig

# #----- DASH APPLICATION STRUCTURE---------------------------------------------
app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Montreal Bicycle Traffic', ta='center', style=style_h2),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_map_style, span=2, offset = 1),
    ]),  
    # dmc.Space(h=30), 
    # dmc.Grid(children = [
    #     dmc.GridCol(card_borough, span=2, offset=0),
    #     dmc.GridCol(card_locker_name, span=2, offset=0),
    #     dmc.GridCol(card_address, span=2, offset=0),
    #     dmc.GridCol(card_location_type, span=2, offset=0),
    #     dmc.GridCol(card_rental_count, span=2, offset=0),
    #     dmc.GridCol(card_locker_count, span=2, offset=0),
    # ]),  
    # dmc.Space(h=30),
    # dmc.Space(h=0),
    # html.Hr(style=style_horizontal_thin_line),
    dmc.Grid(children = [
            dmc.GridCol(dcc.Graph(id='scatter-map'), span=6, offset=0),  
            # dmc.GridCol(dag.AgGrid(id='ag-grid'),span=5, offset=0),              
        ]),
    # dmc.Grid(children = [
    #         dmc.GridCol(dcc.Graph(id='histo'), span=6, offset=0),            
    #         dmc.GridCol(dcc.Graph(id='time-plot'), span=6, offset=0), 
    #     ]),
])
@app.callback(
    Output('scatter-map', 'figure'),
    Input('map-style', 'value'),
)
def callback(map_style):
    print(f'{map_style = }')
    # if selected_borough_list is None:
    #     print('use first item on borough list')
    #     selected_borough_list = [borough_list[0]]
    # if not isinstance(selected_borough_list, list):
    #     print('convert single selected borough to list')
    #     selected_borough_list = [borough_list[0]]
    scatter_map=get_scatter_map(map_style)
    return scatter_map

if __name__ == '__main__':
    app.run(debug=True)