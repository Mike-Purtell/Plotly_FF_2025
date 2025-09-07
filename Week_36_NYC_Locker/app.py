import polars as pl
import polars.selectors as cs
import os
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

# #----- GLOBALS -----------------------------------------------------------------
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_horizontal_thin_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 12}

style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold', 'color': 'gray'}
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal'}
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

#----- DASH COMPONENTS------ ---------------------------------------------------
dmc_select_borough = (
    dmc.MultiSelect(
        label='Pick 1 or more Boroughs',
        id='borough',
        data= ['Brooklyn', 'Manhattan', 'Queens'],
        value=['Brooklyn', 'Manhattan', 'Queens'],
        searchable=False,  # Enables search functionality
        clearable=True,   # Allows clearing the selection
        size='sm',
    ),
)

# #----- FUNCTIONS ---------------------------------------------------------------
def read_and_clean_csv():
    ''' read and clead data from csv, save as parquet '''
    print('reading and cleaning csv file')
    return(
        pl.scan_csv(
            'LockerNYC_Reservations_20250903.csv',
        )
        .with_columns(
            pl.col('Locker Size').fill_null('Small')
                .replace('S', 'Small')
                .replace('M', 'Medium')
                .replace('L', 'Large')
                .replace('XL', 'Extra Large'),
        )
        .rename(    
            lambda c: 
                c.upper()          # all column names to upper case
                .replace(' ', '_') # replace blanks with underscores
                .replace(r'(', '') # replace left parens with underscores
                .replace(r')', '') # replace left parens with underscores
        )
        .select(
            ['TYPE', 'DELIVERED', 'RECEIVED', 'LOCKER_NAME', 'LOCKER_SIZE',
            'LOCKER_BOX_DOOR', 'PICKUP_DURATION', 'DELIVERY_DURATION', 
            'LOCATION_TYPE', 'ADDRESS', 'LATITUDE', 'LONGITUDE', 'BOROUGH',
            'RECEIVE_DATE', 'CREATED_DATE', 'DELIVERY_DATE', 'WITHDRAW_DATE', 
            'EXPIRE_DATE']
        )
        .with_columns(
            cs.ends_with('_DATE').str.to_datetime(format="%m/%d/%Y %H:%M"),
            ZIP_CODE = pl.col('ADDRESS').str.split(' ').list.last(),
            RENTAL_COUNT = pl.col('ADDRESS').len().over('ADDRESS'),
        )
        .with_columns(ADDRESS = pl.col('ADDRESS').str.split(',').list.first())
        .with_columns(RENTAL_COUNT = pl.col('ADDRESS').count().over('ADDRESS'))
        .with_columns(
            LOCKER_COUNT = pl.col('ADDRESS').count().over(['ADDRESS','LOCKER_BOX_DOOR']))
        .collect()
    )

def get_scatter_map(borough):
    ''' return scatter map of dataset '''
    group_by_cols = [
        'LOCKER_NAME', 'ADDRESS', 'LOCATION_TYPE', 'LATITUDE', 'LONGITUDE', 
        'BOROUGH', 'ZIP_CODE', 'RENTAL_COUNT', 'LOCKER_COUNT'
    ]
    df_group_by = df_global.group_by(group_by_cols).len()

    if len(borough) < 3:
        df_group_by = (
            df_group_by
            .filter(pl.col('BOROUGH').is_in(borough))
        )
    print(df_group_by)

    fig_scatter_map = px.scatter_map(
        df_group_by,
        lat = 'LATITUDE',
        lon = 'LONGITUDE',
        color='BOROUGH',
        color_discrete_map=borough_color_map,
        custom_data=['BOROUGH', 'LOCKER_NAME', 'ADDRESS', 'ZIP_CODE', 
                     'LOCATION_TYPE', 'RENTAL_COUNT', 'LOCKER_COUNT'],
        size='RENTAL_COUNT',
        zoom=10,
        map_style = 'light'
    )
    hovertemplate = (
        '%{customdata[0]}<br>' + 
        '%{customdata[1]}<br>' + 
        '%{customdata[2]}, %{customdata[3]}<br>' + 
        '%{customdata[4]}<br>' + 
        '%{customdata[5]:,} Rentals<br>' + 
        '%{customdata[6]:,} Lockers<br>' + 
        '<extra></extra>')

    fig_scatter_map.update_traces(
        hovertemplate=hovertemplate,
        showlegend=False
    )

    # fig_scatter_map.update_traces(showlegend=False)
    return fig_scatter_map

def get_histogram(address):
    ''' return histogram locker sizes for specified address '''
    df_histo = (
        df_global
        .filter(pl.col('ADDRESS') == address)
        .sort('LOCKER_SIZE')
    )
    print(df_histo.glimpse())
    this_borough = ''
    try:
        this_borough = df_histo.item(0, 'BOROUGH')
    except:
        pass
    print(f'{this_borough = }')
    this_locker_name = ''
    try:
        this_locker_name = df_histo.item(0, 'LOCKER_NAME')
    except:
        pass
    print(f'{this_locker_name = }')

    fig_histo = px.histogram(
        df_histo,
        x='LOCKER_SIZE',
        # title=f'{this_locker_name}, {this_borough}<br><sup>RENTAL COUNT BY LOCKER SIZE'.upper(),
        
        title=(
            f'{this_locker_name}, {this_borough}'.upper() + '<br>' + 
            f'<sup>{address}<br>'.upper() +
            'RENTAL COUNT BY LOCKER SIZE' + '</sup>'
        ),
        
        template=viz_template
    )
    fig_histo.update_layout(
        yaxis_title = 'RENTAL_COUNT', xaxis_title = 'LOCKER SIZE', 
        xaxis=dict(
            categoryorder='array',  # Specify custom order
            categoryarray=['Small', 'Medium', 'Large', 'Extra Large'],  # Desired order of categories
        )
    )
    fig_histo.update_xaxes(showgrid=False)
    fig_histo.update_yaxes(showgrid=False)
    return fig_histo

def get_time_plot(address):
    ''' return histogram locker sizes for specified address '''
    df_time_plot = (
        df_global
        .filter(pl.col('ADDRESS') == address)
        .sort(['LOCKER_SIZE', 'DELIVERY_DATE'])
        .with_columns(RENTAL_COUNT = pl.col('LOCKER_SIZE').cum_count().over('LOCKER_SIZE')
        )
    )
    print(df_time_plot.glimpse())
    this_borough = ''
    try:
        this_borough = df_time_plot.item(0, 'BOROUGH')
    except:
        pass
    print(f'{this_borough = }')
    this_locker_name = ''
    try:
        this_locker_name = df_time_plot.item(0, 'LOCKER_NAME')
    except:
        pass
    print(f'{this_locker_name = }')

 
    size_list = df_time_plot.unique('LOCKER_SIZE').get_column('LOCKER_SIZE').to_list()
    print(f'{size_list = }')
    time_plot = go.Figure()
    for s in size_list:
        trace_color = size_color_map[s]
        print(f'{s} color is {trace_color}')
        df_size = df_time_plot.filter(pl.col('LOCKER_SIZE') ==  s).sort('DELIVERY_DATE')
        time_plot.add_trace(go.Line(
            x=df_size['DELIVERY_DATE'], y=df_size['RENTAL_COUNT'],
            name=s,
            mode='lines+markers',
            line=dict(color=trace_color, width=1),  # Set line color and width
            marker=dict(color=trace_color, size=3)  # Set line color and width
            )
        )
        max_y = df_size['RENTAL_COUNT'].to_list()[-1]
        print(f'{max_y = }')

        max_x = df_size['DELIVERY_DATE'].to_list()[-1]

        time_plot.add_annotation(
            text=s,
            xref='x',   x=max_x,  xanchor='left', xshift = 10,
            yref='y',   y=max_y,
            showarrow=False,
            font=dict(color= trace_color, size=16)
        )
    time_plot.update_layout(
        title=(
            f'{this_locker_name}, {this_borough}'.upper() + '<br>' + 
            f'<sup>{address}<br>'.upper() +
            'RENTAL COUNT TIMELINE BY LOCKER SIZE' + '</sup>'
        ),
        yaxis_title = 'CUMULATIVE RENTAL COUNT', xaxis_title = '', 
        template=viz_template,
        showlegend=False
    )
    time_plot.update_xaxes(showgrid=False)
    time_plot.update_yaxes(showgrid=False)
    return time_plot


#----- GATHER AND CLEAN DATA ---------------------------------------------------
if False:   # os.path.exists('df.parquet'):     # read parquet file if it exists
    print('reading data from parquet file')
    df_global=pl.read_parquet('df.parquet')
else:  # if no parquet file, read csv file, clean, save df as parquet
    df_global = read_and_clean_csv()
    df_global.write_parquet('df.parquet')

print(df_global)
print(df_global.glimpse())

# #----- DASH APPLICATION STRUCTURE---------------------------------------------
app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('NYC Locker Data', ta='center', style=style_h2),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_borough, span=4, offset = 1),
    ]),  
    dmc.Space(h=0),
    html.Hr(style=style_horizontal_thin_line),
    dmc.Grid(children = [
            dmc.GridCol(dcc.Graph(id='scatter-map'), span=6, offset=0),            
            
        ]),
    dmc.Grid(children = [
            dmc.GridCol(dcc.Graph(id='histo'), span=6, offset=0),            
            dmc.GridCol(dcc.Graph(id='time-plot'), span=6, offset=0), 
        ]),
])
# 2 call back design avoids infinite loop
# first call back reads borough selection and updates scatter map only
@app.callback(
    Output('scatter-map', 'figure'),

    Input('borough', 'value'),
)
def update_scatter(selected_borough_list):
    print(f'{selected_borough_list = }')
    if selected_borough_list is None:
        print('use first item on borough list')
        selected_borough_list = [borough_list[0]]
    if not isinstance(selected_borough_list, list):
        print('convert single selected borough to list')
        selected_borough_list = [borough_list[0]]
    scatter_map=get_scatter_map(selected_borough_list)
    return scatter_map

# 2 call back design avoids infinite loop
# second call back reads scatter hover values and updates histogram, time plot
@app.callback(
    Output('histo', 'figure'),
    Output('time-plot', 'figure'),
    Input('scatter-map', 'hoverData')
)
def update_histo(hover_info):
    print(f'{hover_info = }')
    address = '508  East 12th St'
    if hover_info is not None and 'points' in hover_info.keys():
        address = hover_info['points'][0]['customdata'][2]
        print(f'{address = }')
    else:
        print('key points not found')
    histogram = get_histogram(address)
    time_plot = get_time_plot(address)

    return histogram, time_plot

if __name__ == '__main__':
    app.run(debug=True)