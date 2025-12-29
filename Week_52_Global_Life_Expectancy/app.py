import polars as pl
import polars.selectors as cs
import os
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc

#----- GLOBALS -----------------------------------------------------------------
root_file = 'live_expectancy_at_birth'
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}
style_horizontal_thin_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 12}
style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold', 'color': 'gray'}

#-----  FUNCTIONS --------------------------------------------------------------

def get_timeline_plot(df, plot_type, code_1, code_2, code_3):
    ''' Generate timeline plot based on filtered dataframe and plot type '''
    
    marker_size = 5
    line_width = 1
    y_axis_label = '' # initialize
    if plot_type == 'Raw Data':
        y_axis_label = 'Life Expectancy (Years)'
    elif plot_type == 'Norm Data':
        df = ( 
            df        
            .select(
                'YEAR',
                (cs.all().exclude('YEAR')-cs.all().exclude('YEAR').first()), # .name.suffix('_NORM')
            )
        )
        y_axis_label = 'Cumulative Change (Years)'
    elif plot_type == 'PCT Change':
        df = ( 
            df   
            .select(
                'YEAR',
                (  
                (cs.all().exclude('YEAR') - cs.all().exclude('YEAR').first()) 
                / cs.all().exclude('YEAR').first()
                )
            )
        )
        y_axis_label = 'Cumulative Change (%)'

    y_cols = [c for c in df.columns if c != 'YEAR']
    first_year = df['YEAR'].min()
    last_year = df['YEAR'].max()
    # Create figure with go.Scatter for each country
    import random
    fig = go.Figure()
    for col in y_cols:
        random_color = f'rgb({random.randint(0,255)},{random.randint(0,255)},{random.randint(0,255)})'
        fig.add_trace(
            go.Scatter(
                x=df['YEAR'],
                y=df[col],
                name=col,
                mode='lines+markers',
                line=dict(color=random_color,width=line_width),
                marker=dict(size=marker_size, color=random_color),
                zorder=0,
            )
        )
    fig.update_layout(
        title=f'Life Expectancy Timeline by Country, {first_year} to {last_year}',
        template='simple_white',
    )
    
    using_focus_countries = any([code_1, code_2, code_3])
    if using_focus_countries:
        # when focus countries are selected, gray out/de-emphasize all others
        print(f'Using focus countries: {using_focus_countries}')
        fig.update_traces(
            mode='lines',
            line=dict(color='lightgray'), 
            showlegend=False,
            hoverinfo='none',
            # hoverinfo=None, hover_label=None,
        )
        fig.update_layout(
            showlegend=True, 
            legend_title_text='Focus Country',
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='center',
                x=0.5
            ),
        )
        # marker_size = 10
        # line_width = 2
        if code_1:
            print(f'Adding country 1: {code_1}')
            fig.add_traces([
                go.Scatter(
                    x=df['YEAR'],
                    y=df[code_1],
                    name=get_country_name(code_1),
                    marker=dict(size=marker_size, color='red'),
                    line=dict(width=line_width, color='red', dash='solid'),
                    mode='lines+markers',
                    showlegend=True,
                    zorder=1,
                ),
            ])
        if code_2:
            print(f'Adding country 2: {code_2}')
            fig.add_traces([
                go.Scatter(
                    x=df['YEAR'],
                    y=df[code_2],
                    name=get_country_name(code_2),
                    marker=dict(size=marker_size, color='blue'),
                    line=dict(width=line_width, color='blue', dash='solid'),
                    mode='lines+markers',
                    showlegend=True,
                    zorder=1
                ),
            ])
        if code_3:
            print(f'Adding country 3: {code_3}')
            fig.add_traces([
                go.Scatter(
                    x=df['YEAR'],
                    y=df[code_3],
                    name=get_country_name(code_3),
                    marker=dict(size=marker_size, color='green'),  
                    line=dict(width=line_width, color='green', dash='solid'),
                    mode='lines+markers',
                    showlegend=True,
                    zorder=1
                ),
            ])

    fig.update_xaxes(
        showticklabels=True,
        ticks='',
        showline=True,
        title_text='',
        showgrid=True,
    )
    fig.update_yaxes(
        showticklabels=True,
        ticks='',
        showline=True,
        title_text=y_axis_label
    )
    return fig

def get_histogram(df) -> go.Figure:
    ''' Generate histogram of life expectancy '''
    df_melt = (
        df
        .with_columns(DECADE=(pl.col('YEAR').cast(pl.Utf8).str.slice(0, 3) + '0s'))
        .unpivot(on=cs.all().exclude(['YEAR', 'DECADE']),index='DECADE')
    )

    # Create figure with go.Histogram for each decade
    fig = go.Figure()
    decades = sorted(df_melt['DECADE'].unique().to_list())
    first_decade = decades[0]
    last_decade = decades[-1]
    # Define consistent bin edges for all histograms
    bin_start = 20
    bin_end = 95
    bin_size = (bin_end - bin_start) / 25
    for decade in decades:
        df_decade = df_melt.filter(pl.col('DECADE') == decade)
        fig.add_trace(
            go.Histogram(
                x=df_decade['value'],
                name=decade,
                xbins=dict(start=bin_start, end=bin_end, size=bin_size),
            )
        )
    fig.update_layout(
        title=f'Life Expectancy by Decade, {first_decade} to {last_decade}',
        xaxis_title='Life Expectancy (Years)',
        template='simple_white',
        barmode='stack',
        bargap=0.1,
    )
    fig.update_traces(opacity=0.5)
    fig.update_xaxes(
        showticklabels=True,
        ticks='',
        showline=True,
        title_text='',
        showgrid=True,
        range=[20, 90]
    )
    return fig

def get_boxplot(df) -> go.Figure:
    ''' Generate boxplot of life expectancy '''
    df_melt = (
        df
        .with_columns(DECADE=(pl.col('YEAR').cast(pl.Utf8).str.slice(0, 3) + '0s'))
        .unpivot(on=cs.all().exclude(['YEAR', 'DECADE']),index='DECADE')
    )

    # Create figure 
    fig = go.Figure()
    decades = sorted(df_melt['DECADE'].unique().to_list())
    first_decade = decades[0]
    last_decade = decades[-1]
    # Define consistent bin edges for all histograms
    for decade in decades:
        df_decade = df_melt.filter(pl.col('DECADE') == decade)
        values = df_decade['value'].drop_nulls()
        median_val = values.median() if len(values) > 0 else 0
        fig.add_trace(
            go.Box(
                y=df_decade['value'],
                name=decade,
                hoverinfo='skip',
                boxmean=True,
            )
        )
        # Add invisible scatter for custom hover showing only median
        fig.add_trace(
            go.Scatter(
                x=[decade],
                y=[median_val],
                mode='markers',
                marker=dict(size=0.1, opacity=0),
                showlegend=False,
                hovertemplate=f'{decade}<br>Median: {median_val:.1f}<extra></extra>',
            )
        )
    fig.update_layout(
        title=f'Life Expectancy by Decade, {first_decade} to {last_decade}',
        yaxis_title='Life Expectancy (Years)',
        template='simple_white',
    )
    fig.update_yaxes(
        showticklabels=True,
        ticks='',
        showline=True,
        title_text='',
        showgrid=True,
        range=[20, 90]
    )
    return fig


def get_choropleth(df) -> go.Figure:
    ''' Generate choropleth of median by country '''
    first_year = df['YEAR'].min()
    last_year = df['YEAR'].max()
    fig = go.Figure()
    df_transposed = (
        df
        .with_columns(pl.col('YEAR').cast(pl.String))
        .transpose(
            include_header=True, 
            column_names='YEAR', 
        )
        .with_columns(
            MEAN = pl.mean_horizontal(cs.float())
        )
        .select(
            COUNTRY_CODE = pl.col('column'),
            MEAN = pl.col('MEAN')
        )
        .join(
            df_country_codes, on='COUNTRY_CODE', how='left'
        )
    )

    map_projections = ['equirectangular', 'mercator', 'orthographic', 
        'natural earth', 'kavrayskiy7', 'miller', 'robinson', 'eckert4',
        'azimuthal equal area', 'azimuthal equidistant', 'conic equal area', 
        'conic conformal', 'conic equidistant', 'gnomonic', 'stereographic', 
        'mollweide', 'hammer', 'transverse mercator', 'albers usa', 
        'winkel tripel', 'aitoff'
    ]

    my_map_projection = 'winkel tripel'
    print(f'Using map projection: {my_map_projection}')
    
    fig = px.choropleth(
        df_transposed, 
        locations="COUNTRY_CODE",
        color="MEAN", 
        hover_name="COUNTRY_CODE", # column to add to hover information
        color_continuous_scale=px.colors.sequential.Plasma,
        custom_data=['COUNTRY_CODE', 'COUNTRY_NAME', 'MEAN'],
        title=f'Average Life Expectancy by Country, {first_year} to {last_year}',
        projection=my_map_projection,
        )
    
    fig.update_traces(
        hovertemplate=(
            '<b>%{customdata[0]}</b><br>' +   
            '<b>%{customdata[1]}</b><br>' +   
            '%{customdata[2]:.1f} years<extra></extra>'
        )
    )  
    return fig


def get_country_code(country_name: str) -> str:
    ''' Return country code for given country name '''
    code = (
        df_country_codes
        .filter(pl.col('COUNTRY_NAME') == country_name)
        .select('COUNTRY_CODE')
        .item(0, 'COUNTRY_CODE')
    )
    return code

def get_country_name(country_code: str) -> str:
    ''' Return country name for given country code '''
    name = (
        df_country_codes
        .filter(pl.col('COUNTRY_CODE') == country_code)
        .select('COUNTRY_NAME')
        .item(0, 'COUNTRY_NAME')
    )
    return name

#----- LOAD AND CLEAN DATA -----------------------------------------------------
if os.path.exists(root_file + '.parquet'):
# if False: # re-generates parquet from CSV
    print(f'{"*"*20} Reading {root_file}.parquet  {"*"*20}')
    df = pl.read_parquet(root_file + '.parquet')
  
else:
    print(f'{"*"*20} Reading {root_file}.csv  {"*"*20}')
    df = (
        pl.read_csv(root_file + '.csv', ignore_errors=True)
        .drop('Indicator Name')
        .rename({'Country Code': 'COUNTRY_CODE','Country Name':'COUNTRY_NAME'})
        .sort('COUNTRY_CODE')
    )
    df.write_parquet(root_file + '.parquet')

df_country_codes = (  # use in a join to map country codes to names
    df.select(cs.starts_with('COUNTRY_'))
)
df_transposed = (
    df
    .drop('COUNTRY_NAME')
    .transpose(
        include_header=True, 
        column_names='COUNTRY_CODE', 
        header_name ='YEAR'
    )
    .with_columns(YEAR = pl.col('YEAR').cast(pl.UInt16))
    .with_columns(cs.all().exclude('YEAR').cast(pl.Float32))
    )

#----- GLOBAL LISTS ------------------------------------------------------------
plot_types = ['Raw Data', 'Norm Data', 'PCT Change']
country_names = sorted(df_country_codes.unique('COUNTRY_NAME')['COUNTRY_NAME'].to_list())
country_codes = list(df_country_codes['COUNTRY_CODE'])
year_min = int(df_transposed['YEAR'].min())
year_max = int(df_transposed['YEAR'].max())

#----- DASH COMPONENTS------ ---------------------------------------------------
dcc_plot_type = (
    dmc.RadioGroup(
        children=dmc.Stack([dmc.Radio(label=pt, value=pt) for pt in plot_types]),
        value=plot_types[0],
        id='id_select_plot_type'
    )
)
dmc_year_range_slider = (
    dmc.RangeSlider(
        id='id_year_range_slider',
        value=[year_min, year_max],
        min=year_min,
        max=year_max,
        step=1,
        marks=[
            {'value': y, 'label': str(y)} 
            for y in range(year_min, year_max + 1) 
            if y % 5 == 0
        ],
    )
)
dcc_select_country_1 = (
    dcc.Dropdown(
        placeholder='Select Country(ies)', 
        options=['SKIP'] + country_names, # menu choices  
        value='SKIP', # initial value              
        style={'fontSize': '18px', 'color': 'black'},
        id='id_select_country_1'
    )
)
dcc_select_country_2 = (
    dcc.Dropdown(
        placeholder='Select Country(ies)', 
        options=['SKIP'] + country_names, # menu choices  
        value='SKIP', # initial value              
        style={'fontSize': '18px', 'color': 'black'},
        id='id_select_country_2'
    )
)
dcc_select_country_3 = (
    dcc.Dropdown(
        placeholder='Select Country(ies)', 
        options=['SKIP'] + country_names, # menu choices  
        value='SKIP', # initial value       
        style={'fontSize': '18px', 'color': 'black'},
        id='id_select_country_3'
    )
)
#----- DASH APPLICATION STRUCTURE ----------------------------------------------

app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Global Life Expectancy', ta='center', style=style_h2),
    dmc.Space(h=30),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Space(h=30),
    dmc.Grid(children =  [
        dmc.GridCol(dmc.Text('Timeline Source Data', ta='left'), span=2, offset=1),
        dmc.GridCol(dmc.Text(
            'Year Range Slider - Filters all visualizations', ta='left'), 
            span=6, offset=1
        ),
    ]),
    dmc.Space(h=10),
    dmc.Grid(
        children = [  
            dmc.GridCol(dcc_plot_type, span=2, offset=1),
            dmc.GridCol(dmc_year_range_slider, span=6, offset=1),
        ],
    ),
    dmc.Space(h=30),
        dmc.Grid(children =  [
        dmc.GridCol(dmc.Text('Timeline Focus Country 1', ta='left'), span=2, offset=1),
        dmc.GridCol(dmc.Text('Timeline Focus Country 2', ta='left'), span=2, offset=0),
        dmc.GridCol(dmc.Text('Timeline Focus Country 3', ta='left'), span=2, offset=0),
    ]),
    dmc.Grid(
        children = [  
            dmc.GridCol(dcc_select_country_1, span=2, offset=1),
            dmc.GridCol(dcc_select_country_2, span=2, offset=0),
            dmc.GridCol(dcc_select_country_3, span=2, offset=0),
        ],
    ),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='timeline_plot'), span=5,offset=1), 
        dmc.GridCol(dcc.Graph(id='choropleth'), span=5, offset=1),     
    ]),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='histogram'), span=5, offset=1),  
        dmc.GridCol(dcc.Graph(id='boxplot'), span=5,offset=1), 
    ]),
])
@app.callback(
    Output('timeline_plot', 'figure'),
    Output('histogram', 'figure'),
    Output('boxplot', 'figure'),
    Output('choropleth', 'figure'),
    Input('id_select_plot_type', 'value'),
    Input('id_year_range_slider', 'value'),
    Input('id_select_country_1', 'value'),
    Input('id_select_country_2', 'value'),
    Input('id_select_country_3', 'value'),
    )
def callback(selected_plot_type, year_range, country_1, country_2, country_3):
    code_1 = get_country_code(country_1) if country_1 != 'SKIP' else None
    code_2 = get_country_code(country_2) if country_2 != 'SKIP' else None
    code_3 = get_country_code(country_3) if country_3 != 'SKIP' else None
    df = (
        df_transposed
        .filter(pl.col('YEAR').is_between(year_range[0], year_range[1]))
    )
    timeline_plot=get_timeline_plot(
        df, selected_plot_type, code_1, code_2, code_3)
    histogram = get_histogram(df)
    boxplot = get_boxplot(df)
    choropleth = get_choropleth(df)
    return timeline_plot, histogram, boxplot, choropleth

if __name__ == '__main__':
    app.run(debug=True)