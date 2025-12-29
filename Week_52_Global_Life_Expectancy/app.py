""" 
Global Life Expectancy Dashboard
================================
An interactive Dash application visualizing life expectancy data across countries and decades.

Features:
- Timeline plot showing life expectancy trends by country (raw, normalized, or % change)
- Focus country highlighting with up to 3 countries
- Stacked histogram showing distribution by decade
- Box plot comparing decades
- Choropleth map showing average life expectancy by country

Data Source: World Bank life expectancy at birth dataset
Date: December 2025
"""

# Data manipulation
import polars as pl
import polars.selectors as cs
import os

# Visualization
import plotly.express as px
import plotly.graph_objects as go

# Dashboard framework
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc

#----- GLOBALS -----------------------------------------------------------------
# CSS styles for consistent UI elements throughout the dashboard
root_file = 'live_expectancy_at_birth'  # Base filename for data (CSV/parquet)

# Decorative horizontal line styles (blue-to-orange gradient)
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}
style_horizontal_thin_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 12}

# Title text style
style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold', 'color': 'gray'}

#-----  VISUALIZATION FUNCTIONS ------------------------------------------------
# Each function generates a specific Plotly figure for the dashboard

def get_timeline_plot(df, plot_type, code_1, code_2, code_3):
    '''
    Generate a multi-line timeline plot showing life expectancy over time.
    
    Args:
        df: Polars DataFrame with YEAR column and country code columns
        plot_type: 'Raw Data', 'Norm Data', or 'PCT Change'
        code_1, code_2, code_3: Optional country codes to highlight (or None)
    
    Returns:
        Plotly Figure object
    
    When focus countries are selected, all other countries are grayed out
    and the focus countries are highlighted in red, blue, and green.
    '''
    # Visual parameters for traces
    marker_size = 5
    line_width = 1
    y_axis_label = ''  # Will be set based on plot_type
    # Transform data based on selected plot type
    if plot_type == 'Raw Data':
        # Show actual life expectancy values
        y_axis_label = 'Life Expectancy (Years)'
    elif plot_type == 'Norm Data':
        # Normalize: subtract first year's value from all years
        df = ( 
            df        
            .select(
                'YEAR',
                (cs.all().exclude('YEAR')-cs.all().exclude('YEAR').first()),
            )
        )
        y_axis_label = 'Cumulative Change (Years)'
    elif plot_type == 'PCT Change':
        # Percentage change relative to first year
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

    # Deterministic color palette
    color_palette = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
        '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
        '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5'
    ]
    # Create figure with go.Scatter for each country
    fig = go.Figure()
    for i, col in enumerate(y_cols):
        color = color_palette[i % len(color_palette)]
        fig.add_trace(
            go.Scatter(
                x=df['YEAR'],
                y=df[col],
                name=col,
                mode='lines+markers',
                line=dict(color=color, width=line_width),
                marker=dict(size=marker_size, color=color),
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
                    hovertemplate='%{fullData.name}: %{y:.1f}<extra></extra>',
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
                    zorder=1,
                    hovertemplate='%{fullData.name}: %{y:.1f}<extra></extra>',
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
                    zorder=1,
                    hovertemplate='%{fullData.name}: %{y:.1f}<extra></extra>',
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
    '''
    Generate a stacked histogram showing life expectancy distribution by decade.
    
    Args:
        df: Polars DataFrame with YEAR column and country code columns
    
    Returns:
        Plotly Figure with stacked histograms, one per decade
    
    Data is unpivoted (melted) to create a long-format DataFrame with
    DECADE and value columns for histogram binning.
    '''
    # Reshape data: add DECADE column and unpivot country columns to rows
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
    '''
    Generate vertical box plots comparing life expectancy distribution by decade.
    
    Args:
        df: Polars DataFrame with YEAR column and country code columns
    
    Returns:
        Plotly Figure with one box per decade, plus invisible scatter
        points for custom median-only hover display
    
    Box plots show median, quartiles, and outliers. Mean line is also displayed.
    '''
    # Reshape data: add DECADE column and unpivot country columns to rows
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
                boxmean=True,
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
    '''
    Generate a world choropleth map showing average life expectancy by country.
    
    Args:
        df: Polars DataFrame with YEAR column and country code columns
    
    Returns:
        Plotly Figure with choropleth map using Winkel Tripel projection
    
    Data is transposed back to country rows, then average life expectancy
    is calculated across all years for each country.
    '''
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

    fig = px.choropleth(
        df_transposed, 
        locations="COUNTRY_CODE",
        color="MEAN", 
        hover_name="COUNTRY_CODE", # column to add to hover information
        color_continuous_scale=px.colors.sequential.Plasma,
        custom_data=['COUNTRY_CODE', 'COUNTRY_NAME', 'MEAN'],
        title=f'Average Life Expectancy by Country, {first_year} to {last_year}',
        projection='winkel tripel',
        )
    
    fig.update_traces(
        hovertemplate=(
            '<b>%{customdata[0]}</b><br>' +   
            '<b>%{customdata[1]}</b><br>' +   
            '%{customdata[2]:.1f} years<extra></extra>'
        )
    )  
    return fig


#----- HELPER FUNCTIONS --------------------------------------------------------
# Utility functions for country code/name lookups

def get_country_code(country_name: str) -> str:
    '''Look up 3-letter country code from full country name.'''
    code = (
        df_country_codes
        .filter(pl.col('COUNTRY_NAME') == country_name)
        .select('COUNTRY_CODE')
        .item(0, 'COUNTRY_CODE')
    )
    return code

def get_country_name(country_code: str) -> str:
    '''Look up full country name from 3-letter country code.'''
    name = (
        df_country_codes
        .filter(pl.col('COUNTRY_CODE') == country_code)
        .select('COUNTRY_NAME')
        .item(0, 'COUNTRY_NAME')
    )
    return name

#----- LOAD AND CLEAN DATA -----------------------------------------------------
# Load data from parquet (fast) if available, otherwise from CSV
# Parquet is cached after first CSV read for faster subsequent loads

if os.path.exists(root_file + '.parquet'):
# if False:  # Uncomment to force re-read from CSV
    print(f'{"*"*20} Reading {root_file}.parquet  {"*"*20}')
    df = pl.read_parquet(root_file + '.parquet')
  
else:
    print(f'{"*"*20} Reading {root_file}.csv  {"*"*20}')
    df = (
        pl.read_csv(root_file + '.csv', ignore_errors=True)
        .drop('Indicator Name')  # Not needed
        .rename({'Country Code': 'COUNTRY_CODE','Country Name':'COUNTRY_NAME'})
        .sort('COUNTRY_CODE')
    )
    df.write_parquet(root_file + '.parquet')  # Cache for next time

# Lookup table: country code <-> country name mapping
df_country_codes = (
    df.select(cs.starts_with('COUNTRY_'))
)

# Transpose data: rows become years, columns become country codes
# This format is needed for timeline plotting (one trace per country)
df_transposed = (
    df
    .drop('COUNTRY_NAME')
    .transpose(
        include_header=True, 
        column_names='COUNTRY_CODE', 
        header_name='YEAR'
    )
    .with_columns(YEAR=pl.col('YEAR').cast(pl.UInt16))  # Year as integer
    .with_columns(cs.all().exclude('YEAR').cast(pl.Float32))  # Values as float
)

#----- GLOBAL LISTS ------------------------------------------------------------
# Options and bounds derived from data, used by UI components

plot_types = ['Raw Data', 'Norm Data', 'PCT Change']  # Timeline view options
country_names = sorted(df_country_codes.unique('COUNTRY_NAME')['COUNTRY_NAME'].to_list())  # Dropdown options
country_codes = list(df_country_codes['COUNTRY_CODE'])
year_min = int(df_transposed['YEAR'].min())  # Slider bounds
year_max = int(df_transposed['YEAR'].max())

#----- DASH COMPONENTS ---------------------------------------------------------
# Reusable UI components defined separately for cleaner layout code

# Radio buttons for selecting timeline data transformation
dcc_plot_type = (
    dmc.RadioGroup(
        children=dmc.Stack([dmc.Radio(label=pt, value=pt) for pt in plot_types]),
        value=plot_types[0],  # Default: Raw Data
        id='id_select_plot_type'
    )
)

# Year range slider - filters ALL visualizations
dmc_year_range_slider = (
    dmc.RangeSlider(
        id='id_year_range_slider',
        value=[year_min, year_max],  # Default: full range
        min=year_min,
        max=year_max,
        step=1,
        marks=[  # Show labels every 5 years
            {'value': y, 'label': str(y)} 
            for y in range(year_min, year_max + 1) 
            if y % 5 == 0
        ],
    )
)

# Focus country dropdowns (3 slots for highlighting specific countries)
dcc_select_country_1 = (
    dcc.Dropdown(
        placeholder='Select Country', 
        options=['SKIP'] + country_names,  # 'SKIP' = no selection
        value='SKIP',
        style={'fontSize': '18px', 'color': 'black'},
        id='id_select_country_1'
    )
)
dcc_select_country_2 = (
    dcc.Dropdown(
        placeholder='Select Country', 
        options=['SKIP'] + country_names,
        value='SKIP',
        style={'fontSize': '18px', 'color': 'black'},
        id='id_select_country_2'
    )
)
dcc_select_country_3 = (
    dcc.Dropdown(
        placeholder='Select Country', 
        options=['SKIP'] + country_names,
        value='SKIP',
        style={'fontSize': '18px', 'color': 'black'},
        id='id_select_country_3'
    )
)
#----- DASH APPLICATION STRUCTURE ----------------------------------------------
# Main application layout using Dash Mantine Components grid system

app = Dash()
server = app.server  # For deployment (e.g., Gunicorn)
app.layout = dmc.MantineProvider([
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
        dmc.GridCol(dcc.Graph(id='boxplot'), span=5, offset=1), 
    ]),
])

#----- CALLBACK ----------------------------------------------------------------
# Main callback: updates all 4 visualizations when any input changes

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
    '''Main callback: filter data and regenerate all figures.'''
    # Convert country names to codes (or None if 'SKIP')
    code_1 = get_country_code(country_1) if country_1 != 'SKIP' else None
    code_2 = get_country_code(country_2) if country_2 != 'SKIP' else None
    code_3 = get_country_code(country_3) if country_3 != 'SKIP' else None
    
    # Filter data by selected year range
    df = (
        df_transposed
        .filter(pl.col('YEAR').is_between(year_range[0], year_range[1]))
    )
    
    # Generate all visualizations
    timeline_plot = get_timeline_plot(df, selected_plot_type, code_1, code_2, code_3)
    histogram = get_histogram(df)
    boxplot = get_boxplot(df)
    choropleth = get_choropleth(df)
    
    return timeline_plot, histogram, boxplot, choropleth


#----- MAIN --------------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True)  # debug=True enables hot-reload during development