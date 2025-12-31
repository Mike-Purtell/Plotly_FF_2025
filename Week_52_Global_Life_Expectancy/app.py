"""
Global Life Expectancy Dashboard
================================
An interactive Dash application visualizing life expectancy data across
countries and decades.

Features:
- Timeline plot showing life expectancy trends by country
  (raw, normalized, or % change)
- Focus country highlighting for up to 4 countries
- Stacked histogram showing distribution by decade
- Box plot comparing decades
- Choropleth map showing average life expectancy by country
- Pareto charts displaying top/bottom N countries ranked by average
  life expectancy, with lollipop-style markers and horizontal orientation
- Range slider for filtering data by year range

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
root_file = 'live_expectancy_at_birth'  # Base filename for data (CSV/parquet)

# CSS styles for consistent UI elements throughout the dashboard
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}
style_horizontal_thin_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 12}

# Title text style
style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold', 'color': 'gray'}
style_h3 = {'text-align': 'center', 'font-size': '16px', 
            'fontFamily': 'Arial','font-weight': 'normal', 'color': 'gray'}
plotly_template = 'simple_white'  # Consistent Plotly template  

color_palette = px.colors.qualitative.Dark24  # Color palette for timeline traces`

#-----  VISUALIZATION FUNCTIONS ------------------------------------------------
# Each function generates a specific Plotly figure for the dashboard

def get_timeline_plot(df, plot_type, focus_country_codes) -> go.Figure:
    '''
    Generate a multi-line timeline plot showing life expectancy over time.
    
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
            .with_columns(cs.all().exclude('YEAR') * 100)
        )
        y_axis_label = 'Cumulative Change (%)'

    y_cols = [c for c in df.columns if c != 'YEAR']
    first_year = df['YEAR'].min()
    last_year = df['YEAR'].max()


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
        template=plotly_template,
    )
    
    if any(focus_country_codes):
        # when focus countries are selected, gray out/de-emphasize all others
        fig.update_traces(
            mode='lines',
            line=dict(color='lightgray'), 
            showlegend=False,
            hoverinfo='none',
        )
        fig.update_layout(
            showlegend=True, 
            legend_title_text='Focus Countries',
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='top',
                y=-0.2,
                xanchor='center',
                x=0.5
            ),
        )
        # Add highlighted traces for each focus country
        for i, code in enumerate(focus_country_codes):
            my_color = color_palette[i % len(color_palette)]
            fig.add_traces([
                go.Scatter(
                    x=df['YEAR'],
                    y=df[code],
                    name=get_country_name(code),
                    marker=dict(size=marker_size, color=my_color),
                    line=dict(width=line_width, color=my_color, dash='solid'),
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
        title=dict(
            text='Life Expectancy by Decade',
            subtitle=dict(text=f'{first_decade} to {last_decade}'),
        ),
        xaxis_title='Life Expectancy (Years)',
        template=plotly_template,
        barmode='stack',
        bargap=0.1,
    )
    fig.update_traces(opacity=0.5)
    fig.update_xaxes(
        showticklabels=True,
        ticks='',
        showline=True,
        title_text='Life Expectancy (Years)',
        showgrid=True,
        range=[20, 90],
    )
    return fig

def get_boxplot(df) -> go.Figure:
    '''
    Generate vertical box plots comparing life expectancy distribution by decade.
    
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
        fig.add_trace(
            go.Box(
                y=df_decade['value'],
                name=decade,
                boxmean=True,
            )
        )
    fig.update_layout(
        title=dict(
            text='Life Expectancy by Decade',
            subtitle=dict(text=f'{first_decade} to {last_decade}'),
        ),
        yaxis_title='Life Expectancy (Years)',
        template=plotly_template,
    )
    fig.update_yaxes(
        showticklabels=True,
        ticks='',
        showline=True,
        showgrid=True,
        range=[20, 90],
        title_text='Life Expectancy (Years)',
    )
    return fig

def get_choropleth(df) -> go.Figure:
    '''
    Generate a world choropleth map showing average life expectancy by country.
    
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
        title='Average Life Expectancy by Country',
        subtitle=f'{first_year} to {last_year}',
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

def get_pareto(df, plot_type: str, category: str, top_n: int) -> go.Figure:
    '''
    Create a Pareto chart showing top or bottom countries by life expectancy.

    Generates a horizontal lollipop chart ranking countries by their mean
    life expectancy across all available years. Countries are sorted and
    displayed with markers and connecting lines.
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
        .select(
            COUNTRY_CODE = pl.col('column'),
            MEAN = pl.mean_horizontal(cs.float()),
            GAIN = (
                pl.concat_list(cs.float()).list.last() - 
                pl.concat_list(cs.float()).list.first()
            ),
            PCT_GAIN = (
                (pl.concat_list(cs.float()).list.last() - 
                pl.concat_list(cs.float()).list.first()) /
                pl.concat_list(cs.float()).list.first() * 100
            ),
        )      
        .join(
            df_country_codes, on='COUNTRY_CODE', how='left'
        )

        .filter(pl.col('MEAN').is_not_null())
        .filter(pl.col('MEAN').is_not_null())
        .filter(pl.col('PCT_GAIN').is_not_null())
    )
    df_transposed.write_csv('debug_pareto.csv')  # Debug output
    if plot_type == 'Raw Data':
        x_cat = 'MEAN'
        my_title=(
            f'{"Top" if category=="TOP" else "Bottom"} ' +
            f'{top_n} Average Life Expectancies'
        )
        my_xaxis_title='Avg. Life Expectancy (Years)'
    elif plot_type == 'Norm Data':
        x_cat = 'GAIN'
        my_title=(
            f'{"Top" if category=="TOP" else "Bottom"} ' +
            f'{top_n} Life Expectancy Gains (Years)'
        )
        my_xaxis_title='Life Expectancy Gains (Years)'
    elif plot_type == 'PCT Change':
        x_cat = 'PCT_GAIN'
        my_title=(
            f'{"Top" if category=="TOP" else "Bottom"} ' +
            f'{top_n} Life Expectancy Gains (%)'
        )
        my_xaxis_title='Life Expectancy Gain (%)'
    

    my_subtitle=f'{first_year} to {last_year}'

    if category == 'TOP':
        df_pareto = df_transposed.sort(x_cat, descending=True).head(top_n)
    elif category == 'BOTTOM':
        df_pareto = df_transposed.sort(x_cat, descending=False).head(top_n)


    # Get country names in reverse order for y-axis (so highest/lowest appears at top)
    country_order = df_pareto['COUNTRY_NAME'].to_list()
    
    fig=px.scatter(
        df_pareto,
        y='COUNTRY_NAME',
        x=x_cat,
        template=plotly_template,
        title=my_title,
        subtitle=my_subtitle,
        custom_data=['COUNTRY_CODE', 'COUNTRY_NAME', 'MEAN', 'GAIN', 'PCT_GAIN'],
        category_orders={'COUNTRY_NAME': country_order},
        text='COUNTRY_NAME',
    )
    labels = df_pareto['COUNTRY_NAME'].to_list()
    fig.update_traces(
        hovertemplate=(
            '<b>%{customdata[0]}</b><br>' +   
            '<b>%{customdata[1]}</b><br><br>' +  
            '<b>MEAN:</b>%{customdata[2]:.3f}<br>' +
            '<b>GAIN:</b>%{customdata[3]:.3f}<br>' + 
            '<b>PCT_GAIN:</b>%{customdata[4]:.1f} %<extra></extra>'
        ),
        mode='lines+markers+text',
        text=labels,                # Text labels for each point
        textposition='top right',  # Position of text relative to markers
        marker=dict(size=10, color='blue'),
        line=dict(color='lightgray', width=2),
    )  

    pareto_min = df_pareto[x_cat].min()
    pareto_max = df_pareto[x_cat].max()
    pareto_margin = 0.05 * (pareto_max - pareto_min)
    
    # Calculate 6 evenly-spaced tick positions
    tick_step = (pareto_max - pareto_min) / 5  # 5 intervals for 6 ticks
    tick_vals = [pareto_min + i * tick_step for i in range(6)]
    
    if category == 'TOP':
        fig.update_xaxes(range=
            [pareto_max + pareto_margin, pareto_min - 8*pareto_margin]
        )
    elif category == 'BOTTOM':
        fig.update_xaxes(range=
            [pareto_min - pareto_margin, pareto_max + 8*pareto_margin]
        )
    # hide y-axis(country name), since it's shown as a text label on each point
    fig.update_yaxes(
        showticklabels=False,
        ticks='',
        showline=False,
        title_text='',
        showgrid=False,
    )
    fig.update_xaxes(
        showgrid=True,
        tickmode='array',
        tickvals=tick_vals,
        tickformat='.1f',
        title_text=my_xaxis_title,
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

plot_types = ['Raw Data', 'Norm Data', 'PCT Change']  # Timeline view options
all_country_names = sorted(df_country_codes.unique('COUNTRY_NAME')['COUNTRY_NAME'].to_list())  # Dropdown options
all_country_codes = list(df_country_codes['COUNTRY_CODE'])
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

# Focus country dropdowns (pick up to 5 countries to highlight on timeline)
dcc_focus_countries = (
    dcc.Dropdown(
        placeholder='Select Country', 
        options=[{'label': name, 'value': name} for name in all_country_names],
        value=['Afghanistan','Canada', 'Bangladesh'],  # Default selection
        style={'fontSize': '18px', 'color': 'black'},
        id='id_focus_countries',multi=True
    )
)

#----- DASH APPLICATION STRUCTURE ----------------------------------------------
# Main application layout using Dash Mantine Components grid system
app_banner='Global Life Expectancy'
app_sub_banner = (
    'An interactive Dash application visualizing worldwide life expectancy ' +
     ' Data Source: World Bank'
)
app = Dash()
server = app.server  # For deployment (e.g., Gunicorn)
app.layout = dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text(app_banner, ta='center', style=style_h2),
    dmc.Space(h=10),
    dmc.Text(app_sub_banner, ta='center', style=style_h3),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Space(h=30),
    dmc.Grid(children =  [
        dmc.GridCol(
            dmc.Text('Timeline, Pareto Data Type', ta='left'), span=2, offset=0),
        dmc.GridCol(dmc.Text(
            'Year Range Slider - Filters all visualizations', ta='left'), 
            span=6, offset=2
        ),
    ]),
    dmc.Space(h=10),
    dmc.Grid(
        children = [  
            dmc.GridCol(html.Div(dcc_plot_type), span=2, offset=0),
            dmc.GridCol(dmc_year_range_slider, span=6, offset=2),
        ],
    ),
    dmc.Space(h=30),
        dmc.Grid(children =  [
            dmc.GridCol(
                dmc.Text('Timeline Focus Countries, pick up to 5', ta='left'), 
                span=4, offset=0
                ),
    ]),
    dmc.Grid(
        children = [  
            dmc.GridCol(html.Div(dcc_focus_countries), span=4, offset=0),
            dmc.GridCol(dmc.Text('Pareto Graphs', ta='left'), span=4, offset=1),
        ],
    ),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='timeline_plot'), span=4,offset=0), 
        dmc.GridCol(dcc.Graph(id='highest-10-expectancy'), span=4, offset=0),
        dmc.GridCol(dcc.Graph(id='lowest-10-expectancy'), span=4, offset=0),      
    ]),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='choropleth'), span=4, offset=0), 
        dmc.GridCol(dcc.Graph(id='histogram'), span=4, offset=0),  
        dmc.GridCol(dcc.Graph(id='boxplot'), span=4, offset=0), 
    ]),
])

#----- CALLBACK ----------------------------------------------------------------
# Main callback: updates all visualizations when any input changes

@app.callback(
    Output('timeline_plot', 'figure'),
    Output('histogram', 'figure'),
    Output('boxplot', 'figure'),
    Output('choropleth', 'figure'),
    Output('highest-10-expectancy', 'figure'),
    Output('lowest-10-expectancy', 'figure'),
    Output('id_focus_countries', 'value'),
    Input('id_select_plot_type', 'value'),
    Input('id_year_range_slider', 'value'),
    Input('id_focus_countries', 'value'),
)
def callback(selected_plot_type, year_range, focus_countries):
    '''Main callback: filter data and regenerate all figures.'''

    max_selections = 5
    if focus_countries and len(focus_countries) > max_selections:
        focus_countries = focus_countries[:max_selections]

    focus_country_codes = [
        get_country_code(country) for country in focus_countries]

    # Filter data by selected year range
    df = (
        df_transposed
        .filter(pl.col('YEAR').is_between(year_range[0], year_range[1]))
    )
    
    # Generate all visualizations
    timeline_plot = get_timeline_plot(df, selected_plot_type,  focus_country_codes)

    histogram = get_histogram(df)
    boxplot = get_boxplot(df)
    choropleth = get_choropleth(df)
    top_10 = get_pareto(df, selected_plot_type, 'TOP', 10) 
    bottom_10 = get_pareto(df, selected_plot_type, 'BOTTOM', 10)
    
    return (timeline_plot, histogram, boxplot, choropleth, 
        top_10, bottom_10, focus_countries)


#----- MAIN --------------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True)  # debug=True enables hot-reload during development