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
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal', 'color': 'gray'}
style_card = {'text-align': 'center', 'font-size': '20px', 
            'fontFamily': 'Arial','font-weight': 'normal'}

# Responsive grid span for stat cards
dmc_card_span = {"base": 12, "sm": 6, "md": 2}
dmc_card_span = {"base": 12, "sm": 6, "md": 2}
#-----  FUNCTIONS --------------------------------------------------------------
def stat_card(title, value, id_prefix=None):
    '''Accessible, responsive stat card.
    title: label shown at top-left
    value: initial value string (can be empty, will show N/A)
    id_prefix: sets the value text id as f'{id_prefix}-info' to match callbacks
    '''
    value_txt = (
        f'{value:,}' if isinstance(value, (int, float)) 
        else (value if value not in (None, '') else 'N/A')
    )
    value_id = f'{id_prefix}-info' if id_prefix else None

    header = dmc.Group(
        justify='space-between',
        align='flex-start',
        children=[
            dmc.Text(title, size='xl', fw=600, c='dimmed'),
        ]
    )

    # Right-side content stack (title + value)
    content_stack = dmc.Stack([
        header,
        dmc.Space(h=4),
        dmc.Text(
            value_txt, 
            id=value_id, 
            size='xl', 
            style={'lineHeight': '1.1', 'color': 'blue'}
        ),
    ], gap=0)

    # Left vertical accent bar
    accent_bar = html.Div(style={
        'width': '15px',
        'borderRadius': '4px',
        'background': 'repeating-linear-gradient(to bottom, #666666, #999999, #666666)'
    })

    # Row layout with accent bar + content
    row = html.Div([
        accent_bar,
        html.Div(content_stack, style={'flex': '1 1 auto'})
    ], style={'display': 'flex', 'gap': '12px', 'alignItems': 'stretch'})

    return dmc.Card(
        withBorder=True,
        shadow='sm',
        radius='md',
        padding='md',
        **{'aria-label': f'{title} statistic'},
        children=row
    )

def normalize_selection(selected_value, all_values_list):
    ''' Normalize dropdown/multiselect to handle ALL and ensure list type
    Input Args:
        selected_value: from dropdown/multiselect (can be string, list, or None)
        all_values_list: list of all possible values
    Returns:
        A list of selected values with 'ALL' properly handled
    '''
    # Handle None or empty list
    if selected_value is None or selected_value == []:
        return all_values_list
    
    # Handle 'ALL' as a string or as the only member of a list
    if selected_value == 'ALL' or selected_value == ['ALL']:
        return all_values_list
    
    # Handle list with 'ALL' in it
    if isinstance(selected_value, list):
        if 'ALL' in selected_value:
            # If list has ALL and other items, remove ALL
            filtered = [s for s in selected_value if s != 'ALL']
            return filtered if filtered else all_values_list
        return selected_value
    
    # Handle single string value
    return [selected_value]


def normalize_year_range(selected_range, lo_bound, hi_bound):
    # Ensure the slider values stay within bounds and are integers
    if not selected_range or len(selected_range) != 2:
        return [lo_bound, hi_bound]
    lo, hi = selected_range
    lo = int(round(lo))
    hi = int(round(hi))
    lo = max(lo_bound, lo)
    hi = min(hi_bound, hi)
    if lo > hi:
        lo, hi = hi, lo
    return [lo, hi]

def get_timeline_plot(df, plot_type):

    if plot_type == 'Raw Data':
        pass  # should be no action needed, use df as is

    elif plot_type == 'Norm Data':
        df = ( 
            df        
            .select(
                'YEAR',
                (cs.all().exclude('YEAR')-cs.all().exclude('YEAR').first()).name.suffix('_NORM')
            )
        )
  
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

    y_cols = [c for c in df.columns if c != 'YEAR']
    fig = px.line(
        df,
        x='YEAR', 
        y=y_cols,
        markers=False,
        title='Life Expectancy by Country'
    )
    fig.update_layout(template='plotly_white')
    # # Extract last timeline point as Python scalars (Polars -> Python)
    # last_date = df_time.select(pl.col('DATE').last()).item()
    # last_count = int(df_time.select(pl.col('Dog Count').last()).item())

    # Add a marker + label using Plotly Graph Objects
    # fig.add_trace(
    #     go.Scatter(
    #         x=[last_date],
    #         y=[last_count],
    #         mode='markers+text',
    #         text=[f'{last_count:,}  '],
    #         textposition='middle left',
    #         textfont=dict(size=16, color='blue'),
    #         marker=dict(size=8, color='gray'),
    #         hoverinfo='skip',
    #         showlegend=False,
    #         name=''
    #     )
    # )
    return fig

def get_top_age_group(df):
    if df.height:
        return(
            df.get_column('AGE')
            .value_counts()
            .sort('count', descending=True)
            .item(0, 'AGE')
        )
    else:
        return('N/A')

def get_dog_name_pareto(df, gender):
    df_gender = (
        df
        .filter(pl.col('SEX') == gender)
        .group_by('NAME')
        .agg(NAME_COUNT = pl.col('ID').len())
        .select('NAME', 'NAME_COUNT')
        .sort('NAME_COUNT', descending=True)
    )
    if len(df_gender) > 10:
        df_gender = df_gender.head(10)

    fig = px.bar(
        df_gender, # .sort('NAME_COUNT'),
        y='NAME',
        x='NAME_COUNT',
        orientation='h',
        template='simple_white',
        title=f'Top 10 {gender} dog names',
        text=df_gender['NAME_COUNT'],
        labels={'NAME': '',}
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(
        showticklabels=False,
        ticks='',
        showline=False,
        title_text=''
    )
    return fig

def get_choropleth(df_filtered):
    # Create a choropleth map of dog counts by state
    df_state = (
        df_filtered
        .group_by('CONTACT_STATE')
        .agg(pl.col('ID').count().alias('Dog Count'))
    )
    fig = px.choropleth(
        df_state,
        locations='CONTACT_STATE',
        locationmode='USA-states',
        color='Dog Count',
        scope='usa',
        title='Dog Counts by State',
        labels={'CONTACT_STATE': 'State', 'Dog Count': 'Number of Dogs'}
    )
    fig.update_layout(template='plotly_white')  
    return fig

#----- LOAD AND CLEAN DATA -----------------------------------------------------
# if os.path.exists(root_file + '.parquet'):
if False: # re-generates parquet from CSV
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

print(df.shape)
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
print(df_transposed.shape)
print(df_country_codes.shape)


#----- GLOBAL LISTS ------------------------------------------------------------
plot_types = ['Raw Data', 'Norm Data', 'PCT Change']
country_names = sorted(df_country_codes.unique('COUNTRY_NAME')['COUNTRY_NAME'].to_list())
country_codes = list(df_country_codes['COUNTRY_CODE'])
year_min = int(df_transposed['YEAR'].min())
year_max = int(df_transposed['YEAR'].max())

# contact_states = sorted(df.unique('CONTACT_STATE')['CONTACT_STATE'].to_list())
# primary_breeds = sorted(df.unique('BREED_PRIMARY')['BREED_PRIMARY'].to_list())
# animal_age_list = ['Baby', 'Young', 'Adult','Senior']
# dog_name_list = sorted(df.unique('NAME')['NAME'].to_list())

#----- DASH COMPONENTS------ ---------------------------------------------------
dcc_plot_type = (
    dcc.Dropdown(
        placeholder='Select Plot Type(s)', 
        options=plot_types, # menu choices  
        value=plot_types[0], # initial value              
        clearable=True, searchable=True, multi=False, closeOnSelect=False,
        style={'fontSize': '18px'},
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
# dcc_select_animal_age = (
#     dcc.Dropdown(
#         placeholder='Select Animal Age(s)', 
#         options=['ALL'] + animal_age_list, # menu choices  
#         value='ALL', # initial value              
#         clearable=True, searchable=True, multi=True, closeOnSelect=False,
#         style={'fontSize': '18px'},
#         id='id_select_animal_age'
#     )
# )
# dcc_select_primary_breed = (
#     dcc.Dropdown(
#         placeholder='Select Primary Breed(s)', 
#         options=['ALL'] + primary_breeds, # menu choices  
#         value='ALL', # initial value              
#         clearable=True, searchable=True, multi=True, closeOnSelect=False,
#         style={'fontSize': '18px'},
#         id='id_select_primary_breed'
#     )
# )

# # Dash Core Dropdown for dog name selection
# dcc_select_dog_name = (
#     dcc.Dropdown(
#         placeholder='Select Dog Names', 
#         options=['ALL'] + dog_name_list, # menu choices  
#         value='ALL', # initial value              
#         clearable=True, searchable=True, multi=True, closeOnSelect=False,
#         style={'fontSize': '18px'},
#         id='id_select_dog_name'
#     )
# )

# # Dash AG Grid Table for full df
# def get_ag_grid_table(df):
#     # Build columnDefs without floatingFilter
#     column_defs = []
#     for col in df.columns:
#         col_def = {"headerName": col, "field": col, "floatingFilter": True}
#         column_defs.append(col_def)
#     return dag.AgGrid(
#         id="ag-table-full-df",
#         columnDefs=column_defs,
#         rowData=df.to_dicts(),
#         defaultColDef={"filter": True, "sortable": True, "resizable": True},
#         style={"height": "500px", "width": "100%"}
#     )
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
        dmc.GridCol(dmc.Text('Select Plot Type', ta='left'), span=2, offset=2),
        dmc.GridCol(dmc.Text('Select Year Range', ta='left'), span=6, offset=0),
    ]),
    dmc.Space(h=10),
    dmc.Grid(
        children = [  
            dmc.GridCol(dcc_plot_type, span=2, offset=2),
            dmc.GridCol(dmc_year_range_slider, span=6, offset=0),
        ],
    ),
    dmc.Space(h=30),
    # dmc.Grid(children = [      # Summary cards row (responsive spans)
    #     dmc.GridCol(stat_card('Dog Count', '', id_prefix='dog-count'), span=dmc_card_span, offset=1),
    #     dmc.GridCol(stat_card('Top Age Group', '', id_prefix='top-age-group'), span=dmc_card_span),
    #     dmc.GridCol(stat_card('Fixed', '', id_prefix='fixed'), span=dmc_card_span),
    #     dmc.GridCol(stat_card('Shots Current', '', id_prefix='shots-current'), span=dmc_card_span),
    #     dmc.GridCol(stat_card('Organizations', '', id_prefix='organizations'), span=dmc_card_span),
    # ]),
    # dmc.Space(h=30),
    # html.Hr(style=style_horizontal_thin_line),
    #     dmc.Grid(children =  [
    #     dmc.GridCol(dmc.Text('Visualizations filtered by the selections above.', 
    #         ta='center'), span=10, offset=1),
    # ]),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='timeline_plot'), span=10,offset=1), 
        # dmc.GridCol(dcc.Graph(id='choropleth-map'), span=5, offset=1),           
    ]),
    # dmc.Grid(children = [
    #     dmc.GridCol(dcc.Graph(id='pareto-female'), span=5, offset=1),    
    #     dmc.GridCol(dcc.Graph(id='pareto-male'), span=5, offset=1),      
    # ]),
    # html.Hr(style=style_horizontal_thin_line),
    #     dmc.Grid(children =  [
    #     dmc.GridCol(dmc.Text('Raw data table with floating filters', 
    #         ta='center'), span=10, offset=1),
    # ]),
    # dmc.Grid(children = [
    #     dmc.GridCol(get_ag_grid_table(df), span=10, offset=1),        
    # ]),

])
@app.callback(
    # Output('dog-count-info', 'children'),
    # Output('top-age-group-info', 'children'),
    # Output('fixed-info', 'children'),
    # Output('shots-current-info', 'children'),
    # Output('organizations-info', 'children'),
    # Output('timeline-plot', 'figure'), 
    # Output('choropleth-map', 'figure'),  
    # Output('pareto-female', 'figure'),  
    Output('timeline_plot', 'figure'),
    Input('id_select_plot_type', 'value'),
    Input('id_year_range_slider', 'value'),
    # Input('id_select_primary_breed', 'value'),
    # Input('id_select_dog_name', 'value')
    )
def callback(selected_plot_type, year_range ):
    print(f'Plot Type selected: {selected_plot_type}')
    print(f'Year Range selected: {year_range}')
    df = (
        df_transposed
        .filter(pl.col('YEAR').is_between(year_range[0], year_range[1]))
    )
    print(df)
    timeline_plot=get_timeline_plot(df, selected_plot_type)
    return timeline_plot

    
    # # Filter dataframe based on selections
    # df_filtered = ( df
    #     .filter(pl.col('CONTACT_STATE').is_in(selected_states))
    #     .filter(pl.col('AGE').is_in(selected_animal_age))
    #     .filter(pl.col('BREED_PRIMARY').is_in(selected_primary_breed))
    #     .filter(pl.col('NAME').is_in(selected_dog_name))
    # )
    # dog_count = df_filtered.height
    # top_age_group = get_top_age_group(df_filtered)
    # fixed_count = df_filtered.filter(pl.col('FIXED')).height
    # fixed_pct = round(100 * fixed_count / dog_count, 1) if dog_count else 0.0
    # shots_count = df_filtered.filter(pl.col('SHOTS_CURRENT')).height
    # shots_pct = round(100 * shots_count / dog_count, 1) if dog_count else 0.0 
    # org_count = df_filtered.select(pl.col('ORG_ID')).unique().height
    # return (
    #     f'{dog_count:,}',
    #     top_age_group,
    #     f'{fixed_count:,} ({fixed_pct}%)',
    #     f'{shots_count:,} ({shots_pct}%)',
    #     f'{org_count:,}',
    #     get_timeline_plot(df_filtered),
    #     get_choropleth(df_filtered),
    #     get_dog_name_pareto(df_filtered, 'Female'),
    #     get_dog_name_pareto(df_filtered, 'Male')
    # )
if __name__ == '__main__':
    app.run(debug=True)