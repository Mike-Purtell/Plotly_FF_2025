import polars as pl
import polars.selectors as cs
import os
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
import dash_ag_grid as dag

# TODO:
#     Add a choropleth of dog counts by state,using df_filtered
#     Add house trained dogs input trend
#     Add Pareto top male dog names, top female dog names
#     Add DAG table at bottom, unfilterd df. Put filters on each column.


#----- GLOBALS -----------------------------------------------------------------
root_file = 'allDogDescriptions'
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
style_h4 = {'text-align': 'center', 'font-size': '20px', 
            'fontFamily': 'Arial','font-weight': 'normal'}
style_card = {'text-align': 'center', 'font-size': '20px', 
            'fontFamily': 'Arial','font-weight': 'normal'}

#-----  FUNCTIONS --------------------------------------------------------------
def get_card(card_title, card_info):
    card_title_id = card_title.lower().replace(' ', '-') + '-title'
    card_info_id = card_title_id.replace('title', 'info')
    if isinstance(card_info, int): 
        card_info = f'{card_info:,}'
    if card_info == '':
        card_info = 'N/A'

    card = dmc.Card(
        dmc.Stack([
            html.Div(style={
                'height': '15px', 
                'background': 'linear-gradient(to right, #007bff, #ff7b00)',
                'width': '100%',
                'marginBottom': '4px'
            }),
            dmc.Text(
                card_title, fz=24, id=card_title_id,
                # style={'display': 'block', 'margin-bottom': '8px'}
            ),
            dmc.Text(f"{card_info}", fz=16, id=card_info_id),
        ],gap="xs"),
        withBorder=True,
        shadow='lg',
        radius='md'
    )
    return card

def get_timeline_plot(df_filtered):
    # Create a timeline plot of dog postings over time
    df_time = (
        df_filtered
        .sort('DATE')             # sort before dynamic_group_by is a must
        .group_by_dynamic(
            index_column='DATE',  # specify the datetime column
            every='1mo',          # interval size
            period='1mo',         # window size
            closed='left'         # interval includes the left endpoint
        )
        .agg(pl.col('ID').count().alias('Dog Count'))   
    )
    fig = px.line(
        df_time,
        x='DATE', 
        y='Dog Count',
        title='Dog Postings Over Time',
        subtitle='Grouped by Month',        
        labels={'DATE': 'Month', 'Dog Count': 'Number of Dogs Posted'},
        markers=True
    )
    fig.update_layout(template='plotly_white', yaxis_type='log')
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
    print(f'{gender = }')
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

    print('df_gender')
    print(gender)
    print(df_gender)

    fig = px.bar(
        df_gender, # .sort('NAME_COUNT'),
        y='NAME',
        x='NAME_COUNT',
        orientation='h',
        template='simple_white',
        title=f'Top 10 {gender} dog names',
        text=df_gender['NAME_COUNT'],
        labels={
        'NAME': '',
        }
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
    print(df_state)
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
    ## Return empty figure for now, until the groupby code is working
    # fig=go.Figure()

    return fig

#----- LOAD AND CLEAN DATA -----------------------------------------------------
root_file = 'allDogDescriptions'
if False: # use this during development to force re-reading CSV
#  if os.path.exists(root_file + '.parquet'):
    print(f'{"*"*20} Reading {root_file}.parquet  {"*"*20}')
    df = pl.read_parquet(root_file + '.parquet')
else:
    print(f'{"*"*20} Reading {root_file}.csv  {"*"*20}')
    # Define Enum categories
    enum_AGE = pl.Enum(['Adult', 'Baby', 'Senior', 'Young'])
    enum_SEX = pl.Enum(['Male', 'Female', 'Unknown'])
    enum_SIZE = pl.Enum(['Small', 'Medium', 'Large','Extra Large'])
    df = (
        pl.read_csv(root_file + '.csv', ignore_errors=True)
        # Only all dog names with letters a-z or whitespace
        .filter(pl.col('name').str.contains(r'^[a-zA-Z\s]+$'))
        .select(
            NAME = pl.col('name').str.strip_chars().str.to_titlecase(),
            CONTACT_CITY = pl.col('contact_city'),
            CONTACT_STATE = pl.col('contact_state'),
            CONTACT_ZIP = pl.col('contact_zip').cast(pl.UInt32),
            BREED_PRIMARY = pl.col('breed_primary'),
            BREED_MIXED = pl.col('breed_mixed'),
            AGE = pl.col('age').cast(enum_AGE),
            SEX = pl.col('sex').cast(enum_SEX),
            SIZE = pl.col('size').cast(enum_SIZE),
            FIXED = pl.col('fixed'),
            HOUSE_TRAINED = pl.col('house_trained'),
            SHOTS_CURRENT = pl.col('shots_current'),
            DATE = pl.col('posted')   # regex for dates formatted as YYYY-MM-DD
                .str.extract(r'(\d{4}-\d{2}-\d{2})', group_index=1)
                .str.to_date('%Y-%m-%d'),
            TIME = pl.col('posted')   # regex for time formatted as HH:MM:SS
                .str.extract(r'(\d{2}:\d{2}:\d{2})', group_index=1)
                .str.to_time('%H:%M:%S'),
            ID = pl.col('id').cast(pl.UInt32),
            ORG_ID = pl.col('org_id'),
        )
        .filter(  # regex to accept states comprised of 2 uppercase letters    
            pl.col('CONTACT_STATE').str.contains(r'^[A-Z]{2}$')
        )
        # merge all Cocker Spaniel variants into 'Cocker Spaniel'
        .with_columns(BREED_PRIMARY = 
            pl.when(pl.col('BREED_PRIMARY').str.contains('Cocker Spaniel'))
            .then(pl.lit('Cocker Spaniel'))
            .otherwise('BREED_PRIMARY')
        )
    )
    df.write_parquet(root_file + '.parquet')

#----- GLOBAL LISTS ------------------------------------------------------------
contact_states = sorted(df.unique('CONTACT_STATE')['CONTACT_STATE'].to_list())
primary_breeds = sorted(df.unique('BREED_PRIMARY')['BREED_PRIMARY'].to_list())
animal_age_list = ['Baby', 'Young', 'Adult','Senior']
dog_name_list = sorted(df.unique('NAME')['NAME'].to_list())

#----- DASH COMPONENTS------ ---------------------------------------------------
dcc_select_contact_state = (
    dcc.Dropdown(
        placeholder='Select Contact State(s)', 
        options=['ALL'] + contact_states, # menu choices  
        value='ALL', # initial value              
        clearable=True, searchable=True, multi=True, closeOnSelect=False,
        id='id_select_contact_state'
    )
)
dcc_select_animal_age = (
    dcc.Dropdown(
        placeholder='Select Animal Age(s)', 
        options=['ALL'] + animal_age_list, # menu choices  
        value='ALL', # initial value              
        clearable=True, searchable=True, multi=True, closeOnSelect=False,
        id='id_select_animal_age'
    )
)

dcc_select_primary_breed = (
    dcc.Dropdown(
        placeholder='Select Primary Breed(s)', 
        options=['ALL'] + primary_breeds, # menu choices  
        value='ALL', # initial value              
        clearable=True, searchable=True, multi=False, closeOnSelect=False,
        id='id_select_primary_breed'
    )
)

dcc_select_dog_name = (
    dcc.Dropdown(
        placeholder='Select Dog Name', 
        options=['ALL'] + dog_name_list, # menu choices  
        value='ALL', # initial value              
        clearable=True, searchable=True, multi=False, closeOnSelect=False,
        id='id_select_dog_name'
    )
)
# Dash AG Grid Table for full df
def get_ag_grid_table(df):
    # Build columnDefs without floatingFilter
    column_defs = []
    for col in df.columns:
        col_def = {"headerName": col, "field": col, "floatingFilter": True}
        column_defs.append(col_def)
    return dag.AgGrid(
        id="ag-table-full-df",
        columnDefs=column_defs,
        rowData=df.to_dicts(),
        defaultColDef={"filter": True, "sortable": True, "resizable": True},
        style={"height": "500px", "width": "100%"}
    )
#----- DASH APPLICATION STRUCTURE ----------------------------------------------

app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Furry Friday: Shelter Animal Analytics Dashboard', ta='center', style=style_h2),
    dmc.Text(
        'Comprehensive analysis of shelter animal intake, demographics,',
        ta='center', style=style_h3
    ),
    dmc.Text(
        'health status, and organizational performance across 58,147 records ' +
        'spanning 2003-2019.', ta='center', style=style_h3
    ),
    dmc.Space(h=30),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Space(h=30),
    dmc.Grid(children =  [
        dmc.GridCol(dmc.Text('State(s)', ta='left'), span=2, offset=2),
        dmc.GridCol(dmc.Text('Age Range', ta='left'), span=2, offset=0),
        dmc.GridCol(dmc.Text('Primary Breed', ta='left'), span=2, offset=0),
        dmc.GridCol(dmc.Text('Dog Name', ta='left'), span=2, offset=0),
    ]),
    dmc.Space(h=10),
    dmc.Grid(
        children = [  
            dmc.GridCol(dcc_select_contact_state, span=2, offset=2),
            dmc.GridCol(dcc_select_animal_age, span=2, offset=0),
            dmc.GridCol(dcc_select_primary_breed, span=2, offset=0),
            dmc.GridCol(dcc_select_dog_name, span=2, offset=0)
        ],
    ),
    dmc.Space(h=30),
    dmc.Grid(children = [      # Summary cards row
        dmc.GridCol(get_card('Dog Count', ''), 
            span=2, offset=1, ta='center', style=style_card),
        dmc.GridCol(get_card('Top Age Group',''), 
            span=2, offset=0, ta='center', style=style_card),
        dmc.GridCol(get_card('Fixed', ''), 
            span=2, offset=0, ta='center', style=style_card),
        dmc.GridCol(get_card('Shots Current', ''), 
            span=2, offset=0, ta='center', style=style_card),
        dmc.GridCol(get_card('Organizations', ''),
            span=2, offset=0, ta='center',style=style_card),
    ]),
    dmc.Space(h=30),
    html.Hr(style=style_horizontal_thin_line),
        dmc.Grid(children =  [
        dmc.GridCol(dmc.Text('Visualizations filtered by the selections above.', 
            ta='center'), span=10, offset=1),
    ]),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='timeline-plot'), span=5, offset=1), 
        dmc.GridCol(dcc.Graph(id='choropleth-map'), span=5, offset=1),           
    ]),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='pareto-female'), span=5, offset=1),    
        dmc.GridCol(dcc.Graph(id='pareto-male'), span=5, offset=1),      
    ]),
    html.Hr(style=style_horizontal_thin_line),
        dmc.Grid(children =  [
        dmc.GridCol(dmc.Text('Raw data table with floating filters', 
            ta='center'), span=10, offset=1),
    ]),
    dmc.Grid(children = [
        dmc.GridCol(get_ag_grid_table(df), span=10, offset=1),        
    ]),

])
@app.callback(
    Output('dog-count-info', 'children'),
    Output('top-age-group-info', 'children'),
    Output('fixed-info', 'children'),
    Output('shots-current-info', 'children'),
    Output('organizations-info', 'children'),
    Output('timeline-plot', 'figure'), 
    Output('choropleth-map', 'figure'),  
    Output('pareto-female', 'figure'),  
    Output('pareto-male', 'figure'),
    Input('id_select_contact_state', 'value'),
    Input('id_select_animal_age', 'value'),
    Input('id_select_primary_breed', 'value'),
    Input('id_select_dog_name', 'value'),
)
def callback(selected_states, selected_animal_age, selected_primary_breed, selected_dog_name):
    print(f'SELECTED STATES: {selected_states}')
    print(f'ANIMAL AGE: {selected_animal_age}')
    print(f'SELECTED PRIMARY BREED: {selected_primary_breed}')
    print(f'SELECTED DOG NAME: {selected_dog_name}')
    if selected_states == 'ALL' or selected_states ==['ALL']:
        selected_states = contact_states
    print(f'{selected_states = }')
    if selected_dog_name == 'ALL' or selected_dog_name ==['ALL']:
        selected_dog_name = dog_name_list
    print(f'{selected_animal_age = }')
    if selected_animal_age == 'ALL':
        selected_animal_age = animal_age_list # Only selected ALL
    
    if isinstance(selected_animal_age, list): 
        if 'ALL' in selected_animal_age:
            if len(selected_animal_age) > 1:  # list has ALL and others
                selected_animal_age = selected_animal_age.remove('ALL')
            else:
                selected_animal_age = animal_age_list # Only selected ALL
    
    
    if isinstance(selected_dog_name, list): 
        selected_dog_name = [name for name in selected_dog_name if name != 'ALL']
    else:
        selected_dog_name = [selected_dog_name]

    print(f'{selected_animal_age = }')
    print(f'{selected_primary_breed = }')
    # print(f'{selected_dog_name = }')
    if selected_primary_breed == 'ALL':
        selected_primary_breed = primary_breeds
    if not isinstance(selected_primary_breed, list):
        selected_primary_breed = [selected_primary_breed]
    df_filtered = ( df
        .filter(pl.col('CONTACT_STATE').is_in(selected_states))
        .filter(pl.col('AGE').is_in(selected_animal_age))
        .filter(pl.col('BREED_PRIMARY').is_in(selected_primary_breed))
        .filter(pl.col('NAME').is_in(selected_dog_name))
    )
    print('df_filtered')
    print(df_filtered)
    dog_count = df_filtered.height
    top_age_group = get_top_age_group(df_filtered)
    fixed_count = df_filtered.filter(pl.col('FIXED')).height
    fixed_pct = round(100 * fixed_count / dog_count, 1) if dog_count else 0.0
    shots_count = df_filtered.filter(pl.col('SHOTS_CURRENT')).height
    shots_pct = round(100 * shots_count / dog_count, 1) if dog_count else 0.0 
    org_count = df_filtered.select(pl.col('ORG_ID')).unique().height
    return (
        f'{dog_count:,}',
        top_age_group,
        f'{fixed_count:,} ({fixed_pct}%)',
        f'{shots_count:,} ({shots_pct}%)',
        f'{org_count:,}',
        get_timeline_plot(df_filtered),
        get_choropleth(df_filtered),
        get_dog_name_pareto(df_filtered, 'Female'),
        get_dog_name_pareto(df_filtered, 'Male')
    )

if __name__ == '__main__':
    app.run(debug=True)
