import polars as pl
import polars.selectors as cs
import os
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
import dash_ag_grid as dag


#----- LOAD AND CLEAN DATA -----------------------------------------------------
root_file = 'allDogDescriptions'
#  if False: # os.path.exists(root_file + '.parquet'):
if os.path.exists(root_file + '.parquet'):
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
        .select(
            ID = pl.col('id').cast(pl.UInt32),
            ORG_ID = pl.col('org_id'),
            BREED_PRIMARY = pl.col('breed_primary'),
            BREED_MIXED = pl.col('breed_mixed'),
            AGE = pl.col('age').cast(enum_AGE),
            SEX = pl.col('sex').cast(enum_SEX),
            SIZE = pl.col('size').cast(enum_SIZE),
            FIXED = pl.col('fixed'),
            HOUSE_TRAINED = pl.col('house_trained'),
            SHOTS_CURRENT = pl.col('shots_current'),
            NAME = pl.col('name').str.strip_chars().str.to_titlecase(),
            DATE = pl.col('posted')   # regex for dates formatted as YYYY-MM-DD
                .str.extract(r'(\d{4}-\d{2}-\d{2})', group_index=1)
                .str.to_date('%Y-%m-%d'),
            TIME = pl.col('posted')   # regex for time formatted as HH:MM:SS
                .str.extract(r'(\d{2}:\d{2}:\d{2})', group_index=1)
                .str.to_time('%H:%M:%S'),
            CONTACT_CITY = pl.col('contact_city'),
            CONTACT_STATE = pl.col('contact_state'),
            CONTACT_ZIP = pl.col('contact_zip').cast(pl.UInt32),
        )
        .filter(  # regex to accept states comprised of 2 uppercase letters    
            pl.col('CONTACT_STATE').str.contains(r'^[A-Z]{2}$')
        )
    )
    df.write_parquet(root_file + '.parquet')

# print(df.shape)
print(df.columns[:8])
print(df.columns[8:])

# print(df.sample(10).glimpse())

#----- GLOBALS -----------------------------------------------------------------
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

contact_states = sorted(df.unique('CONTACT_STATE')['CONTACT_STATE'].to_list())
primary_breeds = sorted(df.unique('BREED_PRIMARY')['BREED_PRIMARY'].to_list())
print(primary_breeds[:25])
# ----- SUMMARY METRICS FOR CARDS ------------------------------------------------
# compute a few summary stats used in the UI cards
total_animals = df.height if hasattr(df, 'height') else (df.shape[0] if hasattr(df, 'shape') else len(df))

# most common age (safe fallback to 'Unknown')
most_common_age = (
    df['AGE']
    .value_counts()
    .sort('count', descending=True)
    .item(0, 'AGE')
)
print(type(most_common_age))
print(most_common_age)


# helper to count truthy-ish values (handles a few common string encodings)
_truthy_values = [True, 1, '1', 'True', 'true', 'Yes', 'yes', 'Y', 'y']
fixed_count = df.filter(pl.col('FIXED').is_in(_truthy_values)).height
fixed_pct = round(100 * fixed_count / total_animals, 1) if total_animals else 0.0

shots_count = df.filter(pl.col('SHOTS_CURRENT').is_in(_truthy_values)).height
shots_pct = round(100 * shots_count / total_animals, 1) if total_animals else 0.0

org_count = df.select(pl.col('ORG_ID')).unique().height

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
        options=['ALL', 'Baby', 'Young', 'Adult','Senior'], # menu choices  
        value='ALL', # initial value              
        clearable=True, searchable=True, multi=True, closeOnSelect=False,
        id='id_select_animal_age'
    )
)

dcc_select_primary_breed = (
    dcc.Dropdown(
        placeholder='Select Primary Breed(s)', 
        options=primary_breeds, # menu choices  
        value=primary_breeds[0], # initial value              
        clearable=True, searchable=True, multi=False, closeOnSelect=False,
        id='id_select_primary_breed'
    )
)


# #----- DASH APPLICATION STRUCTURE---------------------------------------------

app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Shelter Animal Analytics Dashboard', ta='center', style=style_h2),
    # dmc.Text('Comprehensive analysis of shelter animal intake, demographics, health status, and organizational performance across 58,147 records spanning 2003-2019', ta='center', style=style_h3),
    dmc.Text(
        'Comprehensive analysis of shelter animal intake, demographics,',
        ta='center', style=style_h3
    ),
    dmc.Text(
        'health status, and organizational performance across 58,147 records ' +
        'spanning 2003-2019.', ta='center', style=style_h3
    ),
    # Summary cards row
    dmc.Group([
        dmc.Card(
            dmc.Group([
                dmc.Text(f"{total_animals:,}", size='xl', weight=700),
                dmc.Text('Total Animals', size='sm', color='dim')
            ], direction='column', position='center'),
            shadow='sm', padding='md', radius='md', style={'minWidth': '160px', 'textAlign': 'center', 'margin': '6px'}
        ),
        dmc.Card(
            dmc.Group([
                dmc.Text(str(most_common_age), size='xl', weight=700),
                dmc.Text('Most Common Age', size='sm', color='dim')
            ], direction='column', position='center'),
            shadow='sm', padding='md', radius='md', style={'minWidth': '160px', 'textAlign': 'center', 'margin': '6px'}
        ),
        dmc.Card(
            dmc.Group([
                dmc.Text(f"{fixed_pct}%", size='xl', weight=700),
                dmc.Text('Fixed (%)', size='sm', color='dim')
            ], direction='column', position='center'),
            shadow='sm', padding='md', radius='md', style={'minWidth': '160px', 'textAlign': 'center', 'margin': '6px'}
        ),
        dmc.Card(
            dmc.Group([
                dmc.Text(f"{shots_pct}%", size='xl', weight=700),
                dmc.Text('Shots Current (%)', size='sm', color='dim')
            ], direction='column', position='center'),
            shadow='sm', padding='md', radius='md', style={'minWidth': '160px', 'textAlign': 'center', 'margin': '6px'}
        ),
        dmc.Card(
            dmc.Group([
                dmc.Text(str(org_count), size='xl', weight=700),
                dmc.Text('Organizations', size='sm', color='dim')
            ], direction='column', position='center'),
            shadow='sm', padding='md', radius='md', style={'minWidth': '160px', 'textAlign': 'center', 'margin': '6px'}
        ),
    ], position='center', spacing='md', style={'width': '100%', 'margin': '10px 0'}),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Grid(children =  [
        dmc.GridCol(dmc.Text('Select a State(s)', ta='left', style=style_h4), 
        span=2, offset=1
        ),
        dmc.GridCol(dmc.Text('Select Animal Range', ta='left', style=style_h4), 
        span=2, offset=1
        ),
        dmc.GridCol(dmc.Text('Select Primary Breed', ta='left', style=style_h4), 
        span=2, offset=1
        ),
    ]),
    
    dmc.Grid(
        children = [  
            dmc.GridCol(dcc_select_contact_state, span=2, offset=1),
            dmc.GridCol(dcc_select_animal_age, span=2, offset=1),
            dmc.GridCol(dcc_select_primary_breed, span=2, offset=1)
        ],
    ),
    html.Hr(style=style_horizontal_thin_line),
    # dmc.Text('Comprehensive analysis of shelter animal intake, demographics, health status, and organizational performance across 58,147 records spanning 2003-2019', ta='center', style=style_h3),
    # dmc.Text('Comprehensive analysis of shelter animal intake, demographics, health status, and organizational performance across 58,147 records spanning 2003-2019', ta='center', style=style_h3),
    # dmc.Text('Comprehensive analysis of shelter animal intake, demographics, health status, and organizational performance across 58,147 records spanning 2003-2019', ta='center', style=style_h3),




    # dmc.Grid(children = [
    #     dmc.GridCol(dmc_select_map_style, span=2, offset = 1),
    # ]),  
    # dmc.Grid(children = [
    #         dmc.GridCol(dcc.Graph(id='scatter-map'), span=10, offset=1),          
    #     ]),
])
@app.callback(
    # Output('scatter-map', 'figure'),
    Input('id_select_contact_state', 'value'),
    Input('id_select_animal_age', 'value'),
    Input('id_select_primary_breed', 'value'),

)
def callback(selected_states, selected_animal_age, selected_primary_breed):
    print(f'SELECTED STATES: {selected_states}')
    print(f'ANIMAL AGE: {selected_animal_age}')
    print(f'SELECTED PRIMARY BREED: {selected_primary_breed}')


    # scatter_map=get_scatter_map(map_style)
    return # scatter_map

if __name__ == '__main__':
    app.run(debug=True)
