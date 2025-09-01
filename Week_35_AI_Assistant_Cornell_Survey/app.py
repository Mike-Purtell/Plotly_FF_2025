import polars as pl
import polars.selectors as cs
import os
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
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
viz_template = 'plotly_dark'
age_range_order = [ '18–20', '21–29', '30–39',  '40–49', '50–59', '60+']
education_order =['None', 'Elementary', 'High School', 'Some College', 'Bachelors',
                  'Masters',  'Doctorate', 'Professional']
years_experience_order = ['0', '<1', '1–2', '3–5', '6–10',  '11–16',  '16+']
#----- FUNCTIONS ---------------------------------------------------------------
def read_and_clean_csv():
    print('reading and cleaning csv file')
    no_education = 'I’ve never completed any formal education'
    elem_education = 'Primary / elementary school'
    high_school = (
        'Secondary school (e.g. American high school, ' +
        'German Realschule or Gymnasium, etc.)'
    )
    some_college = (
        "Some college or university study " +
        "without earning a bachelor’s degree"
    )
    bachelors = 'Bachelor’s degree (BA, BS, B.Eng., etc.)'
    masters = 'Master’s degree (MA, MS, M.Eng., MBA, etc.)'
    doctorate = 'Doctoral degree (Ph.D, Ed.D., etc.)'
    professional = 'Professional degree (JD, MD, etc.)'
    return(
        pl.scan_csv('dataset.csv')
        .select(
            COUNTRY = cs.starts_with('[1]').cast(pl.Categorical()),
            GENDER = cs.starts_with('[2]').cast(pl.Categorical()),
            AGE_RANGE = cs.starts_with('[3]')
                .replace('60 or older', '60+')
                .cast(pl.Categorical()),
            WORK_LANG = pl.concat_list(cs.starts_with('[9]'))
                .list.eval(pl.element().filter(pl.element().is_not_null())),
            EDUCATION = cs.starts_with('[12]')
                .replace(no_education, 'None')
                .replace(elem_education, 'Elementary')
                .replace(high_school, 'High School')
                .replace(some_college, 'Some College')
                .replace(bachelors, 'Bachelors')
                .replace(masters, 'Masters')
                .replace(doctorate, 'Doctorate')
            .replace(professional, 'Professional')
            .cast(pl.Categorical()),
            JOB_ROLE = pl.concat_list(cs.starts_with('[23]'))
                .list.eval(pl.element().filter(pl.element().is_not_null())),
            JOB_LEVEL = cs.starts_with('[24]').cast(pl.Categorical()),
            YEARS_EXPERIENCE = cs.starts_with('[25]')
                .str.replace(' years', '')
                .str.replace(' year', '')
                .str.replace('Less than 1', '<1')
                .str.replace("I don't have any professional coding experience", '0'),
            CS_LANGS = pl.concat_list(cs.starts_with('[44]'))
                .list.eval(pl.element().filter(pl.element().is_not_null())),
            AI_ASST = pl.concat_list(cs.starts_with('[62]'))
                .list.eval(pl.element().filter(pl.element().is_not_null())),
            AI_FEATURES = pl.concat_list(cs.starts_with('[63]'))
                .list.eval(pl.element().filter(pl.element().is_not_null())),
        )
        .with_columns(
            JOB_ROLE_COUNT = pl.col('JOB_ROLE').list.len(),
            WORK_LANG_COUNT = pl.col('WORK_LANG').list.len(),
            CS_LANGS_COUNT = pl.col('CS_LANGS').list.len(),
            AI_ASST_COUNT = pl.col('AI_ASST').list.len(),
            AI_FEATURES_COUNT = pl.col('AI_FEATURES').list.len(),
        )
        .with_columns(
            JOB_ROLE_WT = pl.when(pl.col('JOB_ROLE_COUNT') > 0)
                .then(1/pl.col('JOB_ROLE_COUNT')).otherwise(pl.lit(0)),        
            WORK_LANG_WT = pl.when(pl.col('WORK_LANG_COUNT') > 0)
                .then(1/pl.col('WORK_LANG_COUNT')).otherwise(pl.lit(0)),
            CS_LANGS_WT = pl.when(pl.col('CS_LANGS_COUNT') > 0)
                .then(1/pl.col('CS_LANGS_COUNT')).otherwise(pl.lit(0)),
            AI_ASST_WT = pl.when(pl.col('AI_ASST_COUNT') > 0)
                .then(1/pl.col('AI_ASST_COUNT')).otherwise(pl.lit(0)),
            AI_FEATURES_WT = pl.when(pl.col('AI_FEATURES_COUNT') > 0)
                .then(1/pl.col('AI_FEATURES_COUNT')).otherwise(pl.lit(0)),
        )
        .drop_nulls('GENDER')
        .filter(pl.col('EDUCATION') != 'Other')
        .select(
            'COUNTRY', 'GENDER', 'AGE_RANGE', 
            'EDUCATION',  'JOB_LEVEL', 'YEARS_EXPERIENCE', 
            'JOB_ROLE',     'JOB_ROLE_COUNT',     'JOB_ROLE_WT',       
            'WORK_LANG',    'WORK_LANG_COUNT',     'WORK_LANG_WT',
            'CS_LANGS',     'CS_LANGS_COUNT',      'CS_LANGS_WT',
            'AI_ASST',      'AI_ASST_COUNT',       'AI_ASST_WT',
            'AI_FEATURES',  'AI_FEATURES_COUNT',   'AI_FEATURES_WT',
        )
        .with_columns(cs.float().cast(pl.Float32))
        .with_columns(cs.integer().cast(pl.UInt8))
        .with_row_index(name='INDEX', offset=1)
        .with_columns(pl.col('INDEX').cast(pl.UInt16))
        .explode('JOB_ROLE')
        .explode('WORK_LANG')
        .explode('CS_LANGS') 
        .explode('AI_ASST')
        .explode('AI_FEATURES')
        .with_columns(cs.string().cast(pl.Categorical()))
        .collect()  # convert Lazy Frame to Data Frame
    )

def get_histo_users(country, group_by):
    print(f'{group_by = }')
    print(f'{country = }')

    if group_by == 'AGE_RANGE':
        group_by_order = age_range_order
    elif group_by == 'YEARS_EXPERIENCE':
        group_by_order = years_experience_order
    elif group_by == 'EDUCATION':
        group_by_order = education_order
    if country == 'ALL_COUNTRIES':
        df_histo = df_global  # no filtering when country is ALL_COUNTRIES 
    else:   # only use data from selected country
        df_histo = df_global.filter(pl.col('COUNTRY') == country)
    
    df_histo = (
        df_histo.filter(pl.col(group_by).is_not_null())
    ) 
    country_title = country.title().replace('_', ' ')
    fig = px.histogram(
        df_histo.unique('INDEX').sort(group_by),
        group_by,
        color='GENDER',
        title=f'{country_title}:  user counts by {group_by}'.upper(),
        template=viz_template
    )

    fig.update_layout(
        # x-title obvious, use empty string to make room for bottom legend
        yaxis_title = 'USER COUNT', xaxis_title = group_by, 
        legend=dict(
            yanchor='top', 
            xanchor='left',
            y=1.1,
            orientation="h",
        ),
        xaxis=dict(
            categoryorder='array',  # Specify custom order
            categoryarray=group_by_order,  # Desired order of categories
        )
    )
    return fig

#----- GATHER AND CLEAN DATA ---------------------------------------------------
if os.path.exists('df.parquet'):     # read parquet file if it exists
    print('reading data from parquet file')
    df_global=pl.read_parquet('df.parquet')
else:                                # read csv file, clean, save df as parquet
    df_global = read_and_clean_csv()
    print(df_global)
    df_global.write_parquet('df.parquet')

print(df_global.shape)
print(df_global)
print(df_global.glimpse())
#----- GLOBALS FROM DATAFRAME --------------------------------------------------
country_list = sorted(df_global.get_column('COUNTRY').unique().to_list())

#----- DASH COMPONENTS------ ---------------------------------------------------
dmc_select_group_by = (
    #  group by country, age, education
    dmc.Select(
        label='Group Data by',
        placeholder="Select one",
        id='group-by',
        data= ['AGE_RANGE', 'EDUCATION', 'YEARS_EXPERIENCE'],
        value='AGE_RANGE',
        size='xl',
    ),
)

dmc_select_country = (
    #  group by country, age, education
    dmc.Select(
        label='Filter by Country - Pick 1 or All',
        placeholder="Select one",
        id='country',
        data= ['ALL_COUNTRIES'] + country_list,
        value=country_list[0],
        # value='ALL_COUNTRIES',
        size='xl',
    ),
)

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Gender Patterns for AI Adoption', ta='center', style=style_h2),
    dmc.Text('', ta='center', style=style_h3, id='zip_code'),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Space(h=30),
    dmc.Grid(children = [
        dmc.GridCol(dmc.Text('Dashboard Control Panel', 
            fw=500, # semi-bold
            style={'fontSize': 28},
            ),
            span=3, offset=1
        )]
    ),
    dmc.Space(h=30),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_group_by, span=3, offset = 1),
        dmc.GridCol(dmc_select_country, span=3, offset = 1),
    ]),  
    dmc.Space(h=75),
    html.Hr(style=style_horizontal_thin_line),
    dmc.Grid(  
        children = [
            # dmc.GridCol(dcc.Graph(id='boxplot'), span=4, offset=0),
            dmc.GridCol(dcc.Graph(id='histogram-users'), span=6, offset=0), 
            # dmc.GridCol(dag.AgGrid(id='ag-grid'),span=3, offset=0),           
        ]
    ),
])

@app.callback(
    Output('histogram-users', 'figure'),
    Input('country', 'value'),
    Input('group-by', 'value')
)
def update(country, group_by):
    print(f'{group_by = }')
    print(f'{country = }')
    if country == None:
        country = country_list[0]
    histo_users = get_histo_users(country, group_by)
    return (
        histo_users
    )

if __name__ == '__main__':
    app.run(debug=True)