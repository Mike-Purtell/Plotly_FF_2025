import polars as pl
import polars.selectors as cs
import os
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

# TODO: Fix pareto to only show top 10
# TODO: unify dashboard theme with visualizations
# TODO: add barbell plot, male vs. famale for all outputs (AI_ASST, CS_LANG, AI_FEATURES)
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
            JOB_LEVEL = cs.starts_with('[24]').cast(pl.Categorical()),
            YEARS_EXPERIENCE = cs.starts_with('[25]')
                .str.replace(' years', '')
                .str.replace(' year', '')
                .str.replace('Less than 1', '<1')
                .str.replace("I don't have any professional coding experience", '0'),
            CS_LANG = pl.concat_list(cs.starts_with('[44]'))
                .list.eval(pl.element().filter(pl.element().is_not_null())),
            AI_ASST = pl.concat_list(cs.starts_with('[62]'))
                .list.eval(pl.element().filter(pl.element().is_not_null())),
            AI_FEATURE = pl.concat_list(cs.starts_with('[63]'))
                .list.eval(pl.element().filter(pl.element().is_not_null())),
        )
        .with_columns(
            CS_LANG_COUNT = pl.col('CS_LANG').list.len(),
            AI_ASST_COUNT = pl.col('AI_ASST').list.len(),
            AI_FEATURE_COUNT = pl.col('AI_FEATURE').list.len(),
        )
        .with_columns(
            CS_LANG_WT = pl.when(pl.col('CS_LANG_COUNT') > 0)
                .then(1/pl.col('CS_LANG_COUNT')).otherwise(pl.lit(0)),
            AI_ASST_WT = pl.when(pl.col('AI_ASST_COUNT') > 0)
                .then(1/pl.col('AI_ASST_COUNT')).otherwise(pl.lit(0)),
            AI_FEATURE_WT = pl.when(pl.col('AI_FEATURE_COUNT') > 0)
                .then(1/pl.col('AI_FEATURE_COUNT')).otherwise(pl.lit(0)),
        )
        .drop_nulls('GENDER')
        .filter(pl.col('EDUCATION') != 'Other')
        .select(
            'COUNTRY',      'GENDER',             'AGE_RANGE', 
            'EDUCATION',    'JOB_LEVEL',          'YEARS_EXPERIENCE', 
            'CS_LANG' ,     'CS_LANG_COUNT',      'CS_LANG_WT',
            'AI_ASST',      'AI_ASST_COUNT',      'AI_ASST_WT',
            'AI_FEATURE',   'AI_FEATURE_COUNT',   'AI_FEATURE_WT',
        )
        .with_columns(cs.float().cast(pl.Float32))
        .with_columns(cs.integer().cast(pl.UInt8))
        .with_row_index(name='INDEX', offset=1)
        .with_columns(pl.col('INDEX').cast(pl.UInt16))
        .explode('CS_LANG') 
        .explode('AI_ASST')
        .explode('AI_FEATURE')
        .with_columns(   # shorten the long name items
            pl.col('CS_LANG')
            .replace('Clojure / ClojureScript','Clojure')
            .replace("I don't use programming languages",'None Used')
            .replace('Platform-tied language (Apex, ABAP, 1C)','Plaform Based')
            .replace(
                'SQL (PL / SQL, T-SQL, and other programming extensions of SQL)',
                'SQL'
            )
            .replace(
                'Shell scripting languages (Bash / Shell / PowerShell)',
                'Shell Scripts'
            )
        )
        .with_columns(   # shorten the long name items
            pl.col('AI_ASST')
            .replace('Dream Studio (Stable Diffusion)','Dream Studio')
            .replace('JetBrains AI Assistant','Jetbrains')
            .replace('Visual Studio IntelliCode','Visual Studio')
        )
        .with_columns(   # shorten the long name items
            pl.col('AI_FEATURE')
            .replace(
                'Assistive technologies (for example, AI-powered text-to-speech and speech-to-text tools)',
                'Assistive tech'
            )
            .replace(
                'Asking general questions about software development in natural languages',
                'Natural Lang Questions'
            )
            .replace(
                'Data analytics for educational insights',
                'Data analytics'
            )
            .replace(
                'Educational content recommendations',
                'Educational content'
            )
            .replace(
                'Explaining exceptions and errors and offering fixes for them',
                'Exceptions, errors'
            )
            .replace(
                'Generating CLI commands by natural language description',
                'CLI commands'
            )
            .replace(
                'Generating code comments, documentation or commit messages',
                'Comments, commits'
            )
            .replace(
                'Help in choosing framework-related settings and methods',
                'Framework, settings'
            )
            .replace(
                'Performing code reviews',
                'Code review'
            )
            .replace(
                "I don’t use AI-based assistants",
                "Don't use"
            )
            .replace(
                'Language translation and pronunciation',
                'Foreign Language'
            )
            .replace(
                'Natural Lang Questions',
                'Natural Language'
            )
            .replace(
                'Refactoring code',
                'Refactoring'
            )
            .replace(
                'Search in natural language queries for code fragments',
                'Code fragments'
            )            
            .replace(
                'Study planning and time management',
                'Planning, time mgmt'
            )
            .replace(
                'Summarizing recent code changes to understand what happened more quickly',
                'Code change summary'
            )
            .replace(
                'Interactive simulations',
                'Interactive sims'
            )
        )
        .with_columns(cs.string().cast(pl.Categorical()))
        .collect()  # convert Lazy Frame to Data Frame
    )

def get_histo_users(df_index, country, group_by):
    print(f'{group_by = }')
    print(f'{country = }')

    if group_by == 'AGE_RANGE':
        group_by_order = age_range_order
    elif group_by == 'YEARS_EXPERIENCE':
        group_by_order = years_experience_order
    elif group_by == 'EDUCATION':
        group_by_order = education_order
    if country == 'ALL_COUNTRIES':
        df_histo = df_index  # no filtering when country is ALL_COUNTRIES 
    else:   # only use data from selected country
        df_histo = df_index.filter(pl.col('COUNTRY') == country)
    
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

def get_pareto(country, metric):
    print(f'{country = }')
    print(f'{metric = }')
    if country == 'ALL_COUNTRIES':
        df_pareto = df_global  # no filtering when country is ALL_COUNTRIES 
    else:   # only use data from selected country
        df_pareto = df_global.filter(pl.col('COUNTRY') == country)
    
    df_pareto = (
        df_pareto.filter(pl.col(metric).is_not_null())
        .select('INDEX', 'COUNTRY', 'GENDER' , metric, metric+'_WT')
        .group_by(['COUNTRY', 'GENDER', metric]).agg(pl.col(metric+'_WT').sum())
        .with_columns(TOTAL_WT = pl.col(metric+'_WT').sum().over(metric))
        .sort('TOTAL_WT',descending=False)
        # .tail(10)
    ) 
    print('df_pareto')
    print(df_pareto)
    country_title = country.title().replace('_', ' ')
    
    fig = px.bar(
        df_pareto,
        x= metric+'_WT',
        y=metric,
        color='GENDER',
        title=(
            f'{country_title}:  {metric} Top 10 PARETO'
            .upper()
        ),
        template=viz_template,
        orientation='h'
    )
    fig.update_layout(
        # x-title obvious, use empty string to make room for bottom legend
        # yaxis_title = 'USER COUNT', xaxis_title = metric, 
        legend=dict(
            yanchor='top', 
            xanchor='left',
            y=1.12,
            x=-0.02,
            orientation="h",
        ),
    )
    # fig.update_yaxes(range=(-.5, 6.5))
    return fig



def get_choro(df_choro, country):
    if country != 'ALL_COUNTRIES':
        df_choro = df_choro.filter(pl.col('COUNTRY') == country)
    df_choro = (
        df_choro
        .select('COUNTRY', 'GENDER')
        .with_columns(
            COUNT = pl.col('GENDER').count().over(['COUNTRY', 'GENDER'])
        )
    )
    fig_choro = px.choropleth(
        df_choro,   # .group_by('COUNTRY').agg(pl.col('GENDER').sum()),
        locations='COUNTRY', 
        locationmode='country names', 
        scope='world',
        color='COUNTRY',
        template=viz_template,
        title=(f'{country.upper()}'),
        labels='COUNTRY',
        projection="natural earth"
    )
    fig_choro.update_traces(
        showlegend=False,
    )
    fig_choro.update_geos(
        resolution=110,
        # showcoastlines=True, coastlinecolor="RebeccaPurple",
        showland=True, landcolor="LightGreen",
        showocean=True, oceancolor="LightBlue",
        # showlakes=True, lakecolor="Blue",
        # showrivers=True, rivercolor="Blue"
    )

    fig_choro.update(layout_coloraxis_showscale=False)
    # fig_choro.update_layout(show_legend=False)
    return fig_choro

#----- GATHER AND CLEAN DATA ---------------------------------------------------
if False: # os.path.exists('df.parquet'):     # read parquet file if it exists
    print('reading data from parquet file')
    df_global=pl.read_parquet('df.parquet')
else:                                # read csv file, clean, save df as parquet
    df_global = read_and_clean_csv()
    print(df_global)
    df_global.write_parquet('df.parquet')

cols = ['CS_LANG', 'AI_ASST', 'AI_FEATURE']
for col in cols[2:3]:
    temp_list = sorted(
        df_global
        .drop_nulls(subset = col)
        .unique(col)
        .get_column(col)
        .to_list()
    )
    print(f'{col = }')
    print(f'unique values of {col}')
    for i in temp_list:
        print(f'{i}: {len(i) = }')
    print(temp_list)

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
        size='sm',
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
        size='sm',
    ),
)

dmc_select_metric = (
    #  look at data for CS_LANG, AI_ASST, or AI_FEATURE
    dmc.Select(
        label='Pick one of these metrics:',
        placeholder="Select one",
        id='metric',
        data= ['CS_LANG', 'AI_ASST', 'AI_FEATURE'],
        value='CS_LANG',
        size='sm',
    ),
)

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    # dmc.Space(h=30),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Gender Patterns for AI Adoption', ta='center', style=style_h2),
    dmc.Text('', ta='center', style=style_h3, id='zip_code'),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_country, span=4, offset = 1),
        dmc.GridCol(dmc_select_group_by, span=4, offset = 1),
    ]),  
    dmc.Space(h=0),
    html.Hr(style=style_horizontal_thin_line),
  
    dmc.Grid(children = [
            dmc.GridCol(dcc.Graph(id='choro'), span=6, offset=0),            
            dmc.GridCol(dcc.Graph(id='histogram-users'), span=6, offset=0), 
        ]),
    dmc.Space(h=0),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_metric, span=3, offset = 1),
    ]),
    dmc.Grid(children = [
            dmc.GridCol(dcc.Graph(id='pareto'), span=6, offset=0),            
            dmc.GridCol(dcc.Graph(id='bar-bell'), span=6, offset=0), 
        ]),
])

@app.callback(
    Output('histogram-users', 'figure'),
    Output('choro', 'figure'),
    Output('pareto', 'figure'),
    Output('bar-bell', 'figure'),
    Input('country', 'value'),
    Input('group-by', 'value'),
    Input('metric', 'value')
)
def update(country, group_by, metric):
    print(f'{group_by = }')
    print(f'{country = }')
    print(f'{metric = }')
    if country == None:
        country = country_list[0]
    df_index=df_global.unique('INDEX')
    histo_users = get_histo_users(df_index, country, group_by)
    choro = get_choro(df_index, country)
    pareto=get_pareto(country, metric)
    bar_bell = histo_users   # place holder


    return histo_users, choro, pareto, bar_bell

if __name__ == '__main__':
    app.run(debug=True)