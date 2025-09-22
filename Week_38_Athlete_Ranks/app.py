import polars as pl
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc

pl.Config().set_tbl_cols(10)

dash._dash_renderer._set_react_version('18.2.0')

'''
    This dashboard has timeline plots of RANK by year and by calendar year. 
    added column RANK_MED is the median rank of each athlete
    added column RANK_MEAN is the mean rank of each athlete
    athletes within each league are ranked by RANK_MED, and then by RANK_MEAN.
    The top 5 athletes are included in this dashboard. Could be ehanced for 
    analysis of lower ranked athletes
'''

viz_template = 'plotly_dark'

#----- LOAD AND CLEAN THE DATASET
df_league_names = pl.DataFrame({
    'LEAGUE_ABBR': ['ATP', 'LPGA', 'MLB', 'NBA', 'NHL', 'PGA', 'WNBA', 'WTA'],
    'LEAGUE_NAME': [
        'Association of Tennis Professionals', 
        'Ladies Professional Golf Association', 
        'Major League Baseball', 
        'National Basketball Association', 
        'National Hockey League', 
        'Professional Golf Association', 
        'Womens National Basketball Association',  
        'Womens Tennis Association'
        ]
})
dict_league_abbr_name = dict(zip(
    df_league_names['LEAGUE_ABBR'], df_league_names['LEAGUE_NAME']))

df  = (
    pl.scan_csv('one-hit-wonders.csv')
    .rename(  # upper case all column names, replace spaces with underscores
        lambda c: 
            c.upper()            # column names to upper case
            .replace(' ', '_')   # blanks replaced with underscores
    )
    .filter(~ pl.col('DNP'))
    .select(
        NAME = pl.col('NAME').str.to_titlecase(),
        FAMILY_NAME = pl.col('NAME').str.to_titlecase().str.split(' ' ).list.last(),
        NAME_COUNT = pl.len().over('NAME').cast(pl.UInt8),
        YEAR = pl.col('YEAR').cast(pl.UInt16),
        YEARS_TOT = pl.col('YEAR').cast(pl.UInt16).len().over('NAME'),
        CAREER_YEAR = (1 + pl.col('YEAR_INDEX')),
        PEAK_CAREER_YEAR = pl.col('PEAK_YEAR_INDEX'),
        TEAM = pl.col('TEAM'),
        PLAYED_VAL = pl.col('PLAYED_VAL').cast(pl.UInt16),
        RANK = pl.col('RANK'),
        RANK_MED = pl.col('RANK').cast(pl.UInt16).median().over('NAME', 'LEAGUE').cast(pl.Float32),
        RANK_MEAN = pl.col('RANK').cast(pl.UInt16).mean().over('NAME').cast(pl.Float32),
        FIRST_YEAR = pl.col('YEAR').min().over('NAME').cast(pl.UInt16),
        LEAGUE_ABBR = pl.col('LEAGUE').str.to_uppercase(),
        NAME_ORG = (  # form of NAME_ORG is NBA: Stephen Curry,  etc.
            pl.col('LEAGUE').str.to_uppercase() + pl.lit(': ') +
            pl.col('NAME').str.to_titlecase()
        )
    )
    .filter(pl.col('YEARS_TOT') >= 5)
    .sort('LEAGUE_ABBR', 'RANK_MED', 'RANK_MEAN')
    .collect()
    .join(df_league_names, on='LEAGUE_ABBR', how='left')
)

top_5_list = sorted(list(
    df
    .lazy()
    .unique('NAME_ORG')    
    .sort('LEAGUE_ABBR', 'RANK_MED', 'RANK_MEAN')
    .with_columns(
        RANK_LEAGUE = 
            pl.col('LEAGUE_ABBR')
            .cum_count()
            .over('LEAGUE_ABBR')
            .cast(pl.UInt8)
        )
    .filter(pl.col('RANK_LEAGUE') <= 5)
    .collect()
    ['NAME_ORG']
))

#----- FUNCTIONS ---------------------------------------------------------------
def get_tl_by_year(df_callback, name_org_list):
    # timeline by calendar year
    fig = px.line(
        df_callback.sort('YEAR'),
        x='YEAR',
        y='RANK',
        color='NAME_ORG',
        template=viz_template,
        markers=True,
        line_shape='spline',
        category_orders={'NAME_ORG': name_org_list}, # handy way to sort legend
        log_y=True,
        title='Timeline by calendar year',
    )
    fig.update_layout(
        xaxis_title='CALENDAR YEAR', yaxis_title='RANK -- LOG SCALE',
        legend_title = 'Athlete'
        )
    # high rankings gets a low rank numbers. Reverse y to put #1 at the top
    fig.update_yaxes(autorange='reversed')
    fig.update_xaxes(showgrid=False)  # y gridlines kept becuase of log scale
    return fig

def get_tl_by_career_year(df_callback, name_org_list):
    # timeline by career year
    fig = px.line(
        df_callback.sort('YEAR'),
        x='CAREER_YEAR',
        y='RANK',
        color='NAME_ORG',
        template=viz_template,
        markers=True,
        line_shape='spline',
        category_orders={'NAME_ORG': name_org_list}, # handy way to sort legend
        log_y=True,
        title='Timeline by career year'
    )
    fig.update_layout(
        xaxis_title='CAREER YEAR', yaxis_title='RANK -- LOG SCALE',
        legend_title = 'Athlete'
        )
    # high rankings gets a low rank numbers. Reverse y to put #1 at the top
    fig.update_yaxes(autorange='reversed')
    fig.update_xaxes(showgrid=False)  # y gridlines kept becuase of log scale
    return fig

#----- GLOBALS -----------------------------------------------------------------
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold', 'color': 'gray'}

#----- DASH COMPONENTS------ ---------------------------------------------------
data = [[i,v] for i, v in dict_league_abbr_name.items()]
dmc_select_league = dmc.Box([
        dmc.Group(
            dmc.ChipGroup(
                [dmc.Chip(k, value=v) for k,v in dict_league_abbr_name.items()],
                multiple=True,
                deselectable=True,
                value=[df_league_names.item(0, 'LEAGUE_NAME')],
                id='select-leagues',
            ),
            justify='left',
        ),
    ])

dmc_selected_leagues = dmc.GridCol(
    dmc.GridCol(
        dmc.Text(
            id='selected_leagues',
            style={'fontSize': '18px','color': 'blue'}
        ), 
    )
)
#----- DASH APPLICATION STRUCTURE---------------------------------------------
app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Consistency - top 5 athletes per league', ta='center', style=style_h2),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Grid(children = [dmc.GridCol(dmc_select_league, span=8, offset = 1)]),
    dmc.Grid(children = [dmc.GridCol(dmc_selected_leagues, span=8, offset = 1)]),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='by-year'), span=6, offset=0),            
        dmc.GridCol(dcc.Graph(id='by-career-year'), span=6, offset=0), 
    ]),
])

@app.callback(
    Output('selected_leagues', 'children'), 
    Output('by-year', 'figure'), 
    Output('by-career-year', 'figure'), 
    Input('select-leagues', 'value')
)
def choose_framework(value):
    print(f'{value = }')
    df_callback=(
        df
        .filter(pl.col('LEAGUE_NAME').is_in(value))
        .filter(pl.col('NAME_ORG').is_in(top_5_list))
    )
    name_org_list = (
        df_callback
        .unique('NAME_ORG')
        .sort('LEAGUE_NAME', 'FAMILY_NAME')
        ['NAME_ORG']
        .to_list()
    )
    tl_by_year= get_tl_by_year(df_callback, name_org_list)
    tl_by_career_year= get_tl_by_career_year(df_callback, name_org_list)
    return  (
        ', '.join(sorted(value)),
        tl_by_year,
        tl_by_career_year
    )
if __name__ == '__main__':
    app.run(debug=True)