import polars as pl
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
import dash_ag_grid as dag
dash._dash_renderer._set_react_version('18.2.0')

#  Plot Rank by Year, and Rank by Career Year.
#  Figure out a good way to filter. Ideas are:
#       filter by league
#       Players with 5 or more years
#       Players who acheived top xx for yy or more years

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
    pl.read_csv('one-hit-wonders.csv')
    .rename(  # upper case all column names, replace spaces with underscores
        lambda c: 
            c.upper()            # column names to upper case
            .replace(' ', '_')   # blanks replaced with underscores
    )
    .filter(~ pl.col('DNP'))
    .select(
        NAME = pl.col('NAME').str.to_titlecase(),
        NAME_COUNT = pl.len().over('NAME').cast(pl.UInt8),
        YEAR = pl.col('YEAR').cast(pl.UInt16),
        CAREER_YEAR = (1 + pl.col('YEAR_INDEX')),
        PEAK_CAREER_YEAR = pl.col('PEAK_YEAR_INDEX'),
        SPORT = pl.col('SPORT_NAME').str.to_titlecase(),
        TEAM = pl.col('TEAM'),
        PLAYED_VAL = pl.col('PLAYED_VAL').cast(pl.UInt16),
        RANK = pl.col('RANK').cast(pl.UInt16),
        HIGHEST_RANK = pl.col('RANK').min().over('NAME').cast(pl.UInt8),
        FIRST_YEAR = pl.col('YEAR').min().over('NAME').cast(pl.UInt16),
        LEAGUE_ABBR = pl.col('LEAGUE').str.to_uppercase(),
    )
    .join(df_league_names, on = 'LEAGUE_ABBR', how='left')
    .filter(pl.col('HIGHEST_RANK') <= 10)
     # .sort('')
)
print(df.shape)
print(df.glimpse())

# #----- FUNCTIONS ---------------------------------------------------------------
def get_card(title, value, id=''):
    card_bg_color = '#F5F5F5'
    return(
        dmc.Card(children=[
            dmc.Text(f'{title}', ta='center', fz=24),
            dmc.Text(f'{value}', ta='center', fz=20, c='blue', id=id,),
        ],
        style={'backgroundColor': card_bg_color}, 
        #mx is left & right margin, my top & bottom margin
        withBorder=True, shadow='sm', radius='xl', mx=2, my=2
        )
    )

#----- GLOBALS -----------------------------------------------------------------
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_horizontal_thin_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 12}

style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold', 'color': 'gray'}

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
league_list = sorted(list(set(df['LEAGUE_ABBR'])))
print(f'{league_list = }')

#----- DASH COMPONENTS------ ---------------------------------------------------

data = [[i,v] for i, v in dict_league_abbr_name.items()]
print(f'{data = }')
# dmc_select_league = dmc.RadioGroup(
#     children=dmc.Group([dmc.Radio(key, value=val) for key, val in data], my=10),
#     id='select-league',
#     value=df_league_names.item(0, 'LEAGUE_NAME'),
#     label='Select a League',
#     size="sm",
#     mb=10,
# )

dmc_select_league = dmc.Box([
        dmc.Group(
            dmc.ChipGroup(
                [dmc.Chip(k, value=v) for k,v in dict_league_abbr_name.items()],
                #     # dmc.Chip("Multiple chips", value="a"),
                #     # dmc.Chip("Can be selected", value="b"),
                #     # dmc.Chip("At a time", value="c"),
                # ],
                multiple=True,
                deselectable=True,
                value=[df_league_names.item(0, 'LEAGUE_NAME')],
                id='select-league',
            ),
            justify='left',
        ),
        dmc.Text(id='selected-league', ta='center')
    ])

dmc_selected_leagues = dmc.GridCol(
    dmc.GridCol(
        dmc.Text(
            id='selected_league',
            style={'fontSize': '18px','color': 'blue'}
            ), 
    )
)


# dmc.RadioGroup(
#     children=dmc.Group([dmc.Radio(key, value=val) for key, val in data], my=10),
#     id='select-league',
#     value=df_league_names.item(0, 'LEAGUE_NAME'),
#     label='Select a League',
#     size="sm",
#     mb=10,
# )



#----- INFO CARDS --------------------------------------------------------------


#----- DASH APPLICATION STRUCTURE---------------------------------------------
app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Athletic Longevity', ta='center', style=style_h2),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_league, span=8, offset = 1),
    ]),
    dmc.Grid(children = [
        dmc.GridCol(dmc.Text('Selected Leagues: '), ta='left', span=8, offset = 1)
    ]),
    dmc.Grid(children = [ 
        dmc.GridCol(dmc_selected_leagues, span=8, offset = 1),
    ]),
    #     dmc.GridCol(
    #         dmc.Text(
    #             id='selected_league',
    #             style={
    #                 'fontSize': '18px',
    #                 'color': 'blue'
    #                 }
    #             ), 
    #         span = 8, offset=1
    #     )
    # ]),

    dmc.Grid(children = [
        dmc.GridCol(
            dmc.Text(
                id='selected_league',
                style={
                    'fontSize': '18px',
                    'color': 'blue'
                    }
                ), 
            span = 8, offset=1
        )
    ]),
])

@app.callback(
    Output('selected_league', 'children'), 
    Input('select-league', 'value')
)
def choose_framework(value):
    print(f'{value = }')
    return  ', '.join(sorted(value))

# f'Selected Leagues: .join(sorted(value))'


if __name__ == '__main__':
    app.run(debug=True)