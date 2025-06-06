import polars as pl
import polars.selectors as cs
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_ag_grid as dag
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

print(f'{dag.__version__ = }')
#----- GLOBALS -----------------------------------------------------------------
org_col_names  = [c for c in pl.scan_csv('Marvel-Movies.csv').collect_schema()]
short_col_names =[
    'FILM', 'FRANCHISE', 'WW_GROSS', 'BUD_PCT_REC', 'CRIT_PCT_SCORE', 'AUD_PCT_SCORE', 
    'CRIT_AUD_PCT', 'BUDGET', 'DOM_GROSS', 'INT_GROSS', 'WEEK1', 'WEEK2',
    'WEEK2_DROP_OFF', 'GROSS_PCT_OPEN', 'BUD_PCT_OPEN', 'YEAR', 'SOURCE'
]
dict_cols = dict(zip(org_col_names, short_col_names))
dict_cols_reversed = dict(zip(short_col_names, org_col_names))
print(f'{len(dict_cols)}')
print(f'{dict_cols = }')
#----- READ & CLEAN DATASET ----------------------------------------------------
df_global = (
    pl.read_csv('Marvel-Movies.csv')
    .rename(dict_cols)
    .drop('SOURCE')
    .filter(pl.col('FRANCHISE') != 'Unique')
    .sort('FRANCHISE', 'YEAR')
    .with_columns(
        cs.string().exclude(['FILM', 'FRANCHISE'])
            .str.replace_all(r'%', '')
            .cast(pl.Float64())
            .mul(0.01)  # divide by 100 for proper percentage format
            .round(3),
        SERIES_NUM = pl.cum_count('FILM').over('FRANCHISE').cast(pl.String)
    )
    .select(
        'FRANCHISE', 'FILM', 'YEAR', 'SERIES_NUM', 'AUD_PCT_SCORE', 'BUDGET', 
        'BUD_PCT_OPEN', 'BUD_PCT_REC', 'CRIT_AUD_PCT', 'CRIT_PCT_SCORE', 
        'DOM_GROSS', 'GROSS_PCT_OPEN', 'INT_GROSS', 'WEEK1', 'WEEK2', 
        'WEEK2_DROP_OFF', 'WW_GROSS'
    )
)
print(df_global.glimpse())
  

df_franchise = (
    df_global
    .select('FRANCHISE', 'SERIES_NUM', 'YEAR', 'FILM')
)

print(df_franchise)

film_list = sorted(df_global.unique('FILM')['FILM'].to_list())
franchise_list = sorted(df_global.unique('FRANCHISE')['FRANCHISE'].to_list())
plot_cols = sorted(df_global.select(cs.numeric().exclude('YEAR')).columns)

style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}

#----- GENERAL FUNCTIONS  ------------------------------------------------------
def get_film_data(film, item):
    print(f'{film = }')
    print(f'{item = }')
    result = (
        df_global
        .filter(pl.col('FILM') == film)
        #.select(item)
        [item]
        [0]
    )
    print(f'{type(result) = }')
    print(f'{result = }')
    return result
# (
#         df_global
#         .filter(pl.col(item) == film)
#         .select(item)
#         [item]
#         [0]
#     )

def get_franchise(film):    
    return(
        df_global
        .filter(pl.col('FILM') ==  film)
        .head(1)
        ['FRANCHISE']
        [0]
    )

#----- DASHBOARD COMPONENTS ----------------------------------------------------
grid = (
    dag.AgGrid(
        rowData=df_global.to_dicts(), 
        columnDefs=[
            {
                'field': i,
                'filter': True,
                'sortable': True,
                'tooltipField': i,
                'headerTooltip': dict_cols_reversed.get(i),
            } 
            for i in df_global.columns
        ],
        dashGridOptions={
            'pagination': False,
            'rowSelection': "multiple", 
            'suppressRowClickSelection': True, 
            'animateRows' : False
        },
        columnSize='autoSize',
        columnSizeOptions={'skipHeader':True},
        id='dash_ag_table'
    ),
)

franchise_dag_table = (
    dag.AgGrid(
        rowData=df_franchise.to_dicts(), 
        columnDefs=[
            {
                'field': i,
                'filter': True,
                'sortable': True,
                'tooltipField': i,
                'headerTooltip': dict_cols_reversed.get(i),
            } 
            for i in df_franchise.columns
        ],
        dashGridOptions={
            'pagination': False,
            'rowSelection': "multiple", 
            'suppressRowClickSelection': True, 
            'animateRows' : False
        },
        columnSize='autoSize',
        columnSizeOptions={'skipHeader':True},
        id='franchise_dag_table'
    ),
)
dmc_select_param = (
    dmc.Select(
        label='Select a Parameter',
        # placeholder="Select one",
        id='dmc_select_parameter',
        value='WW_GROSS',
        data=[{'value' :i, 'label':i} for i in plot_cols],
        maxDropdownHeight=600,
        w=300,
        mb=10, 
        size='xl'
    ),
)
dmc_select_film = (
    dmc.Select(
        label='Select a Film',
        id='dmc_select_film',
        value='Ant-Man',
        data=[{'value' :i, 'label':i} for i in film_list],
        maxDropdownHeight=600,
        mb=30, 
        size='xl'
    ),
)

franchise_card =  dmc.Card(
    children = [
        dmc.CardSection(
            children = [
                dmc.List(
                    children = [
                    dmc.ListItem('', id='slope'),    
                    dmc.ListItem('', id='intercept'), 
                    dmc.ListItem('', id='correlation'), 
                    dmc.ListItem('', id='stderr'),
                    dmc.ListItem('', id='i_stderr')
                    ],
                size='lg'
                )
            ]
        )
    ]
)

card_names = ['FRANCHISE', 'YEAR', 'SERIES_NUM', 'AUD_PCT_SCORE', 'BUDGET', 
    'BUD_PCT_OPEN', 'BUD_PCT_REC', 'CRIT_AUD_PCT', 'CRIT_PCT_SCORE', 
    'DOM_GROSS', 'GROSS_PCT_OPEN', 'INT_GROSS', 'WEEK1', 'WEEK2', 
    'WEEK2_DROP_OFF', 'WW_GROSS'
]
card_list = []
for card in card_names:
    card_list.append(
        dmc.Card(
        children = [
            dmc.Group(
                [
                    dmc.Text(card.title().replace('_', ' '), fw=500),
                ],
                justify="space-between",
                mt="md",
                mb="xs",
            ),
            dmc.Text(
                card,
                size='lg',
                # Text size="xl"
                id=card.lower()
            ),
        ],
        withBorder=True,
        shadow='lg',
        radius='lg',
        # styles={
        #     "root": {'backgroundColor': 'red'},
        #     'label': {'color': 'blue', 'fontSize': 50}
        #     # nner": {"fontSize": 50},
        # },
        )
    )
print(f'{len(card_list) = }')
# franchise_name_card =  dmc.Card(
#     children = [
#         dmc.Group(
#             [
#                 dmc.Text('Franchise', fw=500),
#             ],
#             justify="space-between",
#             mt="md",
#             mb="xs",
#         ),
#         dmc.Text(
#             franchise_list[0],
#             size='lg',
#             id='franchise'
#         ),
#     ],
#     withBorder=True,
#     shadow='lg',
#     radius="md",
# )

#----- CALLBACK FUNCTIONS ------------------------------------------------------
def get_plot(plot_parameter, mode):
    if mode == 'DATA':
        df_plot = (
            df_global
            .select('FILM', 'FRANCHISE', 'YEAR', 'SERIES_NUM', plot_parameter)
            .pivot(
                on='FRANCHISE',
                values=plot_parameter,
                index='SERIES_NUM'
            )
        )
    elif mode == 'NORMALIZED':
        df_plot = (
            df_global
            .select('FILM', 'FRANCHISE', 'YEAR', 'SERIES_NUM', plot_parameter)
            .pivot(
                on='FRANCHISE',
                values=plot_parameter,
                index='SERIES_NUM'
            )
            .with_columns(
                ((pl.col(franchise_list) - pl.col(franchise_list).first()) /
                pl.col(franchise_list).first()).mul(100)
            )
        )
    else:
        print(f'{mode = } is not supported !!!!')
    fig=px.line(
        df_plot,
        'SERIES_NUM',
        franchise_list,
        markers=True,
        line_shape='spline',
        title=f'{plot_parameter}_{mode}'
    )

    fig.update_layout(
        template='simple_white',
        # title_text=plot_parameter,
        yaxis=dict(title=dict(text=f'{plot_parameter} {mode}')),
        legend_title='FRANCHISE'
    )
    return fig

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horiz_line),
    dmc.Text('Marvelous Sequels', ta='center', style=style_h2),
    html.Hr(style=style_horiz_line),
    #html.Div(),
    dmc.Space(h=30),
    dmc.Grid(
        children = [
            dmc.GridCol(dmc_select_param, span=4, offset = 1),
            # dmc.GridCol(radio_graph_type, span=3, offset = 1),
        ]
    ),
    dmc.Grid(
        children = [
            dmc.GridCol(dcc.Graph(id='graph_plot'), span=5, offset = 1),
            dmc.GridCol(dcc.Graph(id='graph_norm'), span=5, offset = 0),
        ]
    ),
    # html.Div(),
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('Mantine Cards', ta='center', style=style_h2, id='mantine_cards'),
    html.Hr(style=style_horiz_line),
    # html.Div(),
    dmc.Space(h=30),
    dmc.Grid(
        children = [
            dmc.GridCol(dmc_select_film, span=3, offset = 1),
        ]
    ),
    dmc.Grid(
        children = [
            dmc.GridCol(card_list[0], span=1, offset = 1),
            dmc.GridCol(card_list[1], span=1, offset = 0),
            dmc.GridCol(card_list[2], span=1, offset = 0),
            dmc.GridCol(card_list[3], span=1, offset = 0),
            dmc.GridCol(card_list[4], span=1, offset = 0),
            dmc.GridCol(card_list[5], span=1, offset = 0),
            dmc.GridCol(card_list[6], span=1, offset = 0),
            dmc.GridCol(card_list[7], span=1, offset = 0),
        ]
    ),
    dmc.Grid(
        children = [
            dmc.GridCol(card_list[8], span=1, offset = 1),
            dmc.GridCol(card_list[9], span=1, offset = 0),
            dmc.GridCol(card_list[10], span=1, offset = 0),
            dmc.GridCol(card_list[11], span=1, offset = 0),
            dmc.GridCol(card_list[12], span=1, offset = 0),
            dmc.GridCol(card_list[13], span=1, offset = 0),
            dmc.GridCol(card_list[14], span=1, offset = 0),
            dmc.GridCol(card_list[15], span=1, offset = 0),
        ]
    ),
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('Dash AG Tables', ta='center', style=style_h2, id='data_and_defs'),
    html.Hr(style=style_horiz_line),
    dmc.Space(h=30),
    dmc.Grid(children = [
        dmc.GridCol(grid, span=5, offset = 1),
        dmc.GridCol(franchise_dag_table, span=4, offset = 1),
        ]
    ),
    dmc.Space(h=50),
])

@app.callback(
    Output('graph_plot', 'figure'),
    Output('graph_norm', 'figure'),
    Output('franchise','children'),
    Output('year','children'),
    Output('series_num','children'),
    Output('aud_pct_score','children'),
    Output('budget','children'),
    Output('bud_pct_open','children'),
    Output('bud_pct_rec','children'),
    Output('crit_aud_pct','children'),
    Output('crit_pct_score','children'),
    Output('dom_gross','children'),
    Output('gross_pct_open','children'),
    Output('int_gross','children'),
    Output('week1','children'), 
    Output('week2','children'),
    Output('week2_drop_off','children'),
    Output('ww_gross','children'),
    Output('mantine_cards','children'),
    Output('franchise_dag_table','rowData'),

    Input('dmc_select_parameter', 'value'),
    Input('dmc_select_film', 'value'),
)
def update_dashboard(parameter, film):
    print(f'{parameter = }')
    print(f'{film = }')
    franchise = get_franchise(film)


    return (
        get_plot(parameter, 'DATA'),
        get_plot(parameter, 'NORMALIZED'),
        get_film_data(film, 'FRANCHISE'),
        get_film_data(film, 'YEAR'),
        get_film_data(film, 'SERIES_NUM'),
        f'{get_film_data(film, 'AUD_PCT_SCORE'):.0%}',
        f'{get_film_data(film, 'BUDGET'):.0f} M$',
        f'{get_film_data(film, 'BUD_PCT_OPEN'):.0%}',
        f'{get_film_data(film, 'BUD_PCT_REC'):.0%}',
        f'{get_film_data(film, 'CRIT_AUD_PCT'):.0%}',
        f'{get_film_data(film, 'CRIT_PCT_SCORE'):.0%}',
        f'{get_film_data(film, 'DOM_GROSS'):.0f} M$',
        f'{get_film_data(film, 'GROSS_PCT_OPEN'):.0f}%',
        f'{get_film_data(film, 'INT_GROSS'):.0f} M$',
        f'{get_film_data(film, 'WEEK1'):.0f} M$',
        f'{get_film_data(film, 'WEEK2'):.0f} M$',
        f'{get_film_data(film, 'WEEK2_DROP_OFF'):.0%}',
        f'{get_film_data(film, 'WW_GROSS'):.0f} M$',
        f'Movie Cards for {film}',
        (
            df_franchise
            .filter(pl.col('FRANCHISE') == franchise)
            .sort('SERIES_NUM')
            .to_dicts()
        )
    )
if __name__ == '__main__':
    app.run_server(debug=True)