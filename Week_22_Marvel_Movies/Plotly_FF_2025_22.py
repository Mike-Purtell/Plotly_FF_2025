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
    'WEEK2_DROP_OFF', 'GROSS_PCT_OPEN', 'BUDGET_PCT_OPEN', 'YEAR', 'SOURCE'
]
dict_cols = dict(zip(org_col_names, short_col_names))
dict_cols_reversed = dict(zip(short_col_names, org_col_names))

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
        'BUDGET_PCT_OPEN', 'BUD_PCT_REC', 'CRIT_AUD_PCT', 'CRIT_PCT_SCORE', 
        'DOM_GROSS', 'GROSS_PCT_OPEN', 'INT_GROSS', 'WEEK1', 'WEEK2', 
        'WEEK2_DROP_OFF', 'WW_GROSS'
    )
)
film_list = sorted(df_global.unique('FILM')['FILM'].to_list())
franchise_list = sorted(df_global.unique('FRANCHISE')['FRANCHISE'].to_list())
plot_cols = sorted(df_global.select(cs.numeric().exclude('YEAR')).columns)

style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}

#----- GENERAL FUNCTIONS  ------------------------------------------------------

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

dmc_select_param = (
    dmc.Select(
        label='Select a Parameter',
        # placeholder="Select one",
        id='dmc_select_parameter',
        value='WW_GROSS',
        data=[{'value' :i, 'label':i} for i in plot_cols],
        w=200,
        mb=10, 
    ),
)
dmc_select_film = (
    dmc.Select(
        label='Select a Film',
        # placeholder="Select Film",
        id='dmc_select_film',
        value='Ant-Man',
        data=[{'value' :i, 'label':i} for i in film_list],
        w=200,
        mb=10, 
    ),
)

film_card =  dmc.Card(
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
    html.Div(),
    dmc.Grid(
        children = [
            dmc.GridCol(dmc_select_param, span=3, offset = 1),
            # dmc.GridCol(radio_graph_type, span=3, offset = 1),
        ]
    ),
    dmc.Grid(
        children = [
            dmc.GridCol(dcc.Graph(id='graph_plot'), span=5, offset = 1),
            dmc.GridCol(dcc.Graph(id='graph_norm'), span=5, offset = 0),
        ]
    ),
    html.Div(),
    html.Hr(style=style_horiz_line),
    dmc.Text('Data and Definitions', ta='center', style=style_h2, id='data_and_defs'),
    html.Hr(style=style_horiz_line),
    html.Div(),
    dmc.Grid(
        children = [
            dmc.GridCol(dmc_select_film, span=3, offset = 7),
            # dmc.GridCol(radio_graph_type, span=3, offset = 1),
        ]
    ),
    dmc.Grid(children = [
        dmc.GridCol(grid, span=6, offset = 1),
        dmc.GridCol(film_card, span=3, offset = 6),
        # dmc.GridCol(franchise_card, span=2, offset = 10)
        ]
    )
])

@app.callback(
    Output('graph_plot', 'figure'),
    Output('graph_norm', 'figure'),
    # Output('dash_ag_table', 'rowData'),
    # # Output('data_and_defs','children'),

    Input('dmc_select_parameter', 'value'),
    Input('dmc_select_film', 'value'),
    # Input('dash_ag_table', 'rowData'),

    # Input('dash_ag_table', 'rowData'),
)
def update_dashboard(parameter, film):
    print(f'{parameter = }')
    print(f'{film = }')
    # print('\n\n')
    return (
        get_plot(parameter, 'DATA'),
        get_plot(parameter, 'NORMALIZED'),
        # df_global.to_dicts()
    )
if __name__ == '__main__':
    app.run_server(debug=True)