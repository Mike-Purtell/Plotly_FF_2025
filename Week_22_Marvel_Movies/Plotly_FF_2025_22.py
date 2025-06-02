import polars as pl
import polars.selectors as cs
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_ag_grid as dag
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

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
)

franchise_list = sorted(df_global.unique('FRANCHISE')['FRANCHISE'].to_list())
plot_cols = sorted(df_global.select(cs.numeric().exclude('YEAR')).columns)

dag_columns = [  # TODO FIX ME
    'DECADE', 'YEAR', 'CEMENT', 'FLARING', 'FOSSIL', 
    'GAS', 'LIQUID', 'SOLID', 'PER_CAP', ]

style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}

#----- GENERAL FUNCTIONS  ------------------------------------------------------

#----- DASHBOARD COMPONENTS ----------------------------------------------------
grid = dag.AgGrid(
    rowData=[],
    columnDefs=[
        {"field": i, 'filter': True, 'sortable': True,
         'tooltipField': i, 'headerTooltip': dict_cols_reversed.get(i) } 
        for i in dag_columns
    ],
    dashGridOptions={"pagination": True},
    columnSize="responsiveSizeToFit",
    id='dash_ag_table',
)

dmc_select = (
    dmc.Select(
        label='Parameter to Analyze',
        placeholder="Select one",
        id='dmc_select_parameter',
        value='WW_GROSS',
        data=[{'value' :i, 'label':i} for i in plot_cols],
        w=200,
        mb=10, 
    ),
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
            dmc.GridCol(dmc_select, span=3, offset = 1),
            # dmc.GridCol(radio_graph_type, span=3, offset = 1),
        ]
    ),
    dmc.Grid(
        children = [
            dmc.GridCol(dcc.Graph(id='graph_plot'), span=5, offset = 1),
            dmc.GridCol(dcc.Graph(id='graph_norm'), span=5, offset = 1),
        ]
    ),
])

@app.callback(
    Output('graph_plot', 'figure'),
    Output('graph_norm', 'figure'),
    # Output('dash_ag_table', 'rowData'),
    # Output('data_and_defs','children'),

    Input('dmc_select_parameter', 'value'),
)
def update_dashboard(parameter_name):
    return (
        get_plot(parameter_name, 'DATA'),
        get_plot(parameter_name, 'NORMALIZED'),
    )
if __name__ == '__main__':
    app.run_server(debug=True)