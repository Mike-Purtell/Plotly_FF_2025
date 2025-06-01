import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_ag_grid as dag
import dash_mantine_components as dmc
from scipy import stats
dash._dash_renderer._set_react_version('18.2.0')

#----- GLOBALS -----------------------------------------------------------------
data_src ='Data Source:  https://rieee.appstate.edu/projects-programs/cdiac/'

df_col_defs  = [c for c in pl.scan_csv('global.1751_2021.csv').collect_schema()]
short_col_names = [
    'YEAR', 'FOSSIL', 'SOLID', 'LIQUID', 'GAS', 
    'CEMENT', 'FLARING', 'PER_CAP','DECADE']
short_param_names = short_col_names[1:-1] # excludes first and last list items
dict_cols = dict(zip(df_col_defs, short_col_names))
dict_cols_reversed = dict(zip(short_col_names, df_col_defs))
dag_columns = [
    'DECADE', 'YEAR', 'CEMENT', 'FLARING', 'FOSSIL', 
    'GAS', 'LIQUID', 'SOLID', 'PER_CAP', ]

style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

# dmc-
style_h2 = {'text-align': 'center', 'font-size': '32px', 'fontFamily': 'Arial','font-weight': 'bold'}
style_h3 = {'text-align': 'center', 'font-size': '16px', 'fontFamily': 'Arial','font-weight': 'normal'}

#----- FUNCTIONS  --------------------------------------------------------------
def get_corr_stat(stat, corr_x, corr_y, group_by):
    df_plot = df_year.drop_nulls([corr_x, corr_y])
    if group_by == 'DECADE':
        df_plot = df_decade.drop_nulls([corr_x, corr_y])
    res = stats.linregress(df_plot[corr_x], df_plot[corr_y])
    if stat == 'SLOPE':
        val = res.slope
    elif stat == 'INTERCEPT':  
        val = res.intercept
    elif stat == 'CORR':  
        val = res.rvalue**2
    elif stat == 'STDERR':  
        val = res.stderr
    elif stat == 'I_STDERR': 
        val = res.intercept_stderr
    else:
        print('illegal value, unrecognized stat')
        val = -999999
    return val

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

radio_group_by = dmc.RadioGroup(
    children=dmc.Group(
        [dmc.Radio(i, value=i) for i in ['DECADE', 'YEAR']], my=10
    ),
    value='YEAR',
    label= 'Group By',
    size='lg',
    mt=10,
    id='group_by_radio'
)

radio_graph_type = dmc.RadioGroup(
    children=dmc.Group(
        [dmc.Radio(i, value=i) for i in ['AREA', 'LINE']], my=10
    ),
    value='AREA',
    label= 'Plot Type',
    size='lg',
    mt=10,
    id='graph_type_radio'
)

def get_radio_corr_param(my_id, my_label, my_def):
    return (
        dmc.RadioGroup(
            children=dmc.Group(
                [dmc.Radio(i, value=i) for i in short_param_names], my=10
            ),
            value=my_def,
            label= my_label,
            size='lg',
            mt=10,
            id=my_id
        )
    )

stats_card =  dmc.Card(
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

definition_card = dmc.Card(
    children = [
        dmc.CardSection(
            children = [
                dmc.List(
                    children = [
                    dmc.ListItem(f'CEMENT: {dict_cols_reversed['CEMENT']}'),
                    dmc.ListItem(f'FLARING: {dict_cols_reversed['FLARING']}'),
                    dmc.ListItem(f'FOSSIL: {dict_cols_reversed['FOSSIL']}'),
                    dmc.ListItem(f'GAS: {dict_cols_reversed['GAS']}'),
                    dmc.ListItem(f'LIQUID: {dict_cols_reversed['LIQUID']}'),
                    dmc.ListItem(f'SOLID: {dict_cols_reversed['SOLID']}'),
                    dmc.ListItem(f'PER_CAP: {dict_cols_reversed['PER_CAP']}'),
                    ],
                size='lg'
                )
            ]
        )
    ],
)

#----- READ & CLEAN DATASET, STORE AS 4 DATAFRAMES -----------------------------
df_year = (
    pl.scan_csv('global.1751_2021.csv')  # lazyframe
    .rename(dict_cols)
    .with_columns(
        pl.col('PER_CAP').cast(pl.Float32),
        pl.col('YEAR').cast(pl.String)
    )
    .with_columns(
        DECADE = pl.col('YEAR').str.slice(0,3) +  pl.lit("0's")
    )
    .collect() # dataframe
)

df_year_diff = (
    df_year
    .lazy()    # lazyframe
    .with_columns(
        pl.col(
            'FOSSIL', 'SOLID', 'LIQUID', 'GAS', 
            'CEMENT', 'FLARING', 'PER_CAP'
        ).diff()
    )
    .collect()   # dataframe
)

df_decade = (
    df_year
    .lazy()   # lazyframe
    .with_columns(DECADE = pl.col('YEAR').str.slice(0,3) +  pl.lit("0's"))
    .filter(pl.col('DECADE') != "2020's")  # only data for 2 years in the 2020s
    .group_by('DECADE').agg(
        pl.col('FOSSIL').sum(),
        pl.col('SOLID').sum(),
        pl.col('LIQUID').sum(),
        pl.col('GAS').sum(),
        pl.col('CEMENT').sum(),
        pl.col('FLARING').sum(),
        pl.col('PER_CAP').sum(),
    )
    .with_columns(YEAR = pl.lit('NA'))
    .sort('DECADE')
    .collect()    # dataframe
)

df_decade_diff = (
    df_decade
    .lazy()  # lazyframe
    .with_columns(
        pl.col(
            'FOSSIL', 'SOLID', 'LIQUID', 'GAS', 
            'CEMENT', 'FLARING', 'PER_CAP'
        ).diff()
    )
    .collect()   # dataframe
)

#----- CALLBACK FUNCTIONS ------------------------------------------------------
def get_data_plot(data_type, group_by, graph_type):
    if (data_type, group_by) == ('DATA', 'YEAR'):
        df_plot = df_year
        graph_title = f'<b>{graph_type} PLOT BY {group_by}</b>'
        y_axis_label = 'TOTAL BY YEAR'
    if (data_type, group_by) == ('DIFF', 'YEAR'):
        df_plot = df_year_diff
        graph_title = f'<b>{graph_type} PLOT INCREMENTAL, {group_by} / {group_by}</b>'
        y_axis_label = 'YEAR/YEAR'
    if (data_type, group_by) == ('DATA', 'DECADE'):
        df_plot = df_decade
        graph_title = f'<b>{graph_type} PLOT BY {group_by}</b>'
        y_axis_label = 'TOTAL BY DECADE'
    if (data_type, group_by) == ('DIFF', 'DECADE'):
        df_plot = df_decade_diff
        graph_title = f'<b>{graph_type} PLOT INCREMENTAL, {group_by} / {group_by}</b>'
        y_axis_label = 'DECACE/DECADE'
    if graph_type == 'LINE':
        fig=px.line(
            df_plot,
            group_by,
            short_col_names[1:-1],
        )
    if graph_type == 'AREA':
        fig=px.area(
            df_plot,
            group_by,
            short_col_names[1:-1],
        )
    fig.update_layout(
        template='simple_white',
        title_text=graph_title,
        yaxis=dict(title=dict(text=y_axis_label))
    )
    if group_by == 'YEAR':
        int_years = sorted(
            [int(x) for x in df_plot['YEAR'].unique() if int(x)%10 ==0]
        )
        fig.update_xaxes(
            tickangle=90,
            tickmode = 'array',
            tickvals = [x for x in int_years if x%10 ==0],
            ticktext= [str(x) for x in int_years if x%10 ==0]
        )
    return fig

def get_corr_plot(corr_x, corr_y, group_by):
    if group_by == 'YEAR':
        df_plot = df_year.drop_nulls([corr_x, corr_y])
    if group_by == 'DECADE':
        df_plot = df_decade.drop_nulls([corr_x, corr_y])
    regression_params = stats.linregress(df_plot[corr_x], df_plot[corr_y])
    x_min = df_plot[corr_x].min()
    x_max = df_plot[corr_x].max()
    # find values for y at x_min and x_max using y = mx + b
    y_at_x_min = (regression_params.slope * x_min) + regression_params.intercept
    y_at_x_max = (regression_params.slope * x_max) + regression_params.intercept
    fig=px.scatter(
        df_plot,
        x=corr_x,
        y=corr_y,
        color='DECADE',   
    )
    fig.add_trace(
        go.Scatter(
            x=[x_min, x_max], 
            y=[y_at_x_min, y_at_x_max], 
            mode='lines', 
            line=dict(dash='longdashdot',width=3, color='lightgray'),
        )
    )

    fig.update_layout(
        template='simple_white',
        title_text=f'<b>CORRELATION PLOT BY {group_by}</b>',
        xaxis=dict(title=dict(text=f'{corr_x}')),
        yaxis=dict(title=dict(text=f'{corr_y}')),
        showlegend=False,
    )
    return fig

def get_table(group_by):
    if group_by == 'YEAR':
        df = df_year.select(dag_columns)
    if group_by == 'DECADE':
        df = (
            df_decade
            .select(dag_columns)
        )
    return df.to_dicts()

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horiz_line),
    dmc.Text('CO2 EMISSIONS', ta='center', style=style_h2),
    dmc.Text(data_src, ta='center', style=style_h3),
    html.Hr(style=style_horiz_line),
    html.Div(),
    dmc.Grid(
        children = [
            dmc.GridCol(radio_group_by, span=3, offset = 1),
            dmc.GridCol(radio_graph_type, span=3, offset = 1),
        ]
    ),
    dmc.Grid(
        children = [
            dmc.GridCol(dcc.Graph(id='graph_plot'), span=5, offset = 1),
            dmc.GridCol(dcc.Graph(id='graph_diff'), span=5, offset = 1),
        ]
    ),
    html.Div(),
    html.Hr(style=style_horiz_line),
    dmc.Text('Correlations', ta='center', style=style_h2),
    dmc.Text('Pearson values from scipy.stats', ta='center', style=style_h3),
    html.Hr(style=style_horiz_line),
    html.Div(),
    dmc.Grid(children = [
        dmc.GridCol(
            get_radio_corr_param('corr_x_radio', 'X Parameter:', 'FOSSIL'), 
            span=7, offset = 1
            )
        ]
    ),
    dmc.Grid(children = [
        dmc.GridCol(
            get_radio_corr_param('corr_y_radio', 'Y Parameter:', 'GAS'), 
            span=7, offset = 1
            ),
        ]
    ),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='graph_corr'), span=5, offset = 1),
        dmc.GridCol(stats_card, span=4, offset=1)
        ]
    ),
    html.Div(),
    html.Hr(style=style_horiz_line),
    dmc.Text('Data and Definitions', ta='center', style=style_h2, id='data_and_defs'),
    html.Hr(style=style_horiz_line),
    html.Div(),
    dmc.Grid(children = [
        dmc.GridCol(grid, span=6, offset = 0),
        dmc.GridCol(definition_card, span=4, offset = 1)
        ]
    ),
])

@app.callback(
    Output('graph_plot', 'figure'),
    Output('graph_diff', 'figure'),
    Output('graph_corr','figure'),
    Output('dash_ag_table', 'rowData'),
    Output('slope','children'),
    Output('intercept','children'),
    Output('correlation','children'),
    Output('stderr','children'),
    Output('i_stderr','children'),
    Output('data_and_defs','children'),

    Input('group_by_radio', 'value'),
    Input('graph_type_radio', 'value'),
    Input('corr_x_radio', 'value'),
    Input('corr_y_radio', 'value'),
)
def update_dashboard(group_by, graph_type, corr_x, corr_y):
    data_defs_title = 'Data and Definitions'
    if group_by == 'DECADE':
        data_defs_title = 'Data (by decade) and Definitions'
    return (
        get_data_plot('DATA', group_by, graph_type),
        get_data_plot('DIFF', group_by, graph_type),
        get_corr_plot(corr_x, corr_y, group_by),
        get_table(group_by),
        f'SLOPE: {get_corr_stat('SLOPE', corr_x, corr_y, group_by):.3f}',
        f'INTERCEPT: {get_corr_stat('INTERCEPT', corr_x, corr_y, group_by):.3f}',
        f'CORR: {get_corr_stat('CORR', corr_x, corr_y, group_by):.3f}',
        f'STDERR: {get_corr_stat('STDERR', corr_x, corr_y, group_by):.3f}',
        f'I_STDERR: {get_corr_stat('I_STDERR', corr_x, corr_y, group_by):.3f}',
        data_defs_title
    )
if __name__ == '__main__':
    app.run_server(debug=True)