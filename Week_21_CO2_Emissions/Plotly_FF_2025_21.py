import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import dash_mantine_components as dmc
import dash
from scipy import stats
dash._dash_renderer._set_react_version('18.2.0')

#----- GLOBAL DATA STRUCTURES --------------------------------------------------
df_col_defs  = [c for c in pl.scan_csv('global.1751_2021.csv').collect_schema()]
# print(f'{df_col_defs = }')

short_col_names = [
    'YEAR', 'FOSSIL', 'SOLID', 'LIQUID', 'GAS', 'CEMENT', 'FLARING', 'PER_CAP','DECADE']
# print(f'{short_col_names = }')

grid = dag.AgGrid(
    rowData=[],
    columnDefs=[
        {"field": i, 'filter': True, 'sortable': True} for i in short_col_names
    ],
    dashGridOptions={"pagination": True},
    id='dag_table'
)

style_horiz_line = {
    'border': 'none',
    'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,',
    'fontsize': 32
}

style_h3 = {
    'text-align': 'center', 'font-size': '16px', 'font-weight': 'normal'}

#----- READ & CLEAN DATASET ----------------------------------------------------
dict_cols = dict(zip( df_col_defs, short_col_names))

# print(f'{dict_cols = }') 
 
df_year = (
    pl.scan_csv('global.1751_2021.csv')
    .rename(dict_cols)
    .with_columns(
        pl.col('PER_CAP').cast(pl.Float32),
        pl.col('YEAR').cast(pl.String)
    )
    .with_columns(
        DECADE = pl.col('YEAR').str.slice(0,3) +  pl.lit("0's")
    )
    .collect()
)

# print('df_year', df_year)
df_year_diff = (
    df_year
        .with_columns(
            pl.col(
                'FOSSIL', 'SOLID', 'LIQUID', 'GAS', 
                'CEMENT', 'FLARING', 'PER_CAP'
            ).diff()
        )
)
# print('df_year_diff', df_year_diff)

df_decade = (
    df_year
    .with_columns(
         DECADE = pl.col('YEAR').str.slice(0,3) +  pl.lit("0's")
    )
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
    .sort('DECADE')
)
# print('df_decade', df_decade)

df_decade_diff = (
    df_decade
        .with_columns(
            pl.col(
                'FOSSIL', 'SOLID', 'LIQUID', 'GAS', 
                'CEMENT', 'FLARING', 'PER_CAP'
            ).diff()
        )
)

# print('df_decade_diff', df_decade_diff)

#----- CALLBACK FUNCTIONS ------------------------------------------------------

def get_data_delta_plot(year_or_decade, line_or_area):
    pass

def get_correlation_plot(year_or_decade):
    pass

def get_state_card_text(selected_state):
    pass

def get_dam_table(selected_state):
    pass


def get_data_plot(data_type, group_by, graph_type):
    if (data_type, group_by) == ('DATA', 'YEAR'):
        df_plot = df_year
    if (data_type, group_by) == ('DIFF', 'YEAR'):
        df_plot = df_year_diff
    if (data_type, group_by) == ('DATA', 'DECADE'):
        df_plot = df_decade
    if (data_type, group_by) == ('DIFF', 'DECADE'):
        df_plot = df_decade_diff
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
        title_text=f'<b>{graph_type} PLOT BY {group_by} : {data_type}</b>',
        yaxis=dict(title=dict(text=f'TOTAL BY {group_by}'))
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
    # these line find values for y at x_min and x_max using y = mx + b
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
    )

    fig.add_annotation(
        x = 0.05, xref ='paper',
        y = 0.95, yref ='paper',
        text=(
            '<b>Regression Stats:</b><br>' +
            f'   Slope = {regression_params.slope:.3f}<br>' +
            f'   Y-Intercept = {regression_params.intercept:.3f}<br>' +
            f'   Correlation = {regression_params.rvalue**2:.3f} (Pearson r<sup>2</sup>)<br>' +
            f'   Slope Std Error = {regression_params.stderr:.3f}<br>' +
            f'   Intercept Std Error = {regression_params.intercept_stderr:.3f}<br>'
        ),
        align='left',
        showarrow=False
    )
    return fig

# def get_state_stat(df, param, col):
#     return(
#         df
#         .filter(pl.col('STATISTIC') == param)
#         [col]
#         [0]
#     )

# def get_scatter_map(state):
#     df_state = (
#         df
#         .filter(pl.col('STATE') == state)
#         .select('STATE', 'LONG', 'LAT', 'DECADE_COMP','MAX_STG_ACR_FT')
#         .sort(['DECADE_COMP', 'MAX_STG_ACR_FT'])
#     )
#     state_zoom = 4  # default. following code changes zoom for listed states
#     if state in ['Alaska']:
#         state_zoom = 2
#     elif state in [ 'Texas']:
#          state_zoom = 3
#     elif state in ['Connecticut', 'Louisiana', 'Massachusetts', 'New Jersey', ]:
#         state_zoom = 5
#     elif state in ['Delaware', 'Rhode Island','Puerto Rico']:
#         state_zoom = 7
#     elif state in ['Guam']:
#         state_zoom = 8
#     scatter_map = px.scatter_map(  # map libre
#         df_state,
#         lat='LAT',
#         lon='LONG',
#         zoom=state_zoom,
#         color='DECADE_COMP',
#     )
#     scatter_map.update_layout(legend_title = '<b>Completed</b>')
#     return(scatter_map)

# def get_top_10_bar(state):
#     df_state = (
#         df
#         .filter(pl.col('STATE') == state)
#         .select(['DAM', 'MAX_STG_ACR_FT', 'MAX_DISCHRG_CUB_FT_SEC'])
#         .sort('MAX_STG_ACR_FT', descending=True)
#         .with_columns(  # split the dam name inot words, only keep 1st 5
#             DAM_SHORT = pl.col('DAM').str.split(' ').list.slice(0, 7).list.join(' ')
#         )
#     )
#     state_dam_count = df_state.height
#     df_state = df_state.head(min(state_dam_count,15))
#     fig = px.bar(
#         df_state, 
#         y='DAM_SHORT', 
#         x='MAX_STG_ACR_FT',
#         template='simple_white',
#         title = f"DAMS ON {state.upper()}'S LARGEST WATERWAYS"
#     )
#     fig.update_layout(
#         yaxis=dict(
#             autorange='reversed',
#             title='',
#         ),
#         xaxis=dict(
#             title='MAXIMUM STORAGE [ACRE-FEET]'
#         ),
#     )
#     return(fig)

# def get_state_card_text(state):
#     df_state = (
#         df
#         .filter(pl.col('STATE') == state)
#         .select(['STATE'] + dam_stats)
#     )
#     dam_count=df_state.shape[0]
#     df_state_stats = (
#         df_state.group_by('STATE').agg(pl.col(dam_stats).sum())
#         .transpose(
#             include_header=True,
#             header_name='STATISTIC',
#         )
#         .rename({'column_0': 'TOTAL'})
#         .filter(pl.col('STATISTIC') != 'STATE')
#         # cannot directly cast Int as to String. Cast to Float, then cast to Int
#         .with_columns(pl.col('TOTAL').cast(pl.Float64).cast(pl.Int64))
#         .with_columns(
#             AVERAGE = (
#                 pl.col('TOTAL')/dam_count)
#                 .cast(pl.Int64)
#             )
#     )
     
#     tot_max_acre_feet = get_state_stat(df_state_stats, 'MAX_STG_ACR_FT', 'TOTAL')
#     avg_max_acre_feet = get_state_stat(df_state_stats, 'MAX_STG_ACR_FT', 'AVERAGE')
#     title_text = (
#        f"{state}'s {dam_count:,} dams " + 
#        f'contain a total of {tot_max_acre_feet:,} acre-feet of water. ' + 
#        f'Average containment per dam is {avg_max_acre_feet:,} acre-feet'
#     ) 
#     return title_text

def get_table():
    df_table = (
        df_year
        # .filter(pl.col('STATE') == state)
        .select(short_col_names)
        # .sort('MAX_STG_ACR_FT', descending=True)
    )
    return df_table.to_dicts()

#----- DASH APPLICATION STRUCTURE-----------------------------------------------

data_source_link = (
    f'Data Source:  https://rieee.appstate.edu/projects-programs/cdiac/'
    )
print(f'{data_source_link = }')
app = Dash(external_stylesheets=[dbc.themes.LITERA])
app.layout =  dmc.MantineProvider(
    [
        html.Hr(style=style_horiz_line),
        html.H2(
            'CO2 EMISSIONS DATA', 
            style={'text-align': 'center', 'font-size': '32px'}
        ),
        html.H3(data_source_link, style=style_h3),
        html.Hr(style=style_horiz_line),
        dbc.Row([ # cols 2 & 3 for Group by, 5 & 6 for Plot type
            dbc.Col(html.Div('Group by:'), width={'size': 2, 'offset': 1}),
            dbc.Col(html.Div('Plot type:'), width={'size': 2, 'offset': 1}),
        ]),
        dbc.Row([       
            dbc.Col(
                [
                    dcc.RadioItems(
                        ['DECADE', 'YEAR'], 'YEAR', inline=False,
                        labelStyle={'margin-right': '30px',},
                        id='group_by_radio', 
                    ),
                ],
                width={'size': 2, 'offset': 1}), # cols 2 & 3
            dbc.Col(
                [
                    dcc.RadioItems(
                        ['AREA', 'LINE'], 'AREA',  inline=False,
                        labelStyle={'margin-right': '30px',},
                        id='graph_type_radio', 
                    ),
                ],
                width={'size': 2, 'offset': 1}), # column 5 to 6
        ]),
        html.Div(),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id='graph_plot'),
                    width=6
                ),
                dbc.Col(
                    dcc.Graph(id='graph_diff'),
                    width=6
                ),
            ]
        ),
        html.Div(),
                html.Hr(style=style_horiz_line),
        html.H2(
            'Correlations', 
            style={'text-align': 'center', 'font-size': '32px'}
        ),
        html.H3('Pearson values from scipy.stats', style=style_h3),
        html.Hr(style=style_horiz_line),
        dbc.Row([
            dbc.Col(
                html.Div('X-Axis Corr Parameter:'), 
                width={'size': 2, 'offset': 0}
            ),
            dbc.Col(
                [
                    dcc.RadioItems(
                        sorted(short_col_names[1:-1]), 
                        sorted(short_col_names[1:-1])[0], 
                        inline=True,
                        labelStyle={'margin-right': '30px',},
                        id='corr_x_radio', 
                    ),
                ],
                width={'size': 10, 'offset': 0}),

        ]),
        html.Div(),
        dbc.Row([       
            dbc.Col(
                    html.Div('Y-Axis Corr Parameter:'), 
                    width={'size': 2, 'offset': 0}
                ),
            dbc.Col(
                [
                    dcc.RadioItems(
                        sorted(short_col_names[1:-1]), 
                        sorted(short_col_names[1:-1])[1], 
                        inline=True,
                        labelStyle={'margin-right': '30px'},
                        id='corr_y_radio',  
                    ),
                ],
                width={'size': 8, 'offset': 0}),
        ]),
        html.Div(),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='graph_corr'),
                width=6
            ),
            dbc.Col(
                dcc.Graph(id='graph_bar'),
                width=6
            ),
        ]),
        dbc.Row([
            dbc.Col([grid],id='dash_ag_table', width=5),
            dbc.Col(dbc.Card([
                dbc.CardBody([
                        html.H4(
                            'YEAR STATISTICS', 
                            className='card-header',
                            id='card_title'
                        ),
                        html.P(
                            "Some quick example text to build on the card title and "
                            "make up the bulk of the card's content.",
                            className="card-text",
                            id='card_text'
                        ),
                    ]),
            ])
            ),
        ])
    ]
)

@app.callback(
    Output('graph_plot', 'figure'),
    Output('graph_diff', 'figure'),
    Output('graph_corr','figure'),
    Output('graph_bar','figure'),
    Output('dash_ag_table', 'rowData'),
    Output('card_title','children'),
    Output('card_text','children'),

    Input('group_by_radio', 'value'),
    Input('graph_type_radio', 'value'),
    Input('corr_x_radio', 'value'),
    Input('corr_y_radio', 'value'),
    # Input('graph_hover', 'value'),
    # Input('diff_hover', 'value'),
    # Input('corr_hover', 'value'),
)
def update_dashboard(group_by, graph_type, corr_x, corr_y
    # graph_hover, diff_hover, corr_hover
    ):
    pass
    # print(f'{group_by = }')
    # print(f'{graph_type = }')
    # print(f'{theme_sel = }')
    # print(f'{corr_x = }')
    # print(f'{corr_y = }')
    # print(f'{graph_hover = }')
    # print(f'{diff_hover = }')
    # print(f'{corr_hover = }')

    return (
        get_data_plot('DATA', group_by, graph_type),
        get_data_plot('DIFF', group_by, graph_type),
        get_corr_plot(corr_x, corr_y, group_by),
        get_data_plot('DIFF', group_by, graph_type),  # replace with get bar
        get_table(),
        'CARD TITLE PLACEHOLDER', # get_card_title(),
        'CARD TEXT PLACEHOLDER'
    )
if __name__ == '__main__':
    app.run_server(debug=True)