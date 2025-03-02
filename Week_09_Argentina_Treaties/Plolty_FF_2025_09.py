import polars as pl
pl.Config().set_tbl_rows(20)
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, jupyter_dash
import dash_bootstrap_components as dbc
import dash_ag_grid as dag

#-------------------------------------------------------------------------------
#    This dashboard shows international agreements between Argentina and other
#    countries, grouped by decade. Selecting 1 or more decades will invoke a bar
#    chart with number of agreements by country, a map showing countries from
#    selected decade, and a table of the agreements
#-------------------------------------------------------------------------------
'''
    This dashboard shows international agreements between Argentina and other
    countries, grouped by decade. Selecting 1 or more decades will invoke a bar
    chart with number of agreements by country, a map showing countries from
    selected decade, and a table of the agreements
'''

#-------------------------------------------------------------------------------
#    constants
#-------------------------------------------------------------------------------
citation = (
    'Santander Javier I., La distribución de tratados como reflejo de la<' +
    'política exterior , Revista de Investigación en Política Exterior<' +
    'Argentina, Volumen 3, Número 6, Septiembre - Diciembre 2023.'
)
style_space = {'border': 'none', 'height': '5px', 'background': 'linear-gradient(to right, #007bff, #ff7b00)', 'margin': '10px 0'}



#-------------------------------------------------------------------------------
#    functions
#-------------------------------------------------------------------------------
def create_bar_plot(df_decade):
    print(df_decade)
    decade = df_decade['DECADE'].to_list()[0]
    fig = px.bar(
        df_decade
            .unique('PARTNER')
            .sort('PARTNER_COUNT')
            .tail(10)
            , 
        x='PARTNER_COUNT', 
        y='PARTNER', 
        # orientation='h',
        #markers=True,
        title=(
            f'TRADE AGREEMENTS {decade}<br>' +
            f'<sup>Top 10 or Fewer</sup>'
        ),
        template='simple_white',
        # height=400, width=600,
        # line_shape='spline'
    )
    fig.update_layout(
        xaxis_title=f'AGREEMENT COUNT',
        yaxis_title='',
    )
    return fig


def get_table_data(df_decade):
    df_table = (
        df_decade
        .select(pl.col('YEAR', 'Title', 'PARTNER','PARTNER_COUNT', 'REGION'))
        .sort(
            ['PARTNER_COUNT', 'YEAR', 'PARTNER', 'Title'],
            descending=[True, False, False, False]
        )
    )
    print(f'{df_table = }')
    return df_table


#-------------------------------------------------------------------------------
#    load and clean data
#-------------------------------------------------------------------------------
df = (
    pl.scan_csv('Argentina-bilateral-instruments-1810-2023.csv')
    .select(
        'Title', 'Counterpart ENG', 'Region ENG', 'Sign year',
        DECADE = pl.col('Sign date').str.slice(-4,3) + '0s'
    )
    .rename(
        {'Counterpart ENG':'PARTNER',
        'Region ENG': 'REGION',
        'Sign year' : 'YEAR'
        }
    )
    .drop_nulls('PARTNER')
    .with_columns(
        PARTNER_COUNT = pl.col('PARTNER').count().over('DECADE', 'PARTNER')
    )
    .sort('DECADE', 'PARTNER')
    .collect()
)
print(df)


drop_down_list = sorted(list(set(df['DECADE'])))

table_cols = ['YEAR', 'Title', 'PARTNER', 'PARTNER_COUNT', 'REGION']
grid = dag.AgGrid(
    rowData=[],
    columnDefs=[{"field": i, 'filter': True, 'sortable': True} for i in table_cols],
    dashGridOptions={"pagination": True},
    id='data_table'
)

#-------------------------------------------------------------------------------
#    Dash App
#-------------------------------------------------------------------------------
app = Dash(external_stylesheets=[dbc.themes.SANDSTONE])

app.layout = dbc.Container([
    html.Hr(style=style_space),
    html.H2(
        'Argentinian Trade Agreements by Decade', 
        style={
            'text-align': 'left'
        }
    ),
    html.Hr(style=style_space),
    html.Div([
        html.P(
            f'Citation: {citation}',
            style={
                'text-align': 'left',
                'margin-top': '20px',
                'font-style':
                'italic','font-size':
                '16px',
                'color': 'gray'
                }
            ),
        html.Hr(style=style_space)
    ]),

    dbc.Row([dcc.Dropdown(drop_down_list, drop_down_list[0], id='my_dropdown'),]),
 
    html.Div(id='dd-output-container'),

    dbc.Row([
        #dbc.Col(dcc.Graph(id='world_map'), width=4),
        dbc.Col(dcc.Graph(id='bar_plot'), width=4),
    ]),

    dbc.Row([
        dbc.Col([grid]), # , width=4),
    ])

])

@app.callback(
        Output('bar_plot', 'figure'),
        # Output('world_map', 'figure'),
        Output('data_table', 'rowData'),
        Input('my_dropdown', 'value')
)
def update_dashboard(selected_decade):
    df_selected = df.filter(pl.col('DECADE') == selected_decade)
    bar_plot = create_bar_plot(df_selected)
    df_table = get_table_data(df_selected)
    # world_map = get_world_map(df_selected)

    print(f'{df_selected = }')
    
    return bar_plot, df_table.to_dicts()


if __name__ == "__main__":
    app.run(debug=True)