import polars as pl
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
'''
    This dashboard shows international treaties with Argentina. Data grouped by 
    decade are visualized with a bar chart, a world map and a dash AG table. 
'''

#----- DEFINE CONSTANTS---------------------------------------------------------
citation = (
    'Santander Javier I., La distribución de tratados como reflejo de la ' +
    'política exterior , Revista de Investigación en Política Exterior ' +
    'Argentina, Volumen 3, Número 6, Septiembre - Diciembre 2023.'
)
style_space = {
    'border': 'none', 
    'height': '5px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px 0'
    }

# these are the columns to show in Dash AG table, in listed order
table_cols = ['YEAR', 'Title', 'PARTNER', 'TREATIES', 'REGION']

#----- DEFINE FUNCTIONS---------------------------------------------------------
def create_bar_plot(df_decade, decade):
    fig = px.bar(
        df_decade
            .unique('PARTNER')
            .sort(['TREATIES', 'PARTNER'], descending =[False, True])
            .tail(10)
            , 
        x='TREATIES', 
        y='PARTNER', 
        title=(
            f"Argentina's Treaty Partners in the {decade}".upper() +
            f'<br><sup>Top 10 or Fewer</sup>'
        ),
        template='simple_white',
        text_auto=True,   # superimpose data on each bar
    )
    fig.update_layout(yaxis_title='')  # Y-axis is obvious, no label needed
    fig.update_traces(  # disable hover traces on the bar chart
        hovertemplate = None,
        hoverinfo='skip'
    )
    return fig

def get_world_map(df_decade, decade):
    fig = px.choropleth(
        data_frame=df_decade,
        locations='PARTNER',
        locationmode='country names',
        color='TREATIES',
        color_continuous_scale='viridis',
        custom_data=['PARTNER', 'DECADE', 'TREATIES'],
        title=f"Argentina's Treaty Partners in the {decade}".upper(),
        projection='equirectangular',
    )
    fig.update_traces(
        hovertemplate="<br>".join([
            "%{customdata[0]} %{customdata[1]}",
            "%{customdata[2]} Treaty(s)",
        ])
    )
    fig.update_layout(
        boxgap=0.25,
        height=500,
        margin=dict(l=10, r=0, b=10, t=100), # , pad=50),
        coloraxis_showscale=False # turn off the legend/color_scale 
    )
    return fig

def get_table_data(df_decade, decade):
    df_table = (
        df_decade
        .select(pl.col('YEAR', 'Title', 'PARTNER','TREATIES', 'REGION'))
        .sort(
            ['TREATIES', 'YEAR', 'PARTNER', 'Title'],
            descending=[True, False, False, False]
        )
    )
    return df_table

#----- LOAD AND CLEAN DATA -----------------------------------------------------
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
        TREATIES = pl.col('PARTNER').count().over('DECADE', 'PARTNER'),
    )
    .sort('DECADE', 'PARTNER')
    .collect()
)

drop_down_list = sorted(list(set(df['DECADE'])))
grid = dag.AgGrid(
    rowData=[],
    columnDefs=[{"field": i, 'filter': True, 'sortable': True} for i in table_cols],
    dashGridOptions={"pagination": True},
    id='data_table'
)

#----- DASH APP ----------------------------------------------------------------
app = Dash(external_stylesheets=[dbc.themes.SANDSTONE])

app.layout = dbc.Container([
    html.Hr(style=style_space),
    html.H2(
        'International Treaties with Argentina by Decade', 
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

    dbc.Row(
        [dcc.Dropdown(
            drop_down_list,       # list of choices
            drop_down_list[0],    # use first item on list as default
            id='my_dropdown', 
            style={'width': '50%'}),
        ],
    ),
    
    html.Div(id='dd-output-container'),

    dbc.Row([
        dbc.Col(dcc.Graph(id='bar_plot')),    
        dbc.Col([grid]),

    ]),

    dbc.Row([
        dbc.Col(dcc.Graph(id='world_map'),),  # width=4),
    ])
])

@app.callback(
        Output('bar_plot', 'figure'),
        Output('world_map', 'figure'),
        Output('data_table', 'rowData'),
        Input('my_dropdown', 'value')
)

def update_dashboard(decade):
    df_selected = df.filter(pl.col('DECADE') == decade)
    bar_plot = create_bar_plot(df_selected, decade)
    df_table = get_table_data(df_selected, decade)
    world_map = get_world_map(df_selected, decade)
    return bar_plot, world_map, df_table.to_dicts()

#----- RUN THE APP -------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)