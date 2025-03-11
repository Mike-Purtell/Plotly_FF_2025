import polars as pl
import polars.selectors as cs
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
'''
    This dashboard shows popularity of programming languages from 2004 to 2024.
    All data are percentages, where the totals across each row should add up to 
    100%. This will be checked, exceptions will be noted. 
'''

#----- DEFINE CONSTANTS---------------------------------------------------------
citation = (
    'Thank you to Muhammad Khalid and Kaggle for the data, ' +
     'https://www.kaggle.com/datasets/muhammadkhalid/'   
)

style_space = {
    'border': 'none', 
    'height': '5px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px 0'
    }

#----- DEFINE FUNCTIONS---------------------------------------------------------
def create_line_plot(lang_1, lang_2):
    fig = px.line(
        df,
        'DATE',
        [lang_1, lang_2], 
        title=(f'{lang_1} vs. {lang_2}<br><sup>Popularity by Year</sup>'),
        template='simple_white',
    )
    fig.update_layout(
        yaxis_title='POPULARITY',
        xaxis_title='',   # that X is year is obvious, no label
        hovermode='x unified',
        legend_title_text='Prog. Lang.'
    )  
    return fig

def create_scatter_plot(lang_1, lang_2):
    fig = px.scatter(
        df,
        lang_1,
        lang_2,
        template='simple_white',
        title=(f'{lang_1} vs. {lang_2}<br><sup>Scatter Plot/Correlation</sup>'),
    )
    return fig

def get_table_data(lang_1, lang_2):
    df_table = (
        df
        .select(pl.col('DATE', lang_1, lang_2))
        .sort('DATE', descending=False)
    )
    return df_table

#----- LOAD AND CLEAN DATA -----------------------------------------------------
df = (
    pl.read_csv(
        'Popularity of Programming Languages from 2004 to 2024.csv',
        ignore_errors=True
        )
    .with_columns(DATE = pl.col('Date').str.to_date(format='%b-%y'))
    .select('DATE', pl.all().exclude('Date', 'DATE')) 
    
    # move DATE to 1st col, get rid of Date col (string)
    .with_columns(pl.all().fill_null(strategy="zero"))
    
    # use sum_horizontal to see how close row totals are to 100%
    .with_columns(
        PCT_TOT = (pl.sum_horizontal(cs.numeric()))
    )
)
print(f'{df['PCT_TOT'].min() = }')
print(f'{df['PCT_TOT'].max() = }')
print('hi')
print(df)

drop_down_list = sorted(list(df.columns)[1:-1])
print(f'{drop_down_list = }')

grid = dag.AgGrid(
    rowData=[],
    columnDefs=[{"field": i, 'filter': True, 'sortable': True} for i in df.columns],
    dashGridOptions={"pagination": True},
    id='data_table'
)

#----- DASH APP ----------------------------------------------------------------
app = Dash(external_stylesheets=[dbc.themes.SANDSTONE])

app.layout = dbc.Container([
    html.Hr(style=style_space),
    html.H2(
        'Programming Language Popularity, 2004 to 2024', 
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
            ['Python', 'Perl'],   # defaults
            id='lang_dropdown', 
            style={'width': '50%'},
            multi=True,  # first 2 are used, all other ignored
            ),
        ],
    ),

    html.Div(id='dd-output-container'),

    dbc.Row([
        dbc.Col([grid]),
    ]),

    dbc.Row([
        dbc.Col(dcc.Graph(id='scatter_plot')),    
        dbc.Col(dcc.Graph(id='line_plot'),),
    ])
])

@app.callback(
        Output('line_plot', 'figure'),
        Output('scatter_plot', 'figure'),
        Output('data_table', 'rowData'),
        Input('lang_dropdown', 'value'),
)

def update_dashboard(selected_values):
    df_selected = df.select(pl.col('DATE', selected_values[0], selected_values[1]))
    line_plot = create_line_plot(selected_values[0], selected_values[1])
    scatter_plot  = create_scatter_plot(selected_values[0], selected_values[1])
    return line_plot, scatter_plot, df_selected.to_dicts()

#----- RUN THE APP -------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)