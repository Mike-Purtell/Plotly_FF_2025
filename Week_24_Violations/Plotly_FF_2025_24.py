import polars as pl
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
from dash_ag_grid import AgGrid
dash._dash_renderer._set_react_version('18.2.0')

#----- DATA GATHER AND CLEAN ---------------------------------------------------
# provided csv file (49M) was cleaned, saved as a parquet file (87K). 
if False:
    df = (
        pl.read_csv(
            'Open_Parking_and_Camera_Violations.csv',
            try_parse_dates=True,
            ignore_errors=True
        )
        .select( 
            ISSUE_DATE = pl.col('Issue Date'),
            VIOLATION=pl.col('Violation'),
            JUDGE_DATE = pl.col('Judgment Entry Date'),
            FINE_AMT = pl.col('Fine Amount'),
            PAY_AMT = pl.col('Payment Amount'),
            STATUS = pl.col('Violation Status'),
        )
        .with_columns(
            JUDGE_DAYS = (
                pl.col('JUDGE_DATE') - pl.col('ISSUE_DATE'))
                .dt.total_days()
        )
        .filter(pl.col('JUDGE_DAYS') > 0)  
    )
    df.write_parquet('Open_Parking_and_Camera_Violations.parquet')
else:
    df = pl.read_parquet('Open_Parking_and_Camera_Violations.parquet')
#----- GLOBALS -----------------------------------------------------------------
style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal'}

violation_list = df.unique('VIOLATION').select(pl.col('VIOLATION')).to_series().to_list()
print(f'{len(violation_list) = }')
print(f'{violation_list = }')

#----- CALLBACK FUNCTIONS-------------------------------------------------------
def get_px_hist(df, violation, data_col):
    if data_col == 'FINE_AMT':
        graph_title = 'DISTRIBUTION OF FINES ASSESSED'
    if data_col == 'PAY_AMT':
        graph_title = 'DISTRIBUTION OF AMOUNTS PAID'
    if data_col == 'JUDGE_DAYS':
        graph_title = 'DAYS BETWEEN VIOLATION AND JUDGEMENT'
    fig = px.histogram(
        df.filter(pl.col('VIOLATION') == violation),
        x=data_col,
        template='simple_white',
        title=graph_title
    )
    return fig
 
def make_violation_table():
    df_dag = (
        df
        .select('VIOLATION')
        .unique('VIOLATION')
        .sort('VIOLATION')
        .rename({'VIOLATION': 'SELECT A VIOLATION:'})
    )
    row_data = df_dag.to_dicts()
    column_defs = [{"headerName": col, "field": col} for col in df_dag.columns]
    return (
        AgGrid(
            id='violation_list',
            rowData=row_data,
            columnDefs=column_defs,
            defaultColDef={"filter": True},
            columnSize="sizeToFit",
            getRowId='params.data.State',
            dashGridOptions={
                'rowSelection': 'single',
                'animateRows': True
            },
        )
    )

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('New York City Traffic Violation Data', ta='center', style=style_h2),
    dmc.Text('', ta='center', style=style_h3, id='violation_text'),
    html.Hr(style=style_horiz_line),
    dmc.Space(h=30),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(dcc.Graph(id='px_hist_fine'), span=4, offset=1),
            dmc.GridCol(make_violation_table(), span=4, offset=2)
        ]
    ),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(dcc.Graph(id='px_hist_paid'), span=4, offset=1),
            dmc.GridCol(dcc.Graph(id='px_hist_period'), span=4, offset=1),
        ]
    ),
])

@app.callback(
    Output('px_hist_fine', 'figure'),
    Output('px_hist_paid', 'figure'),
    Output('px_hist_period', 'figure'),
    Output('violation_text', 'children'),
    Input('violation_list', 'cellClicked'),
)
def update_dashboard(violation):  # line_shape, scale, test, dag_test):
    if violation is None:
        violation_name=violation_list[0]
    else:
        violation_name = violation["value"]
    print(f'{violation_name = }')
    px_hist_fine = get_px_hist(df, violation_name, 'FINE_AMT')
    px_hist_paid = get_px_hist(df, violation_name, 'PAY_AMT')
    px_hist_period = get_px_hist(df, violation_name, 'JUDGE_DAYS')
    return px_hist_fine, px_hist_paid, px_hist_period, violation_name

if __name__ == '__main__':
    app.run(debug=True)
