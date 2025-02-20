from dash import Dash, dcc, html, Input, Output, jupyter_dash
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import plotly.express as px
import polars as pl
from datetime import datetime
#-------------------------------------------------------------------------------
#  This week 7 submission is an enhancement to the code offered for this exercise.
#  My dashboard skill are barely past the hello world level, hope to get better.
#-------------------------------------------------------------------------------

# Read the given csv data to polars dataframe df
csv_data_source = 'all_outlays_2025-02-14.csv'

df = (
    pl.scan_csv(
        csv_data_source, 
        infer_schema_length=0  # this reads all columns as string
    )
    .rename({'transaction_catg_renamed': 'TRANS_CAT'})
    .with_columns(
        pl.col('Date').str.strptime(pl.Date, '%Y-%m-%d'),
        pl.col('Daily').cast(pl.Float32),
    )
    .with_columns(
        DAILY_MIL_USD = (pl.col('Daily')*1000),
    )
    .filter(pl.col('Date') > pl.lit(datetime(2025, 1, 2)))
    .select('TRANS_CAT', 'Date', 'DAILY_MIL_USD')
    .collect()
)
# make df_usaid using filtered, sorted version of big df.
default_cat = 'IAP - USAID'

grid = dag.AgGrid(
    rowData=[],
    columnDefs=[{"field": i, 'filter': True, 'sortable': True} for i in df.columns],
    dashGridOptions={"pagination": True},
    id='my_grid'
)

drop_down_list = sorted(list(set(df['TRANS_CAT'])))
print(f'{len(drop_down_list) = }')
print(f'{drop_down_list = }')

print(df.filter(pl.col('TRANS_CAT')== default_cat))

app = Dash(external_stylesheets=[dbc.themes.SANDSTONE])
app.layout = dbc.Container([
    dbc.Row([dcc.Dropdown(drop_down_list, default_cat, id='my_dropdown'),]),
    html.Div(id='dd-output-container'),
    dbc.Row([
        dbc.Col(dcc.Graph(id='line_plot'), width=4),
        dbc.Col(dcc.Graph(id='hist_plot'), width=4),
    ]),
    dbc.Row([
        dbc.Col([grid], width=4),
    ])
])

df_selected = df
@app.callback(
        Output('line_plot', 'figure'),
        Output('hist_plot', 'figure'),
        Output('my_grid', 'rowData'),
        Input('my_dropdown', 'value')
)

def update_dashboard(selected_group):
    df_selected = df.filter(pl.col('TRANS_CAT') == selected_group)
    line_plot = px.line(
        df_selected, 
        x='Date', 
        y='DAILY_MIL_USD', 
        markers=True,
        title=f'Expenditures of {selected_group}'.upper(),
        template='simple_white',
        height=400, width=600,
        line_shape='spline'
    )
    line_plot.update_layout(
        xaxis_title='',
        yaxis_title='Daily Expenditures (MILLION $US)'.upper(),
    )
    hist_plot = px.histogram(
        df_selected, 
        x='DAILY_MIL_USD', 
        title=f'Expenditures of {selected_group}'.upper(),
        template='simple_white',
        height=400, width=600,
    )
    hist_plot.update_layout(
        xaxis_title='Daily Expenditures (MILLION $US)'.upper(),
        yaxis_title='count'.upper(),
    )
    print(df_selected)
    return line_plot, hist_plot, df_selected.to_pandas().to_dict('records')

if __name__ == "__main__":
    app.run(jupyter_height=500, jupyter_width='70%')
