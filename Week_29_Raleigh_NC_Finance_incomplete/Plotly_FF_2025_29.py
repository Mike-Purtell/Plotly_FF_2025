import polars as pl
import plotly.express as px
import dash
import dash_ag_grid as dag
from dash import Dash, html, Input, Output
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')
print(f'{dmc.__version__ = }')

df = (
    pl.scan_csv(
        'Budget_FY2023.csv',
        schema_overrides={  # top of these cols are integer-like
            'Div_Code': pl.String,
            'Unit_Code': pl.String,
            },
        try_parse_dates=True
        )
    .select(  # dropped FY (all values are 2023)
        APD = pl.col('APD').cast(pl.String),
        FUND = pl.col('Fund_Name').cast(pl.String),
        BUD_AMT = pl.col('Budgeted_Amount').cast(pl.Int32),
        ACT_AMT = pl.col('Actual_Amount').cast(pl.Int32),
    )
    .collect()
)

df_budget = (
    df
    .with_columns(APD=(pl.col('APD')+pl.lit('_B')))
    .pivot(
        on='APD',
        index=['FUND'],
        values='BUD_AMT',
        aggregate_function='sum',
    )
)
df_actual = (
    df
    .with_columns(APD=(pl.col('APD')+pl.lit('_A')))
    .with_columns()
    .pivot(
        on='APD',
        index='FUND',
        values='ACT_AMT',
        aggregate_function='sum'
    )
)

df_all = (
    df_budget
    .join(
        df_actual, 
        on='FUND',
        how='inner',
    )
)

#----- GLOBALS -----------------------------------------------------------------
style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_plain_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #d3d3d3, #d3d3d3)', 
    'margin': '10px,', 'fontsize': 16}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}
bg_color = 'lightgray'
legend_font_size = 20
px.defaults.color_continuous_scale = px.colors.sequential.YlGnBu

fund_list = sorted(
    df_all
    .unique('FUND')
    .select(pl.col('FUND'))
    ['FUND']
    .to_list()
)

#----- FUNCTIONS ---------------------------------------------------------------
def to_list(my_list):
    if not isinstance(my_list, list):
        my_list = [my_list]
    return my_list

#----- DASH COMPONENTS ---------------------------------------------------------
grid = dag.AgGrid(
    rowData=[],
    columnDefs=[{
        "field": i,
        'filter': True,
        'sortable': True,
        'columnWidth': 'autoSize',
        'minWidth':100,
        } for i in df_all.columns
    ],
    dashGridOptions={"pagination": True},
    columnSize="sizeToFit",
    id='ag-grid'
)

dmc_select_fund = dmc.MultiSelect(
    label='Select a fund',
    data=fund_list,
    value=fund_list,
    checkIconPosition='left',
    size='sm',
    id='select-fund',
)

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text(
        'Raleigh North Carolina - Expense analysis'.upper(), 
        ta='center', 
        style=style_h2
    ),
    dmc.Space(h=20),
    html.Hr(style=style_horiz_line),
    dmc.Space(h=20),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(dmc_select_fund, span=2, offset=1),
        ]
    ),
    html.Hr(style=style_plain_line),
    dmc.Space(h=20),
    # dmc.Grid(
    #     children = [ 
    #         dmc.GridCol(
    #             dcc.Graph(figure=get_choropleth()),
    #             id='choropleth', 
    #             span=6, 
    #             offset=1
    #         ),
    #         dmc.GridCol(dcc.Graph(id='scatter'),span=5, offset=0),
    #     ]
    # ),
    dmc.Grid(
        children = [
            dmc.GridCol(grid, span=12, offset=0),
        ]
    ),
])

#----- CALLBACKS----------------------------------------------------------
@app.callback(
    Output('ag-grid', 'rowData'),   
    Input('select-fund', 'value'),
)
def update_dashboard(fund):
    df_grid = (
        df_all
        .filter(pl.col('FUND').is_in(to_list(fund)))
    )   
    return df_grid.to_dicts()

#----- RUN THE APP -------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)