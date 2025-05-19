import polars as pl
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import dash_ag_grid as dag

#----- GLOBAL DATA STRUCTURES --------------------------------------------------
dam_info = ['DAM','LAT','LONG','STATE','COUNTY','CITY','WATERWAY','YEAR_COMP',]

dam_stats = [
'NID_CAP_ACR_FT', 'MAX_STG_ACR_FT', 'NORM_STG_ACR_FT',
'DRAINAGE_SQ_MILES', 'SURF_AREA_SQM', 'MAX_DISCHRG_CUB_FT_SEC'
]
dam_table_cols = [
    'DAM','COUNTY','CITY','WATERWAY','YEAR_COMP',
    'NID_CAP_ACR_FT', 'MAX_STG_ACR_FT', 'NORM_STG_ACR_FT',
    'DRAINAGE_SQ_MILES', 'SURF_AREA_SQM', 'MAX_DISCHRG_CUB_FT_SEC',
    'LAT','LONG',
]

style_space = {
    'border': 'none',
    'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,',
    'fontsize': 32
}

grid = dag.AgGrid(
    rowData=[],
    columnDefs=[
        {"field": i, 'filter': True, 'sortable': True} for i in dam_table_cols
    ],
    dashGridOptions={"pagination": True},
    id='dam_table'
)

#----- READ & CLEAN DATASET ----------------------------------------------------
df = (
    pl.scan_csv(
        'nation.csv',
        ignore_errors=True, 
        skip_rows=1
    )
    .select(
        DAM = pl.col('Dam Name'),
        LAT = pl.col('Latitude'),
        LONG = pl.col('Longitude'),
        STATE = pl.col('State').str.to_titlecase(),
        COUNTY = pl.col('County').str.to_titlecase(),
        CITY = pl.col('City').str.to_titlecase(),
        WATERWAY = pl.col('River or Stream Name').str.to_titlecase(),
        YEAR_COMP = pl.col('Year Completed'),
        DECADE_COMP = pl.col('Year Completed Category'),

        # storage statisics for group_by aggregations
        NID_CAP_ACR_FT = pl.col('NID Storage (Acre-Ft)'),
        MAX_STG_ACR_FT = pl.col('Max Storage (Acre-Ft)'),
        NORM_STG_ACR_FT = pl.col('Normal Storage (Acre-Ft)'),
        DRAINAGE_SQ_MILES = pl.col('Drainage Area (Sq Miles)'),
        SURF_AREA_SQM = pl.col('Surface Area (Acres)'),
        MAX_DISCHRG_CUB_FT_SEC = pl.col('Max Discharge (Cubic Ft/Second)'),

    )
    .filter(pl.col('DAM').is_not_null())
    .filter(pl.col('MAX_STG_ACR_FT').is_not_null())
    .with_columns(DAM = pl.col('DAM'))
    .collect()
)
state_list = sorted(df['STATE'].unique().to_list())

#----- CALLBACK FUNCTIONS ------------------------------------------------------
def get_state_stat(df, param, col):
    return(
        df
        .filter(pl.col('STATISTIC') == param)
        [col]
        [0]
    )

def get_scatter_map(state):
    df_state = (
        df
        .filter(pl.col('STATE') == state)
        .select('STATE', 'LONG', 'LAT', 'DECADE_COMP','MAX_STG_ACR_FT')
        .sort(['DECADE_COMP', 'MAX_STG_ACR_FT'])
    )
    state_zoom = 4  # default. following code changes zoom for listed states
    if state in ['Alaska']:
        state_zoom = 2
    elif state in [ 'Texas']:
         state_zoom = 3
    elif state in ['Connecticut', 'Louisiana', 'Massachusetts', 'New Jersey', ]:
        state_zoom = 5
    elif state in ['Delaware', 'Rhode Island','Puerto Rico']:
        state_zoom = 7
    elif state in ['Guam']:
        state_zoom = 8
    scatter_map = px.scatter_map(  # map libre
        df_state,
        lat='LAT',
        lon='LONG',
        zoom=state_zoom,
        color='DECADE_COMP',
    )
    scatter_map.update_layout(legend_title = '<b>Completed</b>')
    return(scatter_map)

def get_top_10_bar(state):
    df_state = (
        df
        .filter(pl.col('STATE') == state)
        .select(['DAM', 'MAX_STG_ACR_FT', 'MAX_DISCHRG_CUB_FT_SEC'])
        .sort('MAX_STG_ACR_FT', descending=True)
        .with_columns(  # split the dam name inot words, only keep 1st 5
            DAM_SHORT = pl.col('DAM').str.split(' ').list.slice(0, 7).list.join(' ')
        )
    )
    state_dam_count = df_state.height
    df_state = df_state.head(min(state_dam_count,15))
    fig = px.bar(
        df_state, 
        y='DAM_SHORT', 
        x='MAX_STG_ACR_FT',
        template='simple_white',
        title = f"DAMS ON {state.upper()}'S LARGEST WATERWAYS"
    )
    fig.update_layout(
        yaxis=dict(
            autorange='reversed',
            title='',
        ),
        xaxis=dict(
            title='MAXIMUM STORAGE [ACRE-FEET]'
        ),
    )
    return(fig)

def get_state_card_text(state):
    df_state = (
        df
        .filter(pl.col('STATE') == state)
        .select(['STATE'] + dam_stats)
    )
    dam_count=df_state.shape[0]
    df_state_stats = (
        df_state.group_by('STATE').agg(pl.col(dam_stats).sum())
        .transpose(
            include_header=True,
            header_name='STATISTIC',
        )
        .rename({'column_0': 'TOTAL'})
        .filter(pl.col('STATISTIC') != 'STATE')
        # cannot directly cast Int as to String. Cast to Float, then cast to Int
        .with_columns(pl.col('TOTAL').cast(pl.Float64).cast(pl.Int64))
        .with_columns(
            AVERAGE = (
                pl.col('TOTAL')/dam_count)
                .cast(pl.Int64)
            )
    )
     
    tot_max_acre_feet = get_state_stat(df_state_stats, 'MAX_STG_ACR_FT', 'TOTAL')
    avg_max_acre_feet = get_state_stat(df_state_stats, 'MAX_STG_ACR_FT', 'AVERAGE')
    title_text = (
       f"{state}'s {dam_count:,} dams " + 
       f'contain a total of {tot_max_acre_feet:,} acre-feet of water. ' + 
       f'Average containment per dam is {avg_max_acre_feet:,} acre-feet'
    ) 
    return title_text

def get_dam_table(state):
    df_state = (
        df
        .filter(pl.col('STATE') == state)
        .select(dam_table_cols)
        .sort('MAX_STG_ACR_FT', descending=True)
    )
    return df_state.to_dicts()

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash(external_stylesheets=[dbc.themes.LITERA])
app.layout =  dbc.Container(
    [
        html.Hr(style=style_space),
        html.H2(
            'USA DAM INFO', 
            style={'text-align': 'center', 'font-size': '32px'}
        ),
        html.H3('Mark Twain once said "There are lies, damned lies and statistics". These are dam statistics',
            style={'text-align': 'center', 'font-size': '16px', 'font-weight': 'normal'}
        ),
        html.H3('Data Source: National Inventory of Dams Website: https://nid.sec.usace.army.mil/#/ ', 
            style={'text-align': 'center', 'font-size': '16px', 'font-weight': 'normal'}
        ),
        html.Hr(style=style_space),

            dbc.Row(
                [
                    dbc.Col(html.Div('Select US State or Territory'), width=3),
                ]
            ),
        dbc.Row([       
            dbc.Col(
                [
                    dcc.Dropdown(
                        state_list,
                        state_list[0],
                        id='state_select', 
                        multi= False
                    ),
                ],
                width=2
            ),
        ]),

        html.Div(id='dd-output-container'),
        html.Div(id='dd-choropleth-container'),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id='scatter_map'),
                    width=7
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardBody(
                                [
                                    html.H4(
                                        'STATE STATS', 
                                        className='card-title',
                                        id='id-state-desc-title'
                                    ),
                                    html.P(
                                        "Some quick example text to build on the card title and "
                                        "make up the bulk of the card's content.",
                                        className="card-text",
                                        id='id-state-desc-text'
                                    ),
                                ],                   
                            ),
                        ]
                    )
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id='top_10_bar'),
                    width=7
                ),
                dbc.Col([grid],width=5),
            ]
        )
    ]
)

@app.callback(
    Output('scatter_map', 'figure'),
    Output('top_10_bar', 'figure'),
    Output('id-state-desc-title','children'),
    Output('id-state-desc-text','children'),
    Output('dam_table', 'rowData'),
    Input('state_select', 'value'),
)
def update_dashboard(selected_state):
    return (
        get_scatter_map(selected_state),
        get_top_10_bar(selected_state),
        selected_state.upper(),
        get_state_card_text(selected_state),
        get_dam_table(selected_state),
    )
if __name__ == '__main__':
    app.run_server(debug=True)