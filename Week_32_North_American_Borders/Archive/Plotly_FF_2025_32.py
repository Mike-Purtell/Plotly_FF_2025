import polars as pl
pl.Config().set_tbl_cols(10)
import polars.selectors as cs
import plotly.express as px
import dash_ag_grid as dag
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
import os
dash._dash_renderer._set_react_version('18.2.0')

#----- GLOBALS -----------------------------------------------------------------
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_horizontal_thin_line = {'border': 'none', 'height': '2px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 12}

style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold'}
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal'}

parquet_data_source = 'Border_Crossing_Entry_Data.parquet'
csv_data_source = 'Border_Crossing_Entry_Data.csv' 
date_fmt ='%b-%y'
#----- MAKE LISTS FOR ENUMS, AND DATAFRAMES FOR JOINS  -------------------------
state_list = [
    'Alaska',  'Arizona', 'California', 'Idaho', 'Maine', 'Michigan',
    'Minnesota', 'Montana', 'New Mexico', 'New York', 'North Dakota', 'Texas',
    'Vermont', 'Washington'
    ]
state_abbr_list = [
    'AK', 'AZ', 'CA', 'ID', 'ME', 'MI',
    'MN', 'MT', 'NM', 'NY', 'ND', 'TX', 
    'VT', 'WA'
]
df_states = pl.DataFrame({  # used in join to add state abbreviations
    'STATE' :state_list,
    'STATE_ABBR' : state_abbr_list
})

measure_list = [
    'Bus Passengers', 'Buses', 'Pedestrians', 'Personal Vehicle Passengers', 
    'Personal Vehicles', 'Rail Containers Empty', 'Rail Containers Loaded', 
    'Train Passengers', 'Trains', 'Truck Containers Empty', 
    'Truck Containers Loaded', 'Trucks'
    ]

transpo_list = [ 
    'Bus', 'Bus', 'Walk', 'Car', 
    'Car', 'Train', 'Train', 
    'Train', 'Train', 'Truck', 'Truck', 'Truck'
]

df_transpo = pl.DataFrame({  # used in join for grouping transporation options
    'MEASURE'    : measure_list,
    'TRANSPO'    : transpo_list
})

#----- GATHER AND CLEAN DATA ---------------------------------------------------
if os.path.exists(parquet_data_source): # use pre-cleaned parquet if exists
    print(f'Reading data from {parquet_data_source}')
    df = (
        pl.scan_parquet(parquet_data_source)
        .with_columns(pl.col('LAT', 'LONG').cast(pl.Float64))
        .collect()
    )

else:  # read data from csv and clean, and save to parquet
    print(f'Reading data from {csv_data_source}')
    df = (
        pl.scan_csv(csv_data_source,try_parse_dates=True)
        .rename(lambda c: c.upper()) # col names to upper case
        .select(
            PORT = pl.col('PORT NAME'),
            STATE = pl.col('STATE'),
            BORDER = pl.col('BORDER')
                .str.replace(' Border', ''),
            MEASURE = pl.col('MEASURE'),
            DATE = pl.col('DATE').str.to_date(format=date_fmt),
            VALUE = pl.col('VALUE').cast(pl.UInt32),
            POINT = pl.col('POINT')   # get rid or parens and word POINT
                .str.replace_all('POINT ', '')
                .str.strip_chars('(')
                .str.strip_chars(')')
        )
        .filter(pl.col('VALUE') > 0.0)
        .with_columns(  # LONG and LAT in point column have better resolution
            LAT = pl.col('POINT').str.split(' ').list.first().cast(pl.Float64),
            LONG = pl.col('POINT').str.split(' ').list.last().cast(pl.Float64),        
        )
        .drop('POINT')
        .drop_nulls(subset='STATE')
        .collect()
        .join(
            df_states,
            on='STATE',
            how='left'
        )
        .join(
            df_transpo,
            on='MEASURE',
            how='left'
        )
        .group_by(
            'BORDER', 'STATE', 'STATE_ABBR', 'PORT', 'TRANSPO', 
            'DATE', 'LAT', 'LONG', 
            )
        .agg(pl.col('VALUE').sum())
        .sort(['BORDER', 'STATE','PORT', 'TRANSPO', 'DATE'])  
    )
    # define enum types, and apply to dataframe columns
    port_list=sorted(df.unique('PORT').get_column('PORT').to_list())
    port_enum = pl.Enum(sorted(list(set(port_list))))
    state_enum = pl.Enum(state_list)
    state_abbr_enum = pl.Enum(state_abbr_list)
    transpo_enum = pl.Enum(sorted(list(set(transpo_list))))
    border_enum = pl.Enum(['US-Canada', 'US-Mexico'])
    df = (
        df
        .lazy()
        .with_columns(
            STATE=pl.col('STATE').cast(state_enum),
            STATE_ABBR=pl.col('STATE_ABBR').cast(state_abbr_enum),
            TRANSPO=pl.col('TRANSPO').cast(transpo_enum),
            PORT=pl.col('PORT').cast(port_enum),
            BORDER = pl.col('BORDER').cast(border_enum)
        )
        .collect()
    )
    df.write_parquet(parquet_data_source)
    df.write_excel('df.xlsx')
# print(df)
# #----- DASH COMPONENTS -------------------------------------------------------
dmc_select_group_by = (
    dmc.Select(
        label='Group By',
        placeholder="Select one",
        id='group-by',
        data=['BORDER', 'STATE', 'PORT'],
        value='BORDER',
        size='xl',
    ),
)
fig_choro = px.choropleth(
    df.group_by('STATE', 'STATE_ABBR').agg(pl.col('VALUE').sum()),
    locations='STATE_ABBR', 
    locationmode='USA-states', 
    scope='usa', # 'north america',
    color='VALUE',
    template='plotly_white',
    color_continuous_scale='Viridis_r', 
)
fig_choro.update_layout(coloraxis_colorbar_x=0,)
fig_choro.update_coloraxes(colorbar_ticklabelposition='outside left')

# dmc_select_outcome = (
#     dmc.Select(
#         label='Select OUTCOME',
#         placeholder="Select one",
#         id='outcome',
#         data=outcome_list,
#         value=outcome_list[0],
#         size='xl',
#     ),
# )
# attribute_card = dmc.Card(
#     children=[
#         dmc.Text('attr-title', fz=30, id='attr-title'),
#         dmc.Text('attr-text', fz=20, id='attr-text'),
#     ],
#     withBorder=True,
#     shadow='sm',
#     radius='md'
# )

# outcome_card = dmc.Card(
#     children=[
#         dmc.Text('outcome-title', fz=30, id='outcome-title'),
#         dmc.Text('outcome-text', fz=20, id='outcome-text'),
#     ],
#     withBorder=True,
#     shadow='sm',
#     radius='md'
# )

# #----- FUNCTIONS ---------------------------------------------------------------





# def get_ag_col_defs(columns):
#     ''' return setting for ag columns, with numeric formatting '''
#     ag_col_defs = [{   # make CANDY column wider, pinned with floating filter
#         'field':'CANDY', 
#         'pinned':'left',
#         'width': 150, 
#         'floatingFilter': True,
#         "filter": "agTextColumnFilter", 
#         "suppressHeaderMenuButton": True
#     }]
#     for col in columns[1:]:   # applies to data columns, floating point
#         ag_col_defs.append({
#             'headerName': col,
#             'field': col,
#             'type': "numericColumn",
#             'valueFormatter': {"function": "d3.format('.1f')(params.value)"},
#             'width' : 100,
#             'floatingFilter': False,
#             'suppressHeaderMenuButton' : True,
#         })
#     return ag_col_defs
 
# def get_median(df, attribute, filter, outcome):
#     ''' return median value for specific attribute, outcome '''
#     return df.filter(pl.col(attribute) == filter)[outcome].median()

# def get_subtitle(outcome, direction, pct_color, median_shift):
#     ''' return complex subtitle with f-strings and html color'''
#     return (
#         f'<sup>{outcome} median {direction} by ' +
#         f'<b><span style="color:{pct_color}">' +
#         f'{abs(median_shift):.1f}%</span></b>'
#     )

# def get_box_plot(attribute, outcome):
#     ''' returns plotly graph objects box_plot, created with px.box API '''
    
#     df_box = ( # make data frame for this attribute, outcome
#         df
#         .select(attribute, outcome)
#         .with_columns(
#             pl.col(attribute)
#             .cast(pl.String())
#             .replace('0', f'NO {attribute}')
#             .replace('1', f'HAS {attribute}')
#         )
#         .with_columns(MEDIAN = pl.col(outcome).median().over(attribute))
#         .sort(attribute, descending=True)
#     )
#     no_median = get_median(df_box, attribute, f'NO {attribute}',  outcome)
#     has_median = get_median(df_box,attribute, f'HAS {attribute}', outcome)
#     median_shift = has_median - no_median
#     direction='decreased'
#     pct_color = 'red'
#     if median_shift > 0.0:
#         direction='increased'
#         pct_color = 'green'

#     fig = px.box(
#         df_box,
#         x=attribute,
#         y=outcome,
#         template='plotly_white',
#         title=(
#             f'<b>EFFECT OF {attribute} ON {outcome}</b><br>' +
#             get_subtitle(outcome, direction, pct_color, median_shift)
#         ),
#         color=attribute,
#         color_discrete_map = {
#             f'NO {attribute}' : color1,
#             f'HAS {attribute}' : color2,
#         },
#         points='all', # shows datapoints next to each box_plot item
#     )   
#     fig.update_xaxes(
#         categoryorder='array', 
#         categoryarray= [f'NO {attribute}', f'HAS {attribute}']
#     )
#     fig.update_layout(
#         yaxis_range=[0,100],
#         title_font=dict(size=24),
#     )
#     return fig

# def get_histogram(attribute, outcome):
#     ''' returns plotly graph objects histogram, created with px.box API '''
    
#     df_hist = (  # make data frame for this attribute, outcome
#         df
#         .select(attribute, outcome)
#         .with_columns(
#             pl.col(attribute)
#             .cast(pl.String())
#             .str.replace('0', f'NO {attribute}')
#             .str.replace('1', f'HAS {attribute}')
#         )
#         .sort(attribute, descending=True)
#     )
#     no_median = get_median(df_hist, attribute, f'NO {attribute}',  outcome)
#     has_median = get_median(df_hist,attribute, f'HAS {attribute}', outcome)
#     median_shift = has_median - no_median

#     direction='decreased'
#     pct_color = 'red'
#     if median_shift > 0.0:
#         direction='increased'
#         pct_color = 'green'
#     fig = px.histogram(
#         df_hist, 
#         x=outcome, 
#         color=attribute,
#         color_discrete_map = {
#             f'NO {attribute}' : color1,
#             f'HAS {attribute}' : color2,
#         },
#         template='plotly_white',
#         title=(
#             f'<b>EFFECT OF {attribute} ON {outcome}</b><br>' +
#             get_subtitle(outcome, direction, pct_color, median_shift)
#         ),
#     )
#     # Update layout for overlay and transparency
#     fig.update_layout(
#         barmode='overlay',
#         xaxis_title=outcome,
#         yaxis_title=attribute,
#         title_font=dict(size=24),
#     )
#     fig.update_traces(
#         marker_line_color='black',
#         marker_line_width=1.5
#     )
#     return fig

# #----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server
app.title = 'US Border Crossings, Value of Goods'

app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('US Border Crossings, Value of Goods', ta='center', style=style_h2),
    # dmc.Text('', ta='center', style=style_h3, id='zip_code'),
    html.Hr(style=style_horizontal_thick_line),
#     dmc.Space(h=30),
#     dmc.Grid(children = [
#         dmc.GridCol(dmc.Text('Dashboard Control Panel', 
#             fw=500, # semi-bold
#             style={'fontSize': 28},
#             ),
#             span=3, offset=1
#         )]
#     ),
#     dmc.Space(h=30),
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_group_by, span=2, offset = 1),
    ]),  
#     dmc.Space(h=75),
#     html.Hr(style=style_horizontal_thin_line),
#     dmc.Space(h=75),
#         dmc.Grid(children = [
#         dmc.GridCol(dmc.Text(
#             'Dashboard Output Panel', 
#             fw=500, # semi-bold
#             style={'fontSize': 28},
#             ),
#             span=3, offset=1
#         )]
#     ),

#     dmc.Grid(children = [
#         dmc.GridCol(
#             dmc.Text(
#                 'Data for each value of {attribute}, {outcome}', 
#                 ta='left', 
#                 style=style_h3,
#                 id='table-desc'
#             ),
#             span=3, offset=8
#         )]
#     ),
    dmc.Grid(  
        children = [
            dmc.GridCol(dcc.Graph(figure=fig_choro, id='choropleth'), span=6, offset=0),
            # dmc.GridCol(dcc.Graph(id='histogram'), span=4, offset=0), 
            # dmc.GridCol(dag.AgGrid(id='ag-grid'),span=3, offset=0),           
        ]
    ),
])

# @app.callback(
#     Output('boxplot', 'figure'),
#     Output('histogram', 'figure'),
#     Output('ag-grid', 'columnDefs'),  # columns vary by dataset
#     Output('ag-grid', 'rowData'),
#     Output('attr-title', 'children'),   
#     Output('attr-text', 'children'),
#     Output('outcome-title', 'children'),   
#     Output('outcome-text', 'children'),
#     Output('table-desc', 'children'),
#     Input('attribute', 'value'),
#     Input('outcome', 'value'),
# )
# def update(attribute, outcome):
#     box_plot = get_box_plot(attribute, outcome)
#     histogram = get_histogram(attribute, outcome)
#     df_table = (
#         df
#         .select('CANDY', attribute, outcome)
#         .sort(outcome,descending=True)
#     )
#     ag_col_defs = get_ag_col_defs(df_table.columns)    
#     ag_row_data = df_table.to_dicts()
#     table_desc = f'{attribute} and {outcome} data, all candies'
#     return (
#         box_plot, 
#         histogram,  
#         ag_col_defs, 
#         ag_row_data,
#         attribute,
#         dict_attr_desc[attribute],
#         outcome,
#         dict_outcome_desc[outcome],
#         table_desc
#     )

if __name__ == '__main__':
    app.run_server(debug=True)