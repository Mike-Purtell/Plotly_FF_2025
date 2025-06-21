import polars as pl
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
from dash_ag_grid import AgGrid
dash._dash_renderer._set_react_version('18.2.0')

#----- DATA GATHER AND CLEAN ---------------------------------------------------
df_zip = pl.read_csv('ZIP_INFO.csv')
print(df_zip)

df = (
    pl.scan_csv(
        'Building_Permits_Issued_Past_180_Days.csv',
        ignore_errors=True
    )
    .select(
        ID = pl.col('OBJECTID'),
        WORK = pl.col('workclass'),
        PROP_DESC = pl.col('proposedworkdescription'),
        TYPE = pl.col('permitclassmapped'),
        APP_DATE = pl.col('applieddate').str.split(' ').list.first().str.to_date(),
        ISS_DATE = pl.col('issueddate').str.split(' ').list.first().str.to_date(),
        EST_COST = pl.col('estprojectcost'),
        CEN_USE = pl.col('censuslanduse'),
        CONTRACTOR = pl.col('contractorcompanyname'),
        CONT_CITY = pl.col('contractorcity'),
        DESC = pl.col('description'),
        EXP_DATE = pl.col('expiresdate').str.split(' ').list.first().str.to_date(),
        FEE = pl.col('fee'),
        LAT = pl.col('latitude_perm'),
        LONG = pl.col('longitude_perm'),
        ZIP = pl.col('originalzip'),
        PROP_USE = pl.col('proposeduse'),
        STATUS = pl.col('statuscurrent'),
        EXIST_OR_NEW = pl.col('workclassmapped'),
    )
    .collect()
    .join(
        df_zip,
        on='ZIP',
        how='left'
    )
)

#----- GLOBALS -----------------------------------------------------------------
style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal'}

zip_code_list = (
    df
    .unique('ZIP')
    .sort('ZIP')
    .select(pl.col('ZIP'))
    .to_series()
    .to_list()
)
print(f'{len(zip_code_list) = }')
print(f'{zip_code_list = }')

#----- FUNCTIONS----------------------------------------------------------------
def get_zip_info(zip_code):
    return df_zip.filter(pl.col('ZIP')== zip_code)['INFO'].item()

def get_zip_table():
    df_dag = pl.DataFrame({
        'ZIP CODE:' : zip_code_list
    })
    row_data = df_dag.to_dicts()
    column_defs = [{"headerName": col, "field": col} for col in df_dag.columns]
    return (
        AgGrid(
            id='zip_code_table',
            rowData=row_data,
            columnDefs=column_defs,
            defaultColDef={"filter": True},
            columnSize="sizeToFit",
            getRowId='params.data.State',
            dashGridOptions={
                'rowSelection': 'single',
                'animateRows': False,
                'suppressCellSelection': True,
                'enableCellTextSelection': True,
                # Apply dynamic class rules via AG Grid
                # 'getRowClass': {"selected-row": "params.node.isSelected()"}
            },
            # HEIGHT IS SET TO HIGH VALUE TO SHOW MORE ROWS
            style={'height': '600px', 'width': '150%'},
        )
    )

def get_px_choro(zip_code):
    ''' returns plotly map_libre, type streets, magenta_r sequential  '''
    
    my_color_dict = {
        'BRIGHT' : 'red',
        'DIM'    : '#A0A0A0'
    }
    
    fig = px.scatter_map(
        df.with_columns(
            COLOR_TYPE = 
                pl.when(pl.col('ZIP') ==  zip_code)
                .then(pl.lit('BRIGHT'))
                .otherwise(pl.lit('DIM'))
        ),
        lat='LAT',
        lon='LONG',
        size='EST_COST', 
        color='COLOR_TYPE',
        color_discrete_map= my_color_dict,
        color_continuous_scale='Magenta_r',
        size_max=50,
        zoom=10,
        map_style='streets',
        custom_data=[
            'ID',                #  customdata[0]
            'WORK',              #  customdata[1]
            'PROP_DESC',         #  customdata[2]
            'TYPE',              #  customdata[3]
            'EXIST_OR_NEW',      #  customdata[4]
            'STATUS',            #  customdata[5]
            'PROP_USE',          #  customdata[6]
            'CONTRACTOR',        #  customdata[7]
        ],
        height=800, width =800
        # range_color=(0,30)  # max log of 7 means 10e6)
    )
    #------------------------------------------------------------------------------#
    #     Apply hovertemplate                                                      #
    #------------------------------------------------------------------------------#
    fig.update_traces(
        hovertemplate =
            '<b>ID: %{customdata[0]}<br>' + 
            '<b>WORK: %{customdata[1]}<br>' + 
            '<b>PROP_DESC: %{customdata[2]}<br>' + 
            '<b>TYPE: %{customdata[3]}<br>' + 
            '<b>EXIST_OR_NEW: %{customdata[4]}<br>' + 
            '<b>STATUS: %{customdata[5]}<br>' + 
            '<b>PROP_USE: %{customdata[6]}<br>' + 
            '<b>CONTRACTOR: %{customdata[7]}<br>' + 
            '<extra></extra>'
    )
    fig.update_layout(
        hoverlabel=dict(
            bgcolor="white",
            font_size=16,
            font_family='courier',
        ),
        showlegend=False
    )
    return fig

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('Raleigh North Carolina - Contractor Data', ta='center', style=style_h2),
    dmc.Text('', ta='center', style=style_h3, id='zip_code'),
    html.Hr(style=style_horiz_line),
    dmc.Space(h=30),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(dcc.Graph(id='px_choro'), span=4, offset=1),
            dmc.GridCol(get_zip_table(), span=1, offset=3)
        ]
    ),
])

@app.callback(
    Output('px_choro', 'figure'),
    Output('zip_code', 'children'),
    Input('zip_code_table', 'cellClicked'),
)
def update_dashboard(zip_code):  # line_shape, scale, test, dag_test):
    if zip_code is None:
        zip_code=zip_code_list[0]
    else:
        zip_code = zip_code["value"]
    print(f'{zip_code = }')
    px_choro = get_px_choro(zip_code)
    # px_hist_fine = get_px_hist(df, violation_name, 'FINE_AMT')
    # px_hist_paid = get_px_hist(df, violation_name, 'PAY_AMT')
    # px_hist_period = get_px_hist(df, violation_name, 'JUDGE_DAYS')
    return (
        px_choro, 
        f'Zip Code {zip_code}: {get_zip_info(zip_code)}'
    )

if __name__ == '__main__':
    app.run(debug=True)

