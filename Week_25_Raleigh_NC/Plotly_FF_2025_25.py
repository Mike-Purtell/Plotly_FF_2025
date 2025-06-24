import polars as pl
import plotly.express as px
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
from dash_ag_grid import AgGrid
dash._dash_renderer._set_react_version('18.2.0')

#----- GATHER AND CLEAN DATA ---------------------------------------------------

df_zip = pl.read_csv('ZIP_INFO.csv') # has descriptions of each zip code

df = (
    pl.scan_csv(
        'Building_Permits_Issued_Past_180_Days.csv',
        ignore_errors=True
    )
    .select(
        ID = pl.col('OBJECTID'),
        WORK = pl.col('workclass'),
        PROP_DESC = pl.col('proposedworkdescription'),
        TYPE = pl.col('permitclassmapped'),   # commercial or residential
        EST_COST = pl.col('estprojectcost'),
        CONTRACTOR = pl.col('contractorcompanyname'),
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
print(df.glimpse())


#----- GLOBALS -----------------------------------------------------------------
style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal'}

zip_code_list = sorted(df.unique('ZIP')['ZIP'])

map_styles = ['basic', 'carto-darkmatter', 'carto-darkmatter-nolabels', 
    'carto-positron', 'carto-positron-nolabels', 'carto-voyager', 
    'carto-voyager-nolabels', 'dark', 'light', 'open-street-map', 
    'outdoors', 'satellite', 'satellite-streets', 'streets', 'white-bg'
]

#----- FUNCTIONS----------------------------------------------------------------
def get_zip_info(zip_code):
    return df_zip.filter(pl.col('ZIP')== zip_code)['INFO'].item()

def get_zip_table():
    df_zip_codes =pl.DataFrame({
        'SELECT A ZIP CODE:' : zip_code_list
    })
    row_data = df_zip_codes.to_dicts()
    column_defs = [{"headerName": col, "field": col} for col in df_zip_codes.columns]
    return (
        AgGrid(
            id='zip_code_table',
            rowData=row_data,
            columnDefs=column_defs,
            defaultColDef={"filter": True},
            columnSize="sizeToFit",
            getRowId='params.data.State',
            # HEIGHT IS SET TO HIGH VALUE TO SHOW MORE ROWS
            style={'height': '600px'} # , 'width': '150%'},
        )
    )
def get_info_table(id=271601):
    print(f'{id = }')

    df_info = (
        df
        .filter(pl.col('ID') ==  id)
        .transpose(
            include_header=True, header_name='ITEM'
        )
        .rename({'column_0':'VALUE'})
    )
    print(df_info)
    # row_data = df_info.to_dicts()
    column_defs = [{"headerName": col, "field": col} for col in df_info.columns]
    return (
        AgGrid(
            id='info_table',
            rowData=df_info.to_dicts(),
            columnDefs=column_defs,
            defaultColDef={"sortable": True, "filter": True, "resizable": True},
            columnSize="sizeToFit",
            # getRowId='params.data.State',
            # dashGridOptions={
            #     # 'rowSelection': 'single',
            #     'animateRows': False,
            #     'suppressCellSelection': True,
            #     'enableCellTextSelection': True,
            # },
            # HEIGHT IS SET TO HIGH VALUE TO SHOW MORE ROWS
            style={'height': '800px'}, # , 'width': '150%'},
        )
    )

def get_px_scatter_map(zip_code, this_map_style, property_type):
    ''' returns plotly map_libre, type streets, magenta_r sequential  '''
    
    color_dict={}
    if property_type == 'Residential':         
        color_dict = {
            'BRIGHT'    : 'blue',
            'DIM'       : 'lightgray'
        }
    else:
        color_dict = {
            'BRIGHT'   : 'green',
            'DIM'      : 'lightgray'
        }
    df_map = ( # set COLOR TYPE, and filter by residential or non-residential
        df
            .with_columns(
                COLOR_TYPE = 
                    pl.when(pl.col('ZIP') ==  zip_code)
                    .then(
                        pl.when(pl.col('TYPE') == property_type)
                        .then(pl.lit('BRIGHT'))
                        .otherwise(pl.lit('DIM'))
                    )
                    .otherwise(pl.lit('DIM'))
            )
            .with_columns(
                MARKER_SIZE = 
                    pl.when(
                        (
                            (pl.col('ZIP') ==  zip_code) &
                            (pl.col('TYPE') == property_type)
                        )
                    )
                    .then(pl.lit(4))
                    .otherwise(pl.lit(1))
            )
    )

    fig = px.scatter_map(
        df_map,
        title = f'ZIP CODE: {zip_code}  {property_type}',
        lat='LAT',
        lon='LONG',
        size='MARKER_SIZE', 
        color='COLOR_TYPE',
        color_discrete_map= color_dict,
        color_continuous_scale='Magenta_r',
        size_max=10,
        zoom=10,
        map_style=this_map_style,       
        custom_data=[
            'ID',                 #  customdata[0]
            'ZIP',                #  customdata[1]
            'TYPE',               #  customdata[2]
            'EXIST_OR_NEW',       #  customdata[3]
            'EST_COST',           #  customdata[4]
            'WORK',               #  customdata[5]
            'STATUS',             #  customdata[6]
            'CONTRACTOR',         #  customdata[7]
            'PROP_USE',           #  customdata[8]
        ],
        height=800, width =800
    )
    #----- APPLY HOVER TEMPLATE ------------------------------------------------
    fig.update_traces(
        hovertemplate =
            '<b>ID:</b> %{customdata[0]}<br>' + 
            '<b>ZIP:</b> %{customdata[1]}<br>' + 
            '<b>TYPE:</b> %{customdata[2]}, %{customdata[3]}<br>' + 
            '<b>EST_COST:</b> $%{customdata[4]:,.0f}<br>' + 
            '<b>WORK:</b> %{customdata[5]}<br>' + 
            '<b>STATUS:</b> %{customdata[6]}<br>' + 
            '<b>CONTRACTOR:</b> %{customdata[7]}<br>' + 
            '<b>PROPOSED USE:</b> %{customdata[8]}<br>' + 
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

#----- DASHBOARD COMPONENTS ----------------------------------------------------
dmc_select_map_type = (
    dmc.Select(
        label='Map Style',
        placeholder="Select one",
        id='id_map_style',    # default value
        value='open-street-map',
        data=[{'value' :i, 'label':i} for i in map_styles],
        maxDropdownHeight=600,
        w=400,
        mb=10, 
        size='xl'
    ),
)

dmc_radio_property_type = (
    dmc.RadioGroup(
        label='Property Type',
        id='property_type',
        value='Residential',
        children=dmc.Group(
            [dmc.Radio(i, value=i) for i in ['Residential', 'Non-Residential']], 
        ),
        w=400,
        size="lg",
        mt=30,
        my=10
    ),
)

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
            dmc.GridCol(dmc_select_map_type, span=2, offset = 1),
            dmc.GridCol(dmc_radio_property_type, span=2, offset = 2),
        ]
    ),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(get_zip_table(), span=2, offset=1),
            dmc.GridCol(dcc.Graph(id='px_scatter_map'), span=4, offset=1),
            dmc.GridCol(get_info_table(), span=3, offset=1)
        ]
    ),
])

@app.callback(
    Output('px_scatter_map', 'figure'),
    Output('zip_code', 'children'),
    Output('info_table', 'rowData'),
    Input('zip_code_table', 'cellClicked'),
    Input('id_map_style', 'value'),
    Input('property_type', 'value'),
    Input('px_scatter_map', 'hoverData'),
)
def update_dashboard(zip_code, map_style, property_type, hover_data):
    if zip_code is None:
        zip_code=zip_code_list[0]
    else:
        zip_code = zip_code["value"]
    selected_id=271601
    if hover_data is None:
        pass
    else:
        selected_id = hover_data['points'][0]['customdata'][0]

    print(f'{selected_id = }')
    px_scatter_map = get_px_scatter_map(zip_code, map_style, property_type)
    # info_table = get_info_table(selected_id)
    info_table_df = (
        df
        .filter(pl.col('ID') ==  selected_id)
        .transpose(
            include_header=True, header_name='ITEM'
        )
        .rename({'column_0':'VALUE'})
    )
    print(info_table_df)
    return (
        px_scatter_map, 
        f'Zip Code {zip_code}: {get_zip_info(zip_code)}',
        info_table_df.to_dicts()
    )

if __name__ == '__main__':
    app.run(debug=True)