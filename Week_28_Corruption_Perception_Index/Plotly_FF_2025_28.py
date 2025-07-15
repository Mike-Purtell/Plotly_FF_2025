import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import dash
import dash_ag_grid as dag
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

# cf_region maps regional abbreviations to their full regional names
# added full regional names with a left join
df_region = (
    pl.DataFrame({
        'REGION_ABBR' : ['AME', 'AP', 'ECA', 'MENA', 'SSA','WE/EU'],
        'REGION' : [
            'AMERICAS','ASIA PACIFIC', 'EUROPE CENTRAL ASIA',
            'MIDDLE EAST, NORTH AFRICA','SUB-SAHARAN AFRICA','WESTERN EUROPE, '
            'EUROPEAN UNION'
        ]
    })
)

df_cpi = (
    pl.scan_csv('CPI2024.csv')
    .select(
        COUNTRY = pl.col('Country / Territory').str.to_uppercase(),
        ISO3 = pl.col('ISO3'), # no mods to ISO3 column
        REGION_ABBR = pl.col('Region'),
        RANK_2024= pl.col('Rank'),
    )
    .collect()
    .join(
        df_region,
        on='REGION_ABBR'
    )
    .sort(['REGION', 'RANK_2024'])
)

df_historical = (
    pl.scan_csv('CPI-historical.csv')
    .select(
        COUNTRY = (
            pl.col('Country / Territory')
            .str.to_uppercase()
            .replace('UNITED STATES OF AMERICA', 'UNITED STATES')
        ),
        ISO3 = pl.col('ISO3'), # no mods to ISO3 column
        YEAR = pl.col('Year').cast(pl.String).str.slice(2,2),
        REGION_ABBR = pl.col('Region'),
        RANK= pl.col('Rank'),
    )
    .collect()
    .join(
        df_region,
        on='REGION_ABBR'
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
region_list = sorted(
    df_historical.unique('REGION')
    .select(pl.col('REGION'))
    ['REGION']
    .to_list()
)

country_list = sorted(
    df_historical.unique('COUNTRY')
    .select(pl.col('COUNTRY'))
    ['COUNTRY']
    .to_list()
)

#----- FUNCTIONS ---------------------------------------------------------------

def get_choropleth():
    choropleth = px.choropleth(
        df_cpi.sort('RANK_2024'), 
        color='RANK_2024',
        locations="ISO3", 
        hover_name='COUNTRY',
        title='country level corruption rank 2024'.upper(),
        custom_data=['REGION', 'REGION_ABBR', 'COUNTRY', 'RANK_2024'],
    )
    choropleth.update_layout(
        margin={"r":0,"t":30,"l":0,"b":10}, 
        showlegend=True,
        hoverlabel=dict(
                bgcolor="white",
                font_size=16,
                font_family='arial',
            ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5,
            font=dict(family='Arial', size=legend_font_size, color='black'),
            title=dict(
                text='<b>COUNTRY</b>',
                font=dict(family='Arial', size=legend_font_size, color='black'),
            )
        )
    )
    choropleth.layout.coloraxis.colorbar.title = 'RANK'

    choropleth.update_traces(
        hovertemplate =
            '<b>%{customdata[0]} (%{customdata[1]})</b><br>' + # Region name,abbr
            '%{customdata[2]}<br>' + 
            'Worldwide Rank: %{customdata[3]}' + 
            '<extra></extra>',
    )
    return choropleth

#----- DASH COMPONENTS ---------------------------------------------------------
grid = dag.AgGrid(
    rowData=[],
    columnDefs=[{
        "field": i,
        'filter': True, 
        'sortable': True
        } for i in df_historical.columns
    ],
    dashGridOptions={"pagination": True},
    columnSize="sizeToFit",
    id='ag-grid'
)
dmc_select_region = dmc.RadioGroup(
    children= dmc.Group([dmc.Radio(i, value=i) for i in region_list], my=10),
    value=region_list[0],
    label= 'Select a Region',
    size='sm',
    mt=10,
    id='select-region'
)

dmc_select_country = dmc.Select(
    label='Select a country',
    data=country_list,
    value='',
    checkIconPosition='left',
    size='sm',
    # mt=10,
    id='select-country',
)

def get_px_line(region, focus_country):
    country_list = (
        df_historical
        .filter(pl.col('REGION') == region)
        .unique('COUNTRY')
        ['COUNTRY']
        .to_list()
    )
    df_plot =(
        df_historical
        .filter(pl.col('COUNTRY').is_in(country_list))
        .pivot(
            on='COUNTRY',
            index='YEAR',
            values='RANK'
        )
        .sort('YEAR')
    )
    fig = go.Figure()
    for i, country in enumerate(country_list, start=1):
        scatter_mode='lines'
        trace_color ='#D3D3D3'
        line_width=1
        z=0  # for z order for unfocused countries
        if focus_country == country:
            trace_color='#0000FF'
            line_width=2
            scatter_mode='lines+markers'
            z=1
        x = df_plot['YEAR']
        y = df_plot[country]
        hover_text = [
            f"{country}<br>YEAR: 20{x_val}<br>WW RANK: {y_val}" 
            for x_val, y_val in zip(x, y)
        ]
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode=scatter_mode,
                name=f'Trace {i+1}',
                line=dict(color=trace_color),
                marker=dict(color=trace_color),
                hoverinfo='text',
                hovertext=hover_text,
                zorder=z
            )
        )
    fig.update_traces(showlegend=False)
    fig.update_layout(
        title=(
            f'REGION: {region}<br>' +
            f'<sup>Worldwide Rank <span style="color: blue"> <b>{focus_country}</b></span></sup>'
        ),
        yaxis_title='Worldwide Rank',
        xaxis_title='YEAR (20XX)',
        template='simple_white'
    )
    return fig

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('Country Level Corruption'.upper(), ta='center', style=style_h2),
    dmc.Space(h=20),
    html.Hr(style=style_horiz_line),
    dmc.Space(h=20),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(dmc_select_region, span=7, offset=1),
            dmc.GridCol(dmc_select_country, span=3, offset=0)
        ]
    ),
    html.Hr(style=style_plain_line),
    dmc.Space(h=20),
    dmc.Grid(
        children = [ 
            dmc.GridCol(dcc.Graph(figure=get_choropleth()),id='choropleth', span=6, offset=1),
            dmc.GridCol(dcc.Graph(id='px_line'),span=5, offset=0),
        ]
    ),
    dmc.Grid(
        children = [
            dmc.GridCol(grid, span=8, offset=1),
        ]
    ),
])

#----- CALLBACKS----------------------------------------------------------
@app.callback(
    Output('select-country', 'data'),  # pulldown menu choices
    Output('select-country', 'value'), # pulldown menu default value 
    Input('select-region', 'value'),
)
def update_country_list(region):
    callback_country_list = sorted(
        df_cpi
        .filter(pl.col('REGION') == region)
        .select(pl.col('COUNTRY'))
        ['COUNTRY']
        .to_list()
    )
    default_country = callback_country_list[0]
    return (
        callback_country_list,  # pulldown menu choices
        default_country         # pulldown menu default value 
    )

@app.callback(
    Output('ag-grid', 'rowData'), 
    Output('px_line', 'figure'),  
    Input('select-region', 'value'),
    Input('select-country', 'value'),
)
def update_dashboard(region, country):
    df_ag_grid = (
        df_historical
        .filter(pl.col('COUNTRY') == country)
        .sort('YEAR')
    )
    row_data = df_ag_grid.to_dicts()

    px_line = get_px_line(region, country)
    return (
        row_data, px_line
    )

#----- RUN THE APP -------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)