from dash import Dash, dcc, html, Input, Output, jupyter_dash
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import plotly.express as px
import polars as pl
from datetime import datetime

x_vals = [x for x in range(1,11,1)]
y_vals = [x*x for x in x_vals]
df = pl.DataFrame(
    {
        'PARITY' :['ODD', 'EVEN']*5,
        'X' : x_vals,
        'Y' : y_vals,
    }
)

grid = dag.AgGrid(
    rowData=df.to_pandas().to_dict("records"),
    columnDefs=[{"field": i, 'filter': True, 'sortable': True} for i in df.columns],
    dashGridOptions={"pagination": True}
)

#  app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
# app = Dash(external_stylesheets=[dbc.themes.COSMO]
app = Dash(external_stylesheets=[dbc.themes.SANDSTONE])
#  app = JupyterDash(__name__)

# app.layout = dbc.Container([
#     dbc.Row([dcc.Dropdown(['EVEN', 'ODD'],'ODD' , id='demo-dropdown'),]),
#     html.Div(id='dd-output-container'),
#     dbc.Row([
#         dbc.Col(dcc.Graph(id='fig_line')),
#         dbc.Col(dcc.Graph(id='fig_scatter')),
#     ]),
#     dbc.Row([
#         dbc.Col([grid], id='my_grid'),
#     ]),
# ])
app.layout = dbc.Container([
    dbc.Row([dcc.Dropdown(['EVEN', 'ODD'],'ODD' , id='demo-dropdown'),]),
    html.Div(id='dd-output-container'),
    dbc.Row([
        dbc.Col(dcc.Graph(id='fig_line')),
        dbc.Col(dcc.Graph(id='fig_scatter')),
    ]),
    dbc.Row([
        dbc.Col([grid], id='my_grid'),
    ]),
])
df_selected = df
@app.callback(
    Output('fig_line', 'figure'),
    Output('fig_scatter', 'figure'),
    # Output('my_grid', 'grid'),
    Input('demo-dropdown', 'value')
)
def update_dashboard(selected_group):
    df_selected = df.filter(pl.col('PARITY') == selected_group)
    # my_grid = dag.AgGrid(
    #     rowData=df_selected.to_pandas().to_dict("records"),
    #     columnDefs=[{"field": i, 'filter': True, 'sortable': True} for i in df.columns],
    #     dashGridOptions={"pagination": True}
    # )
    x_ticks = sorted(df_selected['X'].to_list())

    fig_line = px.line(
        df_selected, 
        x='X', 
        y='Y',
        markers=True,
        template='simple_white',
        height=400, width=600,
        line_shape='spline',
        text='Y'
    )
    fig_line.update_layout(
        title=f'Line with X-Values {selected_group}'.upper(),
        xaxis = dict(tickmode = 'array', tickvals = x_ticks),
    )
    fig_line.update_traces(textposition='top center')
    fig_scatter = px.scatter(
        df_selected, 
        x='X', 
        y='Y',
        text='Y',
        template='simple_white',
        height=400, width=600,
        )
    fig_scatter.update_layout(
        title=f'Scatter plot with X-Values {selected_group}'.upper(),
        xaxis = dict(tickmode = 'array', tickvals = x_ticks),
    )
    fig_scatter.update_traces(textposition='top center')
    return fig_line, fig_scatter# , my_grid

# # Run the app
if __name__ == '__main__':
    # app.run_server(debug=True)
    app.run(jupyter_height=500, jupyter_width="70%")