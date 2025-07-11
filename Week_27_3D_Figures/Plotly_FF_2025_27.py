import polars as pl
import numpy as np
from scipy.interpolate import griddata
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

# Groundwater salinity data from recent work.
df = (
    pl.read_csv("model-grid-subsample.csv")
    .filter(pl.col('dem_m') > (pl.col('zkm')* 1e3))
)

grid = df[['xkm', 'ykm', 'zkm', 'mean_tds']].to_numpy()
x = grid[:, 0] * 1e3 # Kilometers to meters.
y = grid[:, 1] * 1e3
z = grid[:, 2] * 1e3
u = grid[:, 3]

# Digital Elevation Model (DEM) - the land surface in the study area.
dem_grid = df[['xkm', 'ykm', 'dem_m']].to_numpy()

x_dem = dem_grid[:, 0] * 1e3
y_dem = dem_grid[:, 1] * 1e3
z_dem = dem_grid[:, 2]

# Interpolate the land surface point data.
xi_dem = np.linspace(min(x_dem), max(x_dem), 200)
yi_dem = np.linspace(min(y_dem), max(y_dem), 200)
zi_dem = griddata(
    (
        x_dem, 
        y_dem
    ), 
    z_dem, 
    (
        xi_dem.reshape(1, -1), 
        yi_dem.reshape(-1, 1)
    )
)


#----- GLOBALS -----------------------------------------------------------------
style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}
bg_color = 'lightgray'

#----- FUNCTIONS ---------------------------------------------------------------

def update_3d_layout(fig, fig_title):
    fig.update_layout(
        title=fig_title,
        title_automargin=True,
        # margin=dict(l=20, r=50, b=20, t=20),
        margin=dict(l=20, r=50, b=20, t=0),
        scene=dict(
                xaxis=dict(
                    title='Easting (m)', 
                    color='black', 
                    showbackground=True, 
                    backgroundcolor=bg_color
                ), 
                yaxis=dict(
                    title='Northing (m)', 
                    color='black', 
                    showbackground=True, 
                    backgroundcolor=bg_color
                ), 
                zaxis=dict(
                    title='Elevation (m)', 
                    color='black', 
                    showbackground=True, 
                    backgroundcolor=bg_color
                ), 
                aspectratio=dict(x=1, y=1, z=1), # changed z from 0.25 to 1.
                camera = dict( # Make north pointing up.
                            up=dict(x=0, y=0, z=1),
                            center=dict(x=0, y=0, z=-0.2),
                            eye=dict(x=-1., y=-1.3, z=1.)
                            )
                        ),
        )
    return fig
# Make the 3-d graph.
trace_dem = go.Figure(go.Surface(
    x=xi_dem, y=yi_dem, z=zi_dem,
    colorscale='Earth',
    name='Land surface',
    showscale=False,
    showlegend=False,
))
trace_dem = update_3d_layout(trace_dem, 'trace_dem Surface')
trace_dem.update_traces(contours_z=dict(
    show=True, usecolormap=True,
    highlightcolor="limegreen",
    project_z=True))

scatter_ticks = (  # replaced hardcoded values with list compreshensions
    [t for t in range(400, 1000, 100)] +     # 200 to 800, step 100
    [t for t in range(1000, 11000, 1000)]    # 1K to 10K, step 1K
)

trace_groundwater = go.Figure(go.Scatter3d(
    x=x, y=y, z=z, 
    mode='markers',
    name='Groundwater salinity',
    showlegend=True,
    marker=dict(
        size=3, 
        symbol='square', 
        colorscale='RdYlBu_r', 
        color=np.log10(u), # Log the colorscale.
        colorbar=dict(
            title=dict(
                text='Salinity (mg/L)', side='right'
            ),
            x=0.94,  # Move cbar over.
            len=0.5,  # Shrink cbar.
            ticks='outside',
            tickvals=np.log10(scatter_ticks),
            ticktext=scatter_ticks,                                                       ))
))
trace_groundwater = update_3d_layout(trace_groundwater, 'trace_groundwater Scatter3D')

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('3D Visualizations', ta='center', style=style_h2),
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Space(h=100),
    dmc.Grid(
        children = [ 
            dmc.GridCol(dcc.Graph(figure=trace_dem),span=5, offset=1),
            dmc.GridCol(dcc.Graph(figure=trace_groundwater),span=5, offset=1),
        ]
    ),
])
if __name__ == '__main__':
    app.run(debug=True)