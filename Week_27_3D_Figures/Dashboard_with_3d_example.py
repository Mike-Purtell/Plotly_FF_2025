import dash
from dash import dcc, html
import plotly.graph_objs as go
import numpy as np

# Sample 3D data
np.random.seed(42)
x = np.random.randn(100)
y = np.random.randn(100)
z = np.random.randn(100)

# Create the 3D plot
scatter_3d = go.Figure(
    data=[go.Scatter3d(
        x=x, y=y, z=z,
        mode='markers',
        marker=dict(size=5, color=z, colorscale='Viridis', opacity=0.8)
    )]
)

# Dash app layout
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("3D Scatter Plot with Plotly in Dash"),
    dcc.Graph(figure=scatter_3d)
])

if __name__ == '__main__':
    app.run_server(debug=True)