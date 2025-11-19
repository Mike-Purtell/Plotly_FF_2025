import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd

# Sample data
df = pd.DataFrame({
    "Year": [2018, 2019, 2020, 2021, 2022],
    "Sales": [100, 200, 300, 400, 500]
})

# Create a Plotly figure
fig = px.line(df, x='Year', y='Sales', title='Sales by Year')

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    html.H1("Simple Plotly Dash Dashboard"),
    dcc.Graph(figure=fig)
])

# Run the app
if __name__ == "__main__":
    app.run(debug=True)