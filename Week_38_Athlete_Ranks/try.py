import dash
from dash import dcc, html
import dash_mantine_components as dmc
import dash_core_components as dcc

app = dash.Dash(__name__)

app.layout = html.Div([
    dmc.CheckboxGroup(
        label="Select your favorite fruits",
        options=[
            {"label": "Apple", "value": "apple"},
            {"label": "Banana", "value": "banana"},
            {"label": "Orange", "value": "orange"},
            {"label": "Grapes", "value": "grapes"}
        ],
        value=["apple", "banana"],  # Default selected values
    ),
    html.Div(id='output')
])

@app.callback(
    dash.dependencies.Output('output', 'children'),
    [dash.dependencies.Input('checkbox-group', 'value')]
)
def display_selected_options(value):
    return f"You have selected: {', '.join(value)}"


if __name__ == '__main__':
    app.run_server(debug=True)