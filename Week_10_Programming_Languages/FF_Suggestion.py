from dash import Dash, dcc
import dash_ag_grid as dag

app = dash.Dash(__name__)

# Sample data (replace with your actual data)
data = {
    'Date': ['2023-01-01', '2023-01-02', '2023-01-03'],
    'Python': [10, 15, 20],
    'JavaScript': [5, 12, 18],
    'Java': [8, 11, 14],
    'C++': [3, 7, 9],
}

# All available programming languages
all_languages = ['Python', 'JavaScript', 'Java', 'C++']

app.layout = html.Div([
    dcc.Dropdown(
        id='language-dropdown',
        options=[{'label': lang, 'value': lang} for lang in all_languages],
        multi=True,
        value=[],
    ),
    dag.AgGrid(
        id='language-table',
        rowData=[{'Date': date, **{lang: data[lang][i] for lang in all_languages}} for i, date in enumerate(data['Date'])],
        columnDefs=[
            {'field': 'Date'},
            # Language columns will be added dynamically
        ],
        columnSize="sizeToFit",
    ),
])

@app.callback(
    Output('language-table', 'columnDefs'),
    Input('language-dropdown', 'value'),
)
def update_columns(selected_languages):
    base_columns = [{'field': 'Date'}]
    language_columns = [{'field': lang} for lang in selected_languages]
    return base_columns + language_columns

if __name__ == '__main__':
    app.run_server(debug=True)