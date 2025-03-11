from dash import Dash, dcc
import dash_ag_grid as dag
import plotly.express as px
import pandas as pd

df = pd.read_csv("https://raw.githubusercontent.com/plotly/Figure-Friday/refs/heads/main/2025/week-10/Popularity%20of%20Programming%20Languages%20from%202004%20to%202024.csv")

# fig = px.line(df, x="Date", y='C/C++')
# fig = px.line(df, x="Date", y=['C/C++', 'Python'])
# fig = px.line(df, x="Date", y=['VBA', 'Python'])
fig = px.line(df, x="Date", y=['Matlab', 'Python'])
grid = dag.AgGrid(
    rowData=df.to_dict("records"),
    columnDefs=[{"field": i, 'filter': True, 'sortable': True} for i in df.columns],
    dashGridOptions={"pagination": True}
)

app = Dash()
app.layout = [
    grid,
    dcc.Graph(figure=fig)
]


if __name__ == "__main__":
    app.run(debug=True)