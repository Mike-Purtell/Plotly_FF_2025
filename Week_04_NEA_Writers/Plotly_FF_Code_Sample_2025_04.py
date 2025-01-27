from dash import Dash, dcc
import dash_ag_grid as dag
import plotly.express as px
import pandas as pd

df = pd.read_csv("https://raw.githubusercontent.com/plotly/Figure-Friday/refs/heads/main/2025/week-4/Post45_NEAData_Final.csv")
df['age of writer'] = df.nea_grant_year - df.birth_year
fig = px.histogram(df, x='age of writer')

app = Dash()
grid = dag.AgGrid(
    rowData=df.to_dict("records"),
    columnDefs=[{"field": i} for i in df.columns],
    dashGridOptions={"pagination": True}
)

app.layout = [
    grid,
    dcc.Graph(figure=fig)
]


if __name__ == "__main__":
    app.run(debug=True)