import plotly.express as px
import pandas as pd

df = pd.read_csv("https://raw.githubusercontent.com/plotly/Figure-Friday/refs/heads/main/2025/week-5/Steam%20Top%20100%20Played%20Games%20-%20List.csv")

# Convert columns' data from string to float or integer
df["Price"] = df["Price"].replace("Free To Play", 0.0)
df["Price"] = df["Price"].astype(str).str.replace("£", "", regex=False).astype(float)
df["Current Players"] = df["Current Players"].str.replace(",", "").astype(int)
df["Peak Today"] = df["Peak Today"].str.replace(",", "").astype(int)


fig = px.scatter(df, x="Price", y="Current Players", marginal_x="histogram", marginal_y="rug")#
fig.show()