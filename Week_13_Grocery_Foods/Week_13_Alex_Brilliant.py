from dash import Dash, html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

# Initialize the Dash app with a modern Bootstrap theme
# Add suppress_callback_exceptions=True to avoid errors with dynamic components
app = Dash(__name__, external_stylesheets=[dbc.themes.UNITED], suppress_callback_exceptions=True)

# Load the GroceryDB_foods dataset and remove rows with NaN
df = pd.read_csv("GroceryDB_foods.csv").dropna()

df = df.reset_index(drop=True)

# Select relevant columns
nutritional_columns = [
    'Protein', 'Total Fat', 'Carbohydrate', 'Sugars, total',
    'Fiber, total dietary', 'Calcium', 'Iron', 'Sodium', 'Vitamin C',
    'Cholesterol', 'Fatty acids, total saturated', 'Total Vitamin A'
]

# Scale the dataset
scaler = MinMaxScaler()
df_scaled = pd.DataFrame(scaler.fit_transform(df[nutritional_columns]), columns=nutritional_columns)

# Custom style for the entire application
CONTENT_STYLE = {
    "marginLeft": "1rem",
    "marginRight": "1rem",
    "padding": "1rem 1rem",
}

# Colors in RGBA format to use transparency correctly
radar_colors = [
    'rgba(31, 119, 180, 0.8)',   # blue
    'rgba(255, 127, 14, 0.8)',   # orange
    'rgba(44, 160, 44, 0.8)',    # green
    'rgba(214, 39, 40, 0.8)',    # red
    'rgba(148, 103, 189, 0.8)',  # purple
    'rgba(140, 86, 75, 0.8)',    # brown
    'rgba(227, 119, 194, 0.8)',  # pink
    'rgba(188, 189, 34, 0.8)',   # olive
    'rgba(23, 190, 207, 0.8)'    # cyan
]

transparent_radar_colors = [
    'rgba(31, 119, 180, 0.2)',   # transparent blue
    'rgba(255, 127, 14, 0.2)',   # transparent orange
    'rgba(44, 160, 44, 0.2)',    # transparent green
    'rgba(214, 39, 40, 0.2)',    # transparent red
    'rgba(148, 103, 189, 0.2)',  # transparent purple
    'rgba(140, 86, 75, 0.2)',    # transparent brown
    'rgba(227, 119, 194, 0.2)',  # transparent pink
    'rgba(188, 189, 34, 0.2)',   # transparent olive
    'rgba(23, 190, 207, 0.2)'    # transparent cyan
]

app.title = "Smart Food Recommender"

# Modernized and reorganized application layout
app.layout = dbc.Container([
    # Header with colored background and centered text
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H3("Smart Food Recommender", className="display-4 text-white"),
                html.P("Find similar products based on their nutritional profile", className="lead text-white")
            ], className="p-2 bg-secondary rounded-3 text-center my-4")
        ])
    ]),

    # Product selection section (now in a separate row)
    dbc.Row([
        dbc.Col([
            dbc.Button("Select Product", id="open-modal-btn", color="primary", className="mb-3"),
            html.Div(id="product-alert"),  # Alert display here
        ], className="text-center")
    ]),

    # Modal for product selection
    dbc.Modal([
        dbc.ModalHeader("Select a product"),
        dbc.ModalBody([
            dcc.Dropdown(
                id="product-dropdown",
                options=[{"label": row['name'], "value": idx} for idx, row in df.iterrows()],
                placeholder="Search and select product...",
                className="mb-4",
                style={"fontSize": "14px"}  
            ),
        ]),
        dbc.ModalFooter(
            dbc.Button("Close", id="close-modal-btn", className="ms-auto", color="secondary")
        ),
    ], id="modal", size="lg"),

    # Section to display the currently selected product
    dbc.Row([
        dbc.Col([
            html.Div(id="selected-product", className="text-center mb-3 fw-bold fs-4"),
        ])
    ]),

    # Visualization options (RadioItems)
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.RadioItems(
                        id="visualization-options",
                        options=[
                            {"label": "Details and Recommendations", "value": "details-recommendations"},
                            {"label": "Nutritional Comparison", "value": "nutritional-comparison"}
                        ],
                        value="details-recommendations",
                        inline=True,
                        className="mb-2",
                        inputClassName="me-2",
                        labelClassName="me-3"
                    )
                ])
            ], className="shadow-sm mb-4")
        ])
    ]),

    # Container for the selected visualization
    html.Div(id="dynamic-content"),

    # IMPORTANT: Add empty components with correct IDs to avoid callback errors
    # These elements will be initially empty, but are necessary for the callbacks to function
    html.Div(id="product-details", style={"display": "none"}),
    html.Div(id="recommendations", style={"display": "none"}),
    dcc.Graph(id="radar-chart", style={"display": "none"}),

    # Footer
    dbc.Row([
        dbc.Col([
            html.Footer([
                html.P("© 2025 Smart Food Recommender", className="text-center text-muted")
            ], className="mt-5 pt-4 border-top")
        ])
    ])
], fluid=True, #style=CONTENT_STYLE
                          )

# Callback to open and close the modal
@app.callback(
    Output("modal", "is_open"),
    [Input("open-modal-btn", "n_clicks"), Input("close-modal-btn", "n_clicks")],
    [State("modal", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

# Callback to update dynamic content based on the selected option
@app.callback(
    Output("dynamic-content", "children"),
    [Input("visualization-options", "value"),
     Input("product-dropdown", "value")]
)
def update_content(selected_option, product_id):
    if selected_option == "details-recommendations":
        return dbc.Row([
            # Left column - Product details
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H6("Product Details", className="card-title mb-0"),
                    ], className="bg-light"),
                    dbc.CardBody([
                        html.Div(id="product-details-container"),  # Container for details
                    ])
                ], className="h-100 shadow-sm")
            ], md=5),

            # Right column - Recommendations
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H6("Recommended Products", className="card-title mb-0"),
                    ], className="bg-light"),
                    dbc.CardBody([
                        dbc.Row(id="recommendations-container", className="row-cols-1 row-cols-md-2 g-4")
                    ])
                ], className="h-100 shadow-sm")
            ], md=7)
        ])
    else:
        return dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H4("Nutritional Comparison", className="card-title mb-0"),
                    ], className="bg-light"),
                    dbc.CardBody([
                        dcc.Graph(id="radar-chart-container")
                    ])
                ], className="shadow-sm")
            ])
        ])

# Callback to display the selected product
@app.callback(
    Output("selected-product", "children"),
    [Input("product-dropdown", "value")]
)
def display_selected_product(product_id):
    if product_id is None:
        return "No product selected"
    try:
        product = df.loc[int(product_id)]
        return f"Selected product: {product['name']}"
    except Exception:
        return "Error loading product"

# Callback to display details, recommendations, charts and alerts
@app.callback(
    [Output("product-details", "children"),
     Output("recommendations", "children"),
     Output("radar-chart", "figure"),
     Output("product-alert", "children")],
    [Input("product-dropdown", "value")]
)
def update_recommendations(product_id):
    if product_id is None:
        # Return initial states when no product is selected
        return html.Div([
            html.P("Select a product to view its details and recommendations.", className="text-muted fst-italic")
        ]), [], go.Figure(layout=dict(title="Select a product to view the comparison")), None

    try:
        # Details of the selected product
        product = df.loc[int(product_id)]

        # Create nutritional information table
        nutrients_values = {
            'Nutrient': nutritional_columns,
            'Value': [round(product[col], 2) for col in nutritional_columns]
        }
        nutritional_table = dbc.Table.from_dataframe(
            pd.DataFrame(nutrients_values),
            striped=True,
            bordered=False,
            hover=True,
            responsive=True,
            className="mt-3"
        )

        details = html.Div([
            html.Div([
                html.H6(product['name'], className="mb-2"),
                html.P(f"Brand: {product['brand']}", className="mb-1 fs-5"),
                html.P([
                    html.Span("Price: ", className="fw-bold"),
                    html.Span(f"US${product['price']:.2f}", className="text-primary fs-4")
                ], className="mb-3"),
            ]),
            html.Hr(),
            html.H5("Nutritional Information", className="mt-4 mb-3"),
            nutritional_table
        ])

        # Evaluate product conditions for the alert
        unmet_criteria = 0
        criteria_text = []

        if product['Total Fat'] > 20:
            unmet_criteria += 1
            criteria_text.append("high in fats")
        if product['Sugars, total'] > 15:
            unmet_criteria += 1
            criteria_text.append("high in sugars")
        if product['Sodium'] > 500:
            unmet_criteria += 1
            criteria_text.append("high in sodium")

        # Determine alert color and message
        if unmet_criteria == 0:
            alert_color = "success"
            alert_message = "This product has a favorable nutritional profile."
            icon = "✓"
        elif unmet_criteria == 1:
            alert_color = "warning"
            alert_message = f"Caution: Product {criteria_text[0]}."
            icon = "⚠️"
        elif unmet_criteria == 2:
            alert_color = "warning"
            alert_message = f"Attention: Product {' and '.join(criteria_text)}."
            icon = "⚠️"
        else:
            alert_color = "danger"
            alert_message = "Not recommended: Product with multiple unfavorable nutritional factors."
            icon = "✗"

        alert = dbc.Alert([
            html.Span(icon, className="me-2"),
            html.Span(alert_message)
        ], color=alert_color, className="mt-3")

        # Calculate similarities for recommendations
        reference_product = df_scaled.iloc[[int(product_id)]]
        similarities = cosine_similarity(reference_product, df_scaled)
        df_temp = df.copy()
        df_temp['similarity'] = similarities[0]

       
        recommended = df_temp.sort_values(by='similarity', ascending=False).iloc[1:7]  # Show 6 recommendations

        cards = []
        for idx, row in recommended.iterrows():
            # Evaluate conditions for this recommended product
            unmet_conditions = 0
            if row['Total Fat'] > 20: unmet_conditions += 1
            if row['Sugars, total'] > 15: unmet_conditions += 1
            if row['Sodium'] > 500: unmet_conditions += 1

            # Determine card border color based on nutritional profile
            if unmet_conditions == 0:
                border_color = "success"
            elif unmet_conditions == 1:
                border_color = "warning"
            elif unmet_conditions == 2:
                border_color = "warning"
            else:
                border_color = "danger"

            # Calculate similarity percentage to display
            similarity_percentage = int(row['similarity'] * 100)

            card = dbc.Col(
                dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.Span(f"{similarity_percentage}% similar",
                                      className="badge bg-primary float-end")
                        ])
                    ], className=f"border-{border_color}"),
                    dbc.CardBody([
                        html.H6(row['name'], className="card-title text-truncate",
                                title=row['name']),
                        html.P(f"Brand: {row['brand']}", className="card-text"),
                        html.Div([
                            html.Span("US$", className="text-muted me-1"),
                            html.Span(f"{row['price']:.2f}", className="fw-bold fs-5")
                        ], className="mt-2")
                    ])
                ], className=f"h-100 shadow-sm border-{border_color}")
            )
            cards.append(card)

        # Create improved radar chart with correct RGBA format
        fig = go.Figure()

        # Add selected product with thicker line
        fig.add_trace(go.Scatterpolar(
            r=[product[col] for col in nutritional_columns],
            theta=nutritional_columns,
            fill='toself',
            name=product['name'],
            line=dict(color=radar_colors[0], width=3),
            fillcolor=transparent_radar_colors[0]  # Use RGBA color with transparency
        ))
        # Add all recommended products to the chart (limiting to available colors)
        max_products = min(len(recommended), len(radar_colors) - 1)  # -1 because we already used the first color
        for i, (_, row) in enumerate(recommended.iloc[:max_products].iterrows()):
            if i < len(radar_colors) - 1:  # Make sure we don't exceed available colors
                fig.add_trace(go.Scatterpolar(
                    r=[row[col] for col in nutritional_columns],
                    theta=nutritional_columns,
                    fill='toself',
                    name=row['name'],
                    line=dict(color=radar_colors[i + 1]),
                    fillcolor=transparent_radar_colors[i + 1]  # Use RGBA color with transparency
                ))

        # Improve chart design
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    showticklabels=True,
                    gridcolor="rgba(0,0,0,0.1)",
                ),
                angularaxis=dict(
                    gridcolor="rgba(0,0,0,0.1)",
                ),
                bgcolor="rgba(0,0,0,0.02)"
            ),
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5
            ),
            title={
                'text': "Nutritional Profile Comparison",
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            },
            margin=dict(l=80, r=80, t=100, b=100),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=600  # Fixed height for better visualization
        )

        return details, cards, fig, alert

    except Exception as e:
        # Error handling for debugging
        print(f"Error in callback: {e}")
        return html.Div([
            html.P(f"Error: Could not process request. {str(e)}", className="text-danger")
        ]), [], go.Figure(), dbc.Alert("Error processing data", color="danger")

# Additional callbacks to update dynamic containers
@app.callback(
    Output("product-details-container", "children"),
    [Input("product-details", "children")]
)
def update_details_container(details):
    return details

@app.callback(
    Output("recommendations-container", "children"),
    [Input("recommendations", "children")]
)
def update_recommendations_container(recommendations):
    return recommendations

@app.callback(
    Output("radar-chart-container", "figure"),
    [Input("radar-chart", "figure")]
)
def update_chart_container(figure):
    return figure

# Run the application
if __name__ == '__main__':
    app.run_server(debug=True,)