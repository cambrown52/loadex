"""
Loadex Browser - Multi-page Dash application for visualizing wind turbine loads data
"""
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Initialize the Dash app with multi-page support
app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True
)

# App layout with navigation
app.layout = dbc.Container([
    dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Upload Database", href="/")),
            dbc.NavItem(dbc.NavLink("File List", href="/filelist")),
            dbc.NavItem(dbc.NavLink("Sensor List", href="/sensorlist")),
            dbc.NavItem(dbc.NavLink("Plot", href="/plot")),
        ],
        brand="Loadex Browser",
        brand_href="/",
        color="primary",
        dark=True,
        className="mb-3"
    ),
    
    # Store for the dataset (will store serialized data)
    dcc.Store(id='session-id-store', storage_type='session'),
    dcc.Store(id='dataset-metadata', storage_type='session'),
    
    # Page content
    dash.page_container
], fluid=True)

if __name__ == '__main__':
    app.run(debug=True, port=8050,dev_tools_hot_reload=True)
