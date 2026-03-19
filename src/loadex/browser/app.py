"""
Loadex Browser - Multi-page Dash application for visualizing wind turbine loads data
"""
import dash
from dash import Input, Output, State, callback, dcc, html
import dash_bootstrap_components as dbc

from loadex.browser.session_cache import get_dataset


def _build_metadata_summary(dataset_name, num_files, num_sensors, num_dlcs, filename, description):
    return dbc.Card(
        dbc.CardBody([
            html.H4(dataset_name, className="card-title mb-2"),
            html.P(description, className="text-muted mb-4"),
            dbc.Row([
                dbc.Col([
                    html.Div("Files", className="text-muted small text-uppercase"),
                    html.Div(str(num_files), className="fs-4 fw-semibold"),
                ], md=4, xs=6),
                dbc.Col([
                    html.Div("Sensors", className="text-muted small text-uppercase"),
                    html.Div(str(num_sensors), className="fs-4 fw-semibold"),
                ], md=4, xs=6),
                dbc.Col([
                    html.Div("DLCs", className="text-muted small text-uppercase"),
                    html.Div(str(num_dlcs), className="fs-4 fw-semibold"),
                ], md=4, xs=12),
            ], className="g-3 mb-4"),
            html.Div("Source Database", className="text-muted small text-uppercase"),
            html.Div(str(filename), className="fw-semibold text-break mb-0"),
        ]),
        className="shadow-sm"
    )


# Initialize the Dash app with multi-page support
app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.icons.BOOTSTRAP, dbc.themes.FLATLY],
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
            dbc.Button(
                id='dataset-metadata-button',
                color="light",
                outline=True,
                className="bi bi-database fs-5 ms-lg-2",
                title="Dataset Metadata",   # native browser tooltip
                disabled=True,  # Initially disabled until a dataset is loaded
            ),
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
    dcc.Store(id='dataset-metadata-open-request'),

    dbc.Offcanvas(
        html.Div(id='dataset-metadata-panel'),
        id='dataset-metadata-offcanvas',
        title="Current Dataset Metadata",
        placement='top',
        is_open=False,
        scrollable=True,
        style={'height': 'auto', 'maxHeight': '70vh'}
    ),
    
    # Page content
    dash.page_container
], fluid=True)


@callback(
    Output('dataset-metadata-panel', 'children'),
    [Input('dataset-metadata', 'data'),
     Input('session-id-store', 'data')],
    prevent_initial_call=False
)
def render_dataset_metadata(metadata, session_id):
    if metadata is None:
        return dbc.Alert(
            "No dataset loaded. Upload a database file to view dataset metadata.",
            color="secondary",
            className="mb-0",
        )

    dataset = get_dataset(session_id) if session_id is not None else None

    if dataset is None:
        return dbc.Alert(
            "The previously loaded dataset is no longer available in memory. Upload the database again to view its metadata.",
            color="warning",
            className="mb-0",
        )

    return _build_metadata_summary(
        dataset_name=dataset.name,
        num_files=len(dataset.filelist),
        num_sensors=len(dataset.sensorlist),
        num_dlcs=len(dataset.dlcs),
        filename=metadata.get('filename', 'Unknown'),
        description="This metadata reflects the dataset currently loaded in memory. Use the navigation bar button to reopen this panel at any time."
    )


@callback(
    Output('dataset-metadata-button', 'disabled'),
    [Input('dataset-metadata', 'data'),
     Input('session-id-store', 'data')],
    prevent_initial_call=False
)
def update_dataset_metadata_button_state(metadata, session_id):
    if metadata is None or session_id is None:
        return True

    return get_dataset(session_id) is None


@callback(
    Output('dataset-metadata-offcanvas', 'is_open'),
    [Input('dataset-metadata-open-request', 'data'),
     Input('dataset-metadata-button', 'n_clicks')],
    State('dataset-metadata-offcanvas', 'is_open'),
    prevent_initial_call=True
)
def toggle_dataset_metadata_offcanvas(open_request, button_clicks, is_open):
    triggered_id = dash.ctx.triggered_id

    if triggered_id == 'dataset-metadata-open-request' and open_request is not None:
        return True

    if triggered_id == 'dataset-metadata-button' and button_clicks:
        return not is_open

    return is_open

if __name__ == '__main__':
    app.run(debug=True, port=8050,dev_tools_hot_reload=True)
