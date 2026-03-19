"""
Upload page for loading SQLite database files containing loadex datasets
"""
import dash
from dash import html, dcc, callback, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import base64
import tempfile
import os
from uuid import uuid4
from pathlib import Path

from loadex.classes.dataset import DataSet
from loadex.browser.session_cache import set_dataset, cleanup_expired

dash.register_page(__name__, path='/', name='Upload Database')


layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Upload Loadex Database", className="mt-4 mb-3"),
            html.P("Drag and drop a SQLite database file (.db) to load a loadex dataset.", 
                   className="text-muted"),
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dcc.Upload(
                id='upload-database',
                children=html.Div([
                    html.I(className="bi bi-cloud-upload", style={'fontSize': '48px'}),
                    html.Br(),
                    html.Br(),
                    'Drag and Drop or ',
                    html.A('Select Database File', style={'textDecoration': 'underline', 'cursor': 'pointer'})
                ]),
                style={
                    'width': '100%',
                    'height': '200px',
                    'lineHeight': '200px',
                    'borderWidth': '2px',
                    'borderStyle': 'dashed',
                    'borderRadius': '10px',
                    'textAlign': 'center',
                    'backgroundColor': '#f8f9fa'
                },
                multiple=False,
                accept='.db,.sqlite,.sqlite3'
            ),
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Alert(id='upload-status', is_open=False, dismissable=True, duration=4000),
        ])
    ]),
])


@callback(
    [Output('session-id-store', 'data'),
     Output('dataset-metadata', 'data'),
     Output('dataset-metadata-open-request', 'data'),
     Output('upload-status', 'children'),
     Output('upload-status', 'color'),
     Output('upload-status', 'is_open')],
    Input('upload-database', 'contents'),
    [State('upload-database', 'filename'),
     State('session-id-store', 'data')],
    prevent_initial_call=True
)
def load_database(contents, filename, session_id):
    """Load dataset from uploaded database file"""
    if contents is None:
        return session_id, no_update, no_update, "", "info", False
    
    try:
        cleanup_expired(max_age_seconds=3600)

        if session_id is None:
            session_id = str(uuid4())

        # Decode the uploaded file
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp_file:
            tmp_file.write(decoded)
            tmp_path = tmp_file.name
        
        # Load dataset using loadex
        dataset = DataSet.from_sql(tmp_path, name=Path(filename).stem)

        # Store dataset server-side in memory
        set_dataset(session_id, dataset)
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        # Store metadata
        metadata = {
            'name': dataset.name,
            'num_files': len(dataset.filelist),
            'num_sensors': len(dataset.sensorlist),
            'num_dlcs': len(dataset.dlcs),
            'filename': filename
        }
        return (
            session_id,
            metadata,
            str(uuid4()),
            f"Successfully loaded dataset '{dataset.name}' with {len(dataset.filelist)} files. Dataset metadata is available from the navigation bar.",
            "success",
            True,
        )
        
    except Exception as e:
        return (
            session_id,
            no_update,
            no_update,
            f"Error loading database: {str(e)}",
            "danger",
            True,
        )
