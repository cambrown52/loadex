"""
Upload page for loading SQLite database files containing loadex datasets
"""
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import base64
import io
import tempfile
import os
from pathlib import Path

from loadex.classes.dataset import DataSet

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
    
    dbc.Row([
        dbc.Col([
            html.Div(id='dataset-info')
        ])
    ])
])


@callback(
    [Output('dataset-store', 'data'),
     Output('dataset-metadata', 'data'),
     Output('upload-status', 'children'),
     Output('upload-status', 'color'),
     Output('upload-status', 'is_open'),
     Output('dataset-info', 'children')],
    Input('upload-database', 'contents'),
    State('upload-database', 'filename'),
    prevent_initial_call=True
)
def load_database(contents, filename):
    """Load dataset from uploaded database file"""
    if contents is None:
        return None, None, "", "info", False, ""
    
    try:
        # Decode the uploaded file
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp_file:
            tmp_file.write(decoded)
            tmp_path = tmp_file.name
        
        # Load dataset using loadex
        dataset = DataSet.from_sql(tmp_path, name=Path(filename).stem)
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        # Convert dataset to JSON-serializable format
        # Store the dataframe as JSON
        df = dataset.to_dataframe()
        dataset_data = df.to_json(date_format='iso', orient='split')
        
        # Store metadata
        metadata = {
            'name': dataset.name,
            'num_files': len(dataset.filelist),
            'num_sensors': len(dataset.sensorlist),
            'num_dlcs': len(dataset.dlcs),
            'filename': filename
        }
        
        # Create info card
        info_card = dbc.Card([
            dbc.CardHeader(html.H4("Dataset Loaded Successfully")),
            dbc.CardBody([
                html.H5(f"Dataset: {dataset.name}", className="card-title"),
                html.Hr(),
                dbc.Row([
                    dbc.Col([
                        html.P([html.Strong("Files: "), str(len(dataset.filelist))]),
                        html.P([html.Strong("Sensors: "), str(len(dataset.sensorlist))]),
                    ], width=6),
                    dbc.Col([
                        html.P([html.Strong("DLCs: "), str(len(dataset.dlcs))]),
                        html.P([html.Strong("Source: "), filename]),
                    ], width=6),
                ]),
                html.Hr(),
                html.P("You can now navigate to other pages to visualize the data.", 
                       className="text-muted"),
                dbc.Button("View Overview", href="/overview", color="primary", className="mt-2")
            ])
        ], className="mt-3")
        
        return (dataset_data, metadata, 
                f"Successfully loaded dataset '{dataset.name}' with {len(dataset.filelist)} files",
                "success", True, info_card)
        
    except Exception as e:
        return (None, None, 
                f"Error loading database: {str(e)}", 
                "danger", True, "")
