"""
Overview page showing dataset statistics and file metadata
"""
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

dash.register_page(__name__, name='Overview')

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Dataset Overview", className="mt-4 mb-3"),
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Alert(
                "No dataset loaded. Please upload a database file first.",
                id='overview-no-data-alert',
                color="warning",
                is_open=True
            )
        ])
    ], id='overview-no-data-row'),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Dataset Information")),
                dbc.CardBody(id='overview-dataset-info')
            ])
        ], width=12)
    ], className="mb-3", id='overview-info-row', style={'display': 'none'}),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("File List")),
                dbc.CardBody([
                    html.Div(id='overview-file-table')
                ])
            ])
        ], width=12)
    ], className="mb-3", id='overview-files-row', style={'display': 'none'}),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Sensor List")),
                dbc.CardBody([
                    html.Div(id='overview-sensor-table')
                ])
            ])
        ], width=12)
    ], className="mb-3", id='overview-sensors-row', style={'display': 'none'}),
])


@callback(
    [Output('overview-no-data-row', 'style'),
     Output('overview-info-row', 'style'),
     Output('overview-files-row', 'style'),
     Output('overview-sensors-row', 'style'),
     Output('overview-dataset-info', 'children'),
     Output('overview-file-table', 'children'),
     Output('overview-sensor-table', 'children')],
    Input('dataset-metadata', 'data'),
    State('dataset-store', 'data'),
    prevent_initial_call=False
)
def update_overview(metadata, dataset_data):
    """Update overview page with dataset information"""
    if metadata is None or dataset_data is None:
        return (
            {'display': 'block'},  # Show no-data alert
            {'display': 'none'},   # Hide info
            {'display': 'none'},   # Hide files
            {'display': 'none'},   # Hide sensors
            "", "", ""
        )
    
    try:
        # Parse the dataframe
        df = pd.read_json(dataset_data, orient='split')
        
        # Dataset info
        info_content = [
            html.P([html.Strong("Dataset Name: "), metadata['name']]),
            html.P([html.Strong("Source File: "), metadata['filename']]),
            html.P([html.Strong("Number of Files: "), str(metadata['num_files'])]),
            html.P([html.Strong("Number of Sensors: "), str(metadata['num_sensors'])]),
            html.P([html.Strong("Number of DLCs: "), str(metadata['num_dlcs'])]),
        ]
        
        # Extract file information from MultiIndex columns
        # The dataframe has MultiIndex columns: (filelist|sensor_name, statistic_name)
        filelist_cols = [col for col in df.columns if 'filelist' in str(col)]
        
        if filelist_cols:
            # Get unique file-related columns
            file_df = df[[col for col in df.columns if col[0] == 'filelist']].copy()
            
            # Create a simple table
            file_table = dbc.Table.from_dataframe(
                file_df.head(50),  # Show first 50 files
                striped=True,
                bordered=True,
                hover=True,
                size='sm',
                responsive=True
            )
            
            if len(file_df) > 50:
                file_content = [
                    file_table,
                    html.P(f"Showing first 50 of {len(file_df)} files", className="text-muted mt-2")
                ]
            else:
                file_content = file_table
        else:
            file_content = html.P("No file metadata available", className="text-muted")
        
        # Extract sensor columns
        sensor_cols = [col for col in df.columns if col[0] != 'filelist']
        
        if sensor_cols:
            # Get unique sensor names from first level of MultiIndex
            sensor_names = list(set([col[0] for col in sensor_cols]))
            
            sensor_table = dbc.Table(
                [html.Thead(html.Tr([html.Th("Sensor Name"), html.Th("Statistics Available")]))] +
                [html.Tbody([
                    html.Tr([
                        html.Td(sensor),
                        html.Td(", ".join([col[1] for col in sensor_cols if col[0] == sensor]))
                    ]) for sensor in sorted(sensor_names)[:50]  # Show first 50 sensors
                ])],
                striped=True,
                bordered=True,
                hover=True,
                size='sm',
                responsive=True
            )
            
            if len(sensor_names) > 50:
                sensor_content = [
                    sensor_table,
                    html.P(f"Showing first 50 of {len(sensor_names)} sensors", className="text-muted mt-2")
                ]
            else:
                sensor_content = sensor_table
        else:
            sensor_content = html.P("No sensor data available", className="text-muted")
        
        return (
            {'display': 'none'},   # Hide no-data alert
            {'display': 'block'},  # Show info
            {'display': 'block'},  # Show files
            {'display': 'block'},  # Show sensors
            info_content,
            file_content,
            sensor_content
        )
        
    except Exception as e:
        return (
            {'display': 'block'},
            {'display': 'none'},
            {'display': 'none'},
            {'display': 'none'},
            "", "", ""
        )
