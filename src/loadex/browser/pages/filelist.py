"""
File list page for displaying all dataset.filelist entries in a Dash AG Grid table.
"""
from pathlib import Path

import dash
from dash import callback, html, Input, Output, State
import dash_ag_grid as dag
import dash_bootstrap_components as dbc

from loadex.browser.session_cache import get_dataset


dash.register_page(__name__, path='/filelist', name='File List')


layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("File List", className="mt-4 mb-3"),
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Alert(
                "No dataset loaded. Please upload a database file first.",
                id='filelist-no-data-alert',
                color="warning",
                is_open=True
            )
        ])
    ], id='filelist-no-data-row'),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Dataset File List")),
                dbc.CardBody([
                    dag.AgGrid(
                        id='filelist-grid',
                        columnDefs=[],
                        rowData=[],
                        defaultColDef={
                            'sortable': True,
                            'filter': True,
                            'resizable': True,
                            'floatingFilter': True,
                        },
                        dashGridOptions={
                            'pagination': True,
                            'paginationPageSize': 50,
                            'animateRows': False,
                            'tooltipShowDelay': 0,
                        },
                        style={'height': '70vh', 'width': '100%'}
                    )
                ])
            ])
        ])
    ], id='filelist-grid-row', style={'display': 'none'}),
])


@callback(
    [Output('filelist-no-data-row', 'style'),
     Output('filelist-grid-row', 'style'),
     Output('filelist-grid', 'columnDefs'),
     Output('filelist-grid', 'rowData')],
    Input('dataset-metadata', 'data'),
    State('session-id-store', 'data'),
    prevent_initial_call=False
)
def update_filelist_page(metadata, session_id):
    """Render dataset.filelist as AG Grid rows/columns."""
    if metadata is None or session_id is None:
        return {'display': 'block'}, {'display': 'none'}, [], []

    dataset = get_dataset(session_id)
    if dataset is None:
        return {'display': 'block'}, {'display': 'none'}, [], []

    file_df = dataset.filelist.to_dataframe().reset_index()

    file_df.insert(0, 'filename', file_df['filepath'].apply(lambda value: Path(str(value)).name))

    # Convert NaN/NaT to None for JSON serialization in rowData
    file_df = file_df.where(file_df.notna(), None)
    safe_columns = {col: col.replace(".", "_") for col in file_df.columns}
    file_df = file_df.rename(columns=safe_columns)

    row_data = file_df.to_dict('records')
    column_defs = [
        {
            'headerName': 'Filename',
            'field': 'filename',
            'pinned': 'left',
            'tooltipField': 'filepath',
            'minWidth': 220,
        }
    ]

    for original_name, safe_column in safe_columns.items():
        if safe_column == 'filename':
            continue
        column_defs.append(
            {
                'headerName': str(original_name),
                'field': str(safe_column),
                'tooltipField': str(original_name),
            }
        )

    return {'display': 'none'}, {'display': 'block'}, column_defs, row_data
