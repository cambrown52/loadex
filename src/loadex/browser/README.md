# Loadex Browser

Multi-page Dash application for visualizing wind turbine loads data from loadex datasets.

## Installation

First, make sure you have the updated dependencies installed:

```powershell
pip install -e .
```

This will install the required Dash dependencies (`dash` and `dash-bootstrap-components`).

## Running the Application

Navigate to the browser directory and run:

```powershell
cd src\loadex\browser
python app.py
```

The application will start on `http://localhost:8050`

## Features

### 1. Upload Database Page (/)
- Drag and drop or select a SQLite database file (.db)
- Loads the dataset using `DataSet.from_sql()`
- Displays dataset summary information
- Stores dataset in browser session for use across pages

### 2. Overview Page (/overview)
- Shows dataset metadata (name, number of files, sensors, DLCs)
- Lists all files in the dataset
- Lists all sensors with available statistics
- Provides summary tables

### 3. Sensors Page (/sensors)
- Interactive sensor visualization
- Select sensor and statistic from dropdowns
- Multiple plot types: Bar Chart, Box Plot, Scatter Plot
- Statistics summary table with descriptive statistics

### 4. DLC Comparison Page (/dlc-comparison)
- Compare sensor statistics across Design Load Cases
- Group by DLC or custom groups
- Box plots with all data points visible
- Interactive hover information

## Application Structure

```
src/loadex/browser/
├── app.py                      # Main application entry point
├── pages/
│   ├── upload.py               # Database upload page
│   ├── overview.py             # Dataset overview page
│   ├── sensors.py              # Sensor visualization page
│   └── dlc_comparison.py       # DLC comparison page
└── README.md                   # This file
```

## Data Flow

1. User uploads a SQLite database file on the Upload page
2. Dataset is loaded using `DataSet.from_sql()`
4. DataFrame is serialized to JSON and stored in a Dash Store (session storage)
5. All other pages read from the stored dataset to create visualizations

## Notes

- Dataset is stored as JSON in browser session storage (session-scoped)
- MultiIndex columns from the dataset are preserved: `(sensor_name|filelist, statistic_name)`
- The application uses Bootstrap styling via `dash-bootstrap-components`
- All visualizations use Plotly for interactive charts
