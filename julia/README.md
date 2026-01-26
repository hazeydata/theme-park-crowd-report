# Julia Dashboards for Theme Park Data

This directory contains Julia code for building interactive dashboards using [Dash.jl](https://dash.plotly.com/julia) to visualize wait time data, WTI, forecasts, and model performance.

## Why Julia + Dash.jl?

- **Performance**: Julia's speed is excellent for data processing and real-time updates
- **Native**: Pure Julia implementation (no Python bridge needed)
- **Interactive**: Reactive callbacks update plots instantly as users interact
- **Beautiful**: Bootstrap components for professional-looking dashboards
- **Fast development**: Similar API to Python Dash, but in Julia

## Setup

### 1. Install Julia

Download Julia from [julialang.org](https://julialang.org/downloads/) (version 1.9+ recommended).

### 2. Install Dependencies

From this directory, activate the project and install packages:

```julia
# In Julia REPL:
using Pkg
Pkg.activate(".")
Pkg.instantiate()
```

Or from command line:

```powershell
julia --project=. -e "using Pkg; Pkg.instantiate()"
```

### 3. Configure Output Base

Set the path to your data directory (where `fact_tables/`, `dimension_tables/`, `wti/`, `curves/` live):

**Option A: Environment variable (recommended)**
```powershell
$env:THEME_PARK_OUTPUT_BASE = "D:\Dropbox\theme-park-data"
```

**Option B: Edit `src/app.jl`**
Change the `OUTPUT_BASE` constant at the top of the file.

## Running the Dashboard

### Development Mode

```powershell
julia --project=. src/app.jl
```

Or in Julia REPL:

```julia
using Pkg
Pkg.activate(".")
include("src/app.jl")
```

The dashboard will be available at: **http://127.0.0.1:8050**

### Production Mode

For production, you might want to:
- Use a production WSGI server (e.g., with `Dash.jl`'s built-in server or behind nginx)
- Set `debug=false` in `run_server()`
- Configure authentication/authorization
- Use environment variables for configuration

## Current Features

The starter app (`src/app.jl`) includes:

1. **Park Selection** - Choose which park to view (MK, EP, HS, AK)
2. **Date Range Picker** - Select date range for historical data
3. **WTI Plot** - Wait Time Index over time (when WTI data is available)
4. **Attraction Selection** - Dropdown to pick specific attractions
5. **Wait Time Curves** - Historical and predicted ACTUAL/POSTED curves

## TODO: Data Loading Functions

The app currently has placeholder functions that need to be implemented:

- `load_wti_data()` - Load from `wti/wti.parquet` or `wti/park_date.csv`
- `load_forecast_curves()` - Load from `curves/forecast/`
- `load_historical_curves()` - Load from `curves/backfill/`
- Entity list loading from `dimension_tables/dimentity.csv`

Once you have WTI and curve data from your pipeline, implement these functions to read from your output_base.

## Example: Loading WTI Data

```julia
using Parquet2  # or Arrow.jl, CSV.jl depending on format

function load_wti_data(park::String, start_date::Date, end_date::Date)
    wti_path = joinpath(OUTPUT_BASE, "wti", "wti.parquet")
    
    if isfile(wti_path)
        df = DataFrame(Parquet2.Dataset(wti_path))
        # Filter by park and date range
        return filter(row -> 
            row.park == park && 
            start_date <= row.park_date <= end_date,
            df
        )
    else
        return DataFrame(park_date=Date[], wti=Float64[], n_entities=Int[])
    end
end
```

## Adding New Dashboards

Create new `.jl` files in `src/` for different dashboard views:

- `wti_dashboard.jl` - Focused WTI analysis
- `forecast_dashboard.jl` - Forecast vs observed comparisons
- `model_performance.jl` - Model metrics, residuals, SHAP plots
- `live_monitoring.jl` - Real-time wait times from staging

Each can be a separate Dash app or combined into a multi-page app using `dcc_location` and callbacks.

## Resources

- [Dash.jl Documentation](https://dash.plotly.com/julia)
- [Dash Bootstrap Components](https://dash-bootstrap-components.opensource.faculty.ai/)
- [PlotlyJS.jl](https://github.com/JuliaPlots/PlotlyJS.jl) - For advanced plotting
- [DataFrames.jl](https://dataframes.juliadata.org/) - For data manipulation

## Performance Tips

1. **Caching**: Use `@memoize` or store precomputed aggregations
2. **Lazy Loading**: Load data only when needed (via callbacks)
3. **Partitioned Reads**: Read only the date range/entities requested
4. **Background Updates**: Use `dcc_interval` for periodic data refreshes

## Next Steps

1. Implement data loading functions to connect to your pipeline output
2. Add more visualizations (heatmaps, comparison charts, etc.)
3. Add model performance metrics when you have trained models
4. Create a live monitoring dashboard for queue-times staging data
5. Add export functionality (download plots, CSV exports)
