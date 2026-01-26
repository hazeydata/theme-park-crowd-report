"""
Theme Park Wait Time Dashboard using Dash.jl

This is a starter Dash.jl application for visualizing:
- Wait Time Index (WTI) over time
- Historical and predicted ACTUAL curves
- Live wait times
- Model performance metrics

Run with: julia --project=. src/app.jl
"""

using Dash
using DashBootstrapComponents
using DataFrames
using CSV
using Dates
using PlotlyJS
using HTTP

# Configuration: path to your output_base (where fact_tables, dimension_tables, etc. live)
# You can set this via environment variable or modify here
const OUTPUT_BASE = get(ENV, "THEME_PARK_OUTPUT_BASE", "D:\\Dropbox\\theme-park-data")

# Helper function to load WTI data (when you have it)
function load_wti_data(park::String, start_date::Date, end_date::Date)
    # TODO: Load from wti/wti.parquet or wti/park_date.csv
    # For now, return empty DataFrame with expected structure
    return DataFrame(
        park_date = Date[],
        wti = Float64[],
        n_entities = Int[]
    )
end

# Helper function to load forecast curves (when you have them)
function load_forecast_curves(entity_code::String, park_date::Date)
    # TODO: Load from curves/forecast/
    # Expected: (entity, park_date, time_slot, actual_predicted, posted_predicted)
    return DataFrame(
        time_slot = DateTime[],
        actual_predicted = Float64[],
        posted_predicted = Float64[]
    )
end

# Create the Dash app
app = dash(
    external_stylesheets=[dbc_themes.BOOTSTRAP],
    title="Theme Park Wait Time Dashboard"
)

# Define the layout
app.layout = dbc_container([
    dbc_row([
        dbc_col([
            html_h1("Theme Park Wait Time Dashboard", className="mb-4"),
            html_p("Visualize WTI, wait time curves, and forecasts", className="text-muted")
        ])
    ]),
    
    dbc_row([
        dbc_col([
            dbc_card([
                dbc_cardbody([
                    html_h4("Park Selection", className="card-title"),
                    dcc_dropdown(
                        id="park-dropdown",
                        options=[
                            Dict("label" => "Magic Kingdom", "value" => "mk"),
                            Dict("label" => "EPCOT", "value" => "ep"),
                            Dict("label" => "Hollywood Studios", "value" => "hs"),
                            Dict("label" => "Animal Kingdom", "value" => "ak"),
                        ],
                        value="mk",
                        clearable=false
                    ),
                    html_br(),
                    html_h5("Date Range", className="mt-3"),
                    dcc_datepickerrange(
                        id="date-range",
                        start_date=Date(2024, 1, 1),
                        end_date=today(),
                        display_format="YYYY-MM-DD"
                    )
                ])
            ])
        ], width=4),
        
        dbc_col([
            dbc_card([
                dbc_cardbody([
                    html_h4("Wait Time Index (WTI)", className="card-title"),
                    dcc_graph(id="wti-plot")
                ])
            ])
        ], width=8)
    ], className="mt-4"),
    
    dbc_row([
        dbc_col([
            dbc_card([
                dbc_cardbody([
                    html_h4("Attraction Selection", className="card-title"),
                    dcc_dropdown(
                        id="entity-dropdown",
                        options=[],  # Populated via callback
                        placeholder="Select an attraction..."
                    ),
                    html_br(),
                    html_h5("View Type", className="mt-3"),
                    dcc_radioitems(
                        id="view-type",
                        options=[
                            Dict("label" => "Historical ACTUAL", "value" => "historical"),
                            Dict("label" => "Forecast (Predicted)", "value" => "forecast"),
                            Dict("label" => "Both", "value" => "both")
                        ],
                        value="both"
                    )
                ])
            ])
        ], width=4),
        
        dbc_col([
            dbc_card([
                dbc_cardbody([
                    html_h4("Wait Time Curves", className="card-title"),
                    dcc_graph(id="curve-plot")
                ])
            ])
        ], width=8)
    ], className="mt-4"),
    
    dbc_row([
        dbc_col([
            html_div(id="status-text", className="text-muted mt-3")
        ])
    ])
], fluid=true)

# Callback: Update entity dropdown based on park
callback!(
    app,
    Output("entity-dropdown", "options"),
    Input("park-dropdown", "value")
) do park_code
    # TODO: Load entities from dimentity.csv filtered by park
    # For now, return empty list
    return []
end

# Callback: Update WTI plot
callback!(
    app,
    Output("wti-plot", "figure"),
    Input("park-dropdown", "value"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date")
) do park_code, start_date_str, end_date_str
    start_date = Date(start_date_str)
    end_date = Date(end_date_str)
    
    # Load WTI data
    df = load_wti_data(park_code, start_date, end_date)
    
    # Create plot
    if nrow(df) > 0
        fig = Plot(
            df,
            x=:park_date,
            y=:wti,
            kind="scatter",
            mode="lines+markers",
            Layout(
                title="Wait Time Index Over Time",
                xaxis_title="Date",
                yaxis_title="WTI (minutes)",
                hovermode="closest"
            )
        )
    else
        # Empty plot with message
        fig = Plot(
            [],
            Layout(
                title="Wait Time Index Over Time",
                xaxis_title="Date",
                yaxis_title="WTI (minutes)",
                annotations=[
                    attr(
                        text="No WTI data available yet. Run the forecast/WTI pipeline first.",
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                        showarrow=false
                    )
                ]
            )
        )
    end
    
    return fig
end

# Callback: Update wait time curve plot
callback!(
    app,
    Output("curve-plot", "figure"),
    Input("entity-dropdown", "value"),
    Input("park-dropdown", "value"),
    Input("date-range", "start_date"),
    Input("view-type", "value")
) do entity_code, park_code, date_str, view_type
    if isnothing(entity_code) || isempty(entity_code)
        return Plot(
            [],
            Layout(
                title="Wait Time Curves",
                xaxis_title="Time of Day",
                yaxis_title="Wait Time (minutes)",
                annotations=[
                    attr(
                        text="Select an attraction to view curves",
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                        showarrow=false
                    )
                ]
            )
        )
    end
    
    park_date = Date(date_str)
    
    # Load forecast curves
    df_forecast = load_forecast_curves(entity_code, park_date)
    
    # TODO: Load historical curves if view_type includes "historical" or "both"
    
    # Create traces
    traces = []
    
    if view_type in ["forecast", "both"] && nrow(df_forecast) > 0
        push!(traces, scatter(
            x=df_forecast.time_slot,
            y=df_forecast.actual_predicted,
            mode="lines",
            name="Predicted ACTUAL",
            line=attr(color="blue", width=2)
        ))
        push!(traces, scatter(
            x=df_forecast.time_slot,
            y=df_forecast.posted_predicted,
            mode="lines",
            name="Predicted POSTED",
            line=attr(color="orange", width=2, dash="dash")
        ))
    end
    
    # TODO: Add historical traces if available
    
    fig = Plot(
        traces,
        Layout(
            title="Wait Time Curves: $(entity_code)",
            xaxis_title="Time of Day",
            yaxis_title="Wait Time (minutes)",
            hovermode="closest",
            legend=attr(x=0.02, y=0.98)
        )
    )
    
    return fig
end

# Callback: Update status text
callback!(
    app,
    Output("status-text", "children"),
    Input("park-dropdown", "value"),
    Input("entity-dropdown", "value")
) do park_code, entity_code
    status = "Park: $(uppercase(park_code))"
    if !isnothing(entity_code) && !isempty(entity_code)
        status *= " | Entity: $(entity_code)"
    end
    return html_p(status)
end

# Run the app
if abspath(PROGRAM_FILE) == @__FILE__
    println("Starting Dash app on http://127.0.0.1:8050")
    run_server(app, "127.0.0.1", 8050, debug=true)
end
