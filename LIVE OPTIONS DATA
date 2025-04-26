import os
import glob
import re
import io
import pandas as pd
import dash
from dash import dash_table, html, dcc, callback_context, no_update
from dash.dependencies import Input, Output, State
import flask, warnings

warnings.simplefilter(action='ignore', category=DeprecationWarning)

# Define the folder path where order book CSV files are stored
folder_path = r"Z:\\API_data_backup\\data\\orderbook\\Open_Position"

# Path to the LTP Excel file
LTP_file = r"C:\API_Trade\data\RTD\LTP.xlsm"

# Initialize the Dash app
app = dash.Dash(__name__)
server = app.server

def load_orderbooks(filter_type, scenario_num_filter="ALL", scenario_letter_filter="ALL"):
    """
    Load and process order book CSV files and merge with LTP Excel data.
    
    For MAIN orders, rows with zero net quantity are removed.
    AVG_PRICE is computed as a weighted average using abs(NetQty):
    
         AVG_PRICE = sum(abs(NetQty) * OrderAverageTradedPrice) / sum(abs(NetQty))
    
    For MAIN orders, two extra columns are added:
      - PAIR_SL: For paired option types ('CE' and 'PE')
      - STRIKE_SL: For a single option type, set equal to the weighted AVG_PRICE.
    
    DECAY% is calculated as follows:
         If a pair exists:
            DECAY% = ((LTP_CE + LTP_PE) - (AVG_PRICE_CE + AVG_PRICE_PE)) / (AVG_PRICE_CE + AVG_PRICE_PE) * 100
         Else:
            DECAY% = ((LTP - weighted_avg) / weighted_avg) * 100
    """
    # ---------------------------
    # Step 1. Read and concatenate CSV files
    # ---------------------------
    files = glob.glob(os.path.join(folder_path, "*.csv"))
    df_list = []
    for file in files:
        if os.path.getsize(file) > 0:
            temp_df = pd.read_csv(file)
            if not temp_df.empty and not temp_df.dropna(how="all").empty:
                df_list.append(temp_df)
    df_list = [df for df in df_list if not df.empty and not df.dropna(how="all").empty]
    if not df_list:
        return pd.DataFrame(columns=[
            "INDEX", "STRIKE", "OPTION TYPE", "NetQty", "STRIKE_TYPE",
            "OrderStatus", "Position", "AVG_PRICE", "LTP", "PAIR_SL", "STRIKE_SL",
            "DECAY%", "ScenarioNumber", "ScenarioLetter", "OrderUniqueIdentifier"
        ])
    df = pd.concat(df_list, ignore_index=True)
    
    # ---------------------------
    # Step 2. Parse OrderUniqueIdentifier and filter for rows containing "scenario"
    # ---------------------------
    df["ScenarioNumber"] = df["OrderUniqueIdentifier"].str.extract(r"scenario_super_(\d+)_", expand=False)
    df["ScenarioLetter"] = df["OrderUniqueIdentifier"].str.extract(r"scenario_super_\d+_([A-Za-z])", expand=False)
    df["ScenarioNumber"] = pd.to_numeric(df["ScenarioNumber"], errors="coerce")
    df = df.dropna(subset=["OrderUniqueIdentifier"])
    df = df[df["OrderUniqueIdentifier"].str.startswith("scenario")]
    
    # ---------------------------
    # Step 3. Filter only "Filled" orders
    # ---------------------------
    if "OrderStatus" in df.columns:
        df = df[df["OrderStatus"].str.strip() == "Filled"]
    else:
        return pd.DataFrame(columns=[
            "INDEX", "STRIKE", "OPTION TYPE", "NetQty", "STRIKE_TYPE",
            "OrderStatus", "Position", "AVG_PRICE", "LTP", "PAIR_SL", "STRIKE_SL",
            "DECAY%", "ScenarioNumber", "ScenarioLetter", "OrderUniqueIdentifier"
        ])
    
    # ---------------------------
    # Step 4. Ensure existence of "Main/Hedge" and create standardized "Filtered_Type"
    # ---------------------------
    if "Main/Hedge" not in df.columns:
        df["Main/Hedge"] = "MAIN"
    df["Filtered_Type"] = df["Main/Hedge"].apply(lambda x: "MAIN" if "MAIN" in str(x).upper() else "HEDGE")
    
    # ---------------------------
    # Step 5. Filter by MAIN/HEDGE dropdown if not "ALL"
    # ---------------------------
    if filter_type != "ALL":
        df = df[df["Filtered_Type"] == filter_type]
    
    # ---------------------------
    # Step 6. Filter by scenario number and scenario letter if specified
    # ---------------------------
    if scenario_num_filter != "ALL" and scenario_num_filter is not None:
        df = df[df["ScenarioNumber"] == int(scenario_num_filter)]
    if scenario_letter_filter != "ALL" and scenario_letter_filter is not None:
        df = df[df["ScenarioLetter"] == scenario_letter_filter]
    
    # ---------------------------
    # Step 7. Read and merge LTP Excel file
    # ---------------------------
    try:
        LTP_df = pd.read_excel(LTP_file, skiprows=4, header=0, engine="openpyxl")
        LTP_df.columns = LTP_df.columns.astype(str).str.strip()
        LTP_df.rename(columns={"LTP": "LTP"}, inplace=True)
    except Exception as e:
        print(f"Error reading LTP file: {e}")
        LTP_df = pd.DataFrame(columns=["INDEX", "STRIKE", "OPTION TYPE", "LTP"])
    if not LTP_df.empty:
        merge_cols = ["SYMBOL", "STRIKE", "OPTION TYPE", "LTP"]
        LTP_df = LTP_df[merge_cols].drop_duplicates()
    
    # ---------------------------
    # Determine view mode: Aggregated vs. Detailed
    # ---------------------------
    hide_oui = (scenario_num_filter == "ALL") and (scenario_letter_filter == "ALL")
    
    if hide_oui:
        # ---------------------------
        # AGGREGATED VIEW
        # ---------------------------
        agg_cols = ["SYMBOL", "STRIKE", "OPTION TYPE", "Filtered_Type"]
        net_qty_df = df.groupby(agg_cols, as_index=False)["NetQty"].sum()
        net_qty_df = net_qty_df[net_qty_df["NetQty"] != 0]
        net_qty_df["Position"] = net_qty_df["NetQty"].apply(lambda x: "Long" if x > 0 else "Short")
        
        df['WeightedPrice'] = df['NetQty'].abs() * df['OrderAverageTradedPrice']
        df['AbsQty'] = df['NetQty'].abs()
        avg_price_df = df.groupby(agg_cols).agg({
            'WeightedPrice': 'sum',
            'AbsQty': 'sum'
        }).reset_index()
        avg_price_df['AVG_PRICE'] = (avg_price_df['WeightedPrice'] / avg_price_df['AbsQty']).round(2)
        
        net_qty_df = net_qty_df.merge(avg_price_df[agg_cols + ['AVG_PRICE']], on=agg_cols, how='left')
        if not LTP_df.empty:
            net_qty_df = net_qty_df.merge(LTP_df, how="left", on=["SYMBOL", "STRIKE", "OPTION TYPE"])
        else:
            net_qty_df["LTP"] = None

        if filter_type.upper() == "MAIN":
            net_qty_df["PAIR_SL"] = ""
            net_qty_df["STRIKE_SL"] = ""
            net_qty_df["DECAY%"] = ""
            
            def compute_sl(group):
                group = group.copy()
                total_qty = group["NetQty"].abs().sum()
                weighted_price = (group["NetQty"].abs() * group["AVG_PRICE"]).sum()
                weighted_avg = weighted_price / total_qty if total_qty != 0 else 0
                option_types = group["OPTION TYPE"].unique()
                if set(["CE", "PE"]).issubset(set(option_types)) and len(group) >= 2:
                    ce_row = group.loc[group["OPTION TYPE"]=="CE"].iloc[0]
                    pe_row = group.loc[group["OPTION TYPE"]=="PE"].iloc[0]
                    pair_sl = (ce_row["AVG_PRICE"] + pe_row["AVG_PRICE"]) * 1.05
                    combined_LTP = ce_row["LTP"] + pe_row["LTP"]
                    combined_AVG_PRICE = ce_row["AVG_PRICE"] + pe_row["AVG_PRICE"]
                    decay = ((combined_LTP - combined_AVG_PRICE) / combined_AVG_PRICE * 100) if combined_AVG_PRICE != 0 else None
                    group.iloc[0, group.columns.get_loc("PAIR_SL")] = round(pair_sl, 2)
                    group.iloc[0, group.columns.get_loc("DECAY%")] = round(decay, 2) if decay is not None else None
                    group.iloc[0, group.columns.get_loc("STRIKE_SL")] = ""
                    # Clear subsequent rows in the group
                    for i in range(1, len(group)):
                        group.iloc[i, group.columns.get_loc("PAIR_SL")] = ""
                        group.iloc[i, group.columns.get_loc("DECAY%")] = ""
                        group.iloc[i, group.columns.get_loc("STRIKE_SL")] = ""
                else:
                    group["PAIR_SL"] = ""
                    group["STRIKE_SL"] = weighted_avg
                    group["DECAY%"] = ((group["LTP"] - weighted_avg) / weighted_avg * 100).round(2)
                return group

            net_qty_df = net_qty_df.groupby(["SYMBOL", "STRIKE"], group_keys=False).apply(compute_sl)
        
        net_qty_df = net_qty_df.sort_values(["SYMBOL", "STRIKE", "OPTION TYPE"])
        net_qty_df.rename(columns={"SYMBOL": "INDEX", "Filtered_Type": "STRIKE_TYPE"}, inplace=True)
        
        return net_qty_df
    
    else:
        # ---------------------------
        # DETAILED VIEW
        # ---------------------------
        agg_cols = ["SYMBOL", "STRIKE", "OPTION TYPE", "Filtered_Type", "OrderUniqueIdentifier"]
        df['WeightedPrice'] = df['NetQty'].abs() * df['OrderAverageTradedPrice']
        df['AbsQty'] = df['NetQty'].abs()
        detailed_df = df.groupby(agg_cols, as_index=False).agg({
            "NetQty": "sum",
            "WeightedPrice": "sum",
            "AbsQty": "sum"
        })
        detailed_df = detailed_df[detailed_df["NetQty"] != 0]
        detailed_df["AVG_PRICE"] = (detailed_df["WeightedPrice"] / detailed_df["AbsQty"]).round(2)
        detailed_df["Position"] = detailed_df["NetQty"].apply(lambda x: "Long" if x > 0 else "Short")
        detailed_df.drop(columns=["WeightedPrice", "AbsQty"], inplace=True)
        if not LTP_df.empty:
            detailed_df = detailed_df.merge(LTP_df, how="left", on=["SYMBOL", "STRIKE", "OPTION TYPE"])
        else:
            detailed_df["LTP"] = None

        if filter_type.upper() == "MAIN":
            detailed_df["PAIR_SL"] = ""
            detailed_df["STRIKE_SL"] = ""
            detailed_df["DECAY%"] = ""
            
            def compute_sl_detail(group):
                group = group.copy()
                total_qty = group["NetQty"].abs().sum()
                weighted_price = (group["NetQty"].abs() * group["AVG_PRICE"]).sum()
                weighted_avg = weighted_price / total_qty if total_qty != 0 else 0
                option_types = group["OPTION TYPE"].unique()
                if set(["CE", "PE"]).issubset(set(option_types)) and len(group) >= 2:
                    ce_row = group.loc[group["OPTION TYPE"]=="CE"].iloc[0]
                    pe_row = group.loc[group["OPTION TYPE"]=="PE"].iloc[0]
                    pair_sl = (ce_row["AVG_PRICE"] + pe_row["AVG_PRICE"]) * 1.05
                    combined_LTP = ce_row["LTP"] + pe_row["LTP"]
                    combined_AVG_PRICE = ce_row["AVG_PRICE"] + pe_row["AVG_PRICE"]
                    decay = ((combined_LTP - combined_AVG_PRICE) / combined_AVG_PRICE * 100) if combined_AVG_PRICE != 0 else None
                    group.iloc[0, group.columns.get_loc("PAIR_SL")] = round(pair_sl, 2)
                    group.iloc[0, group.columns.get_loc("DECAY%")] = round(decay, 2) if decay is not None else None
                    group.iloc[0, group.columns.get_loc("STRIKE_SL")] = ""
                    for i in range(1, len(group)):
                        group.iloc[i, group.columns.get_loc("PAIR_SL")] = ""
                        group.iloc[i, group.columns.get_loc("DECAY%")] = ""
                        group.iloc[i, group.columns.get_loc("STRIKE_SL")] = ""
                else:
                    group["PAIR_SL"] = ""
                    group["STRIKE_SL"] = weighted_avg
                    group["DECAY%"] = ((group["LTP"] - weighted_avg) / weighted_avg * 100).round(2)
                return group

            detailed_df = detailed_df.groupby(["SYMBOL", "STRIKE", "OrderUniqueIdentifier"], group_keys=False).apply(compute_sl_detail)
        
        detailed_df = detailed_df.sort_values("OrderUniqueIdentifier")
        detailed_df.rename(columns={"SYMBOL": "INDEX", "Filtered_Type": "STRIKE_TYPE"}, inplace=True)
        
        return detailed_df

# ---------------------------
# Dash Layout and Callbacks
# ---------------------------

css_style = """
html, body, * {
    filter: none !important;
}
body {
    background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
    color: #f1f1f1;
    font-family: 'Segoe UI', sans-serif;
    margin: 0;
    padding: 0;
}
h1 {
    font-size: 3.5em;
    text-align: center;
    margin-top: 20px;
    color: #f39c12;
    text-shadow: 2px 2px 10px #000;
}
h3 {
    font-size: 1.8em;
    margin-bottom: 15px;
    color: #ecf0f1;
}
.dropdown-container {
    width: 40%;
    margin: 20px auto;
}
.tables-wrapper {
    display: flex;
    flex-wrap: wrap;
    justify-content: center;
    padding: 10px;
}
.table-container {
    margin: 15px;
    padding: 20px;
    border-radius: 15px;
    background: rgba(255, 255, 255, 0.1);
    box-shadow: 0 10px 20px rgba(0,0,0,0.3);
    transition: none;
    perspective: 1000px;
    min-width: 320px;
    max-width: 100%;
    border: 3px solid transparent;
}
.table-container:hover {
    /* No transform or shadow on hover */
}
.dash-table-container {
    background: transparent;
}
.dash-cell {
    font-size: 1.3em;
    font-weight: bold;
}
.dash-cell.column-header--sorted--asc, .dash-cell.column-header--sorted--desc {
    background-color: #34495e !important;
    color: #f1c40f !important;
}
"""

app.index_string = f"""
<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <style>{css_style}</style>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>
"""

app.layout = html.Div(
    children=[
        html.H1("Live Options Data", style={'textAlign': 'center'}),
        # Dropdown: MAIN / HEDGE
        html.Div(
            [
                html.Label("MAIN/HEDGE"),
                dcc.Dropdown(
                    id="type_filter",
                    options=[
                        {"label": "All", "value": "ALL"},
                        {"label": "Main", "value": "MAIN"},
                        {"label": "Hedge", "value": "HEDGE"},
                    ],
                    value="ALL",
                    clearable=False,
                    style={"backgroundColor": "#bdc3c7", "color": "#2c3e50", "fontSize": "1.2em", "width": "150px"},
                ),
            ],
            className="dropdown-container",
            style={"display": "inline-block", "margin": "0 20px"},
        ),
        # Dropdown: Scenario Number
        html.Div(
            [
                html.Label("ENTRY"),
                dcc.Dropdown(
                    id="scenario_number_filter",
                    value="ALL",
                    clearable=False,
                    style={"backgroundColor": "#bdc3c7", "color": "#2c3e50", "fontSize": "1.2em", "width": "100px"},
                ),
            ],
            style={"display": "inline-block", "margin": "0 20px"},
        ),
        # Dropdown: Scenario Letter
        html.Div(
            [
                html.Label("DELAYS"),
                dcc.Dropdown(
                    id="scenario_letter_filter",
                    value="ALL",
                    clearable=False,
                    style={"backgroundColor": "#bdc3c7", "color": "#2c3e50", "fontSize": "1.2em", "width": "100px"},
                ),
            ],
            style={"display": "inline-block", "margin": "0 20px"},
        ),
        # Export to CSV button
        html.Div(
            [
                html.Button("Export to CSV", id="export_button", style={"fontSize": "1.2em", "padding": "10px 20px"})
            ],
            style={"textAlign": "center", "margin": "20px"}
        ),
        # Download component for CSV
        dcc.Download(id="download-dataframe-csv"),
        # Refresh every 10 seconds
        dcc.Interval(id="interval", interval=10000, n_intervals=0),
        html.Div(id="tables_container", className="tables-wrapper"),
    ],
)

@app.callback(
    [
        Output("scenario_number_filter", "options"),
        Output("scenario_letter_filter", "options"),
    ],
    [Input("interval", "n_intervals")]
)
def populate_scenario_dropdowns(n):
    files = glob.glob(os.path.join(folder_path, "*.csv"))
    df_list = []
    for file in files:
        if os.path.getsize(file) > 0:
            temp_df = pd.read_csv(file)
            if not temp_df.empty and not temp_df.dropna(how="all").empty:
                df_list.append(temp_df)
    df_list = [df for df in df_list if not df.empty and not df.dropna(how="all").empty]
    if not df_list:
        return [[{"label": "All", "value": "ALL"}], [{"label": "All", "value": "ALL"}]]
    df = pd.concat(df_list, ignore_index=True)
    df["ScenarioNumber"] = df["OrderUniqueIdentifier"].str.extract(r"scenario_super_(\d+)_", expand=False)
    df["ScenarioLetter"] = df["OrderUniqueIdentifier"].str.extract(r"scenario_super_\d+_([A-Za-z])", expand=False)
    df["ScenarioNumber"] = pd.to_numeric(df["ScenarioNumber"], errors="coerce")
    unique_nums = sorted(df["ScenarioNumber"].dropna().unique())
    unique_letters = sorted(df["ScenarioLetter"].dropna().unique())
    num_options = [{"label": "All", "value": "ALL"}] + [{"label": str(num), "value": str(int(num))} for num in unique_nums]
    letter_options = [{"label": "All", "value": "ALL"}] + [{"label": letter, "value": letter} for letter in unique_letters]
    return num_options, letter_options

@app.callback(
    Output("tables_container", "children"),
    [
        Input("interval", "n_intervals"),
        Input("type_filter", "value"),
        Input("scenario_number_filter", "value"),
        Input("scenario_letter_filter", "value"),
    ]
)
def update_dashboard(n, filter_type, scenario_num_filter, scenario_letter_filter):
    df = load_orderbooks(filter_type, scenario_num_filter, scenario_letter_filter)
    if df.empty:
        return html.H3("No data available", style={"textAlign": "center"})

    # Round numeric columns
    for col in ["AVG_PRICE", "LTP", "PAIR_SL", "STRIKE_SL", "DECAY%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
    
    # Create base styling for each column from a color mapping
    color_mapping = {
        0: {"text": "#e74c3c", "bg": "#fdecea"},
        1: {"text": "#3498db", "bg": "#eaf4fc"},
        2: {"text": "#2ecc71", "bg": "#e8f8f2"},
        3: {"text": "#f1c40f", "bg": "#fff9e6"},
        4: {"text": "#9b59b6", "bg": "#f4e6f7"},
    }
    col_styles = []
    columns = [{"name": col, "id": col} for col in df.columns]
    for i, col_dict in enumerate(columns):
        if i in color_mapping:
            text_color = color_mapping[i]["text"]
            bg_color = color_mapping[i]["bg"]
        else:
            text_color = "#34495e"
            bg_color = "#ecf0f1"
        col_styles.append({
            "if": {"column_id": col_dict["id"]},
            "color": text_color,
            "backgroundColor": bg_color,
            "fontWeight": "bold",
        })
    
    # Additional DECAY% styling for MAIN orders
    additional_styles = []
    if filter_type.upper() == "MAIN":
        additional_styles = [
            # For Short positions: negative DECAY% → green, >= 0 → red
            {'if': {'filter_query': '{Position} = "Short" && {DECAY%} < 0', 'column_id': 'DECAY%'}, 'color': '#006400', 'backgroundColor': '#90EE90'},
            {'if': {'filter_query': '{Position} = "Short" && {DECAY%} > 0', 'column_id': 'DECAY%'}, 'color': '#8B0000', 'backgroundColor': '#FFE5E5'},
            # For Long positions: negative DECAY% → red, >= 0 → green
            {'if': {'filter_query': '{Position} = "Long" && {DECAY%} < 0', 'column_id': 'DECAY%'}, 'color': '#8B0000', 'backgroundColor': '#FFE5E5'},
            {'if': {'filter_query': '{Position} = "Long" && {DECAY%} > 0', 'column_id': 'DECAY%'}, 'color': '#006400', 'backgroundColor': '#90EE90'},
        ]
    
    style_data_conditional = col_styles + additional_styles

    tables = []
    # Group by INDEX for display
    for index_val, group_df in df.groupby("INDEX"):
        type_class = group_df["STRIKE_TYPE"].iloc[0].lower()
        cols = [{"name": col, "id": col} for col in group_df.columns]
        data = group_df.to_dict("records")
        tables.append(
            html.Div(
                [
                    html.H3(f"{index_val} - {group_df['STRIKE_TYPE'].iloc[0]}", style={"textAlign": "center", "marginBottom": "10px"}),
                    dash_table.DataTable(
                        columns=cols,
                        data=data,
                        style_table={"width": "auto"},
                        style_cell={"textAlign": "center", "fontSize": "1.3em", "padding": "8px", "border": "none"},
                        style_header={"fontWeight": "bold", "backgroundColor": "#2c3e50", "color": "#f1c40f", "fontSize": "1.3em"},
                        style_data_conditional=style_data_conditional,
                    ),
                ],
                className=f"table-container {type_class}",
            )
        )
    return tables

@app.callback(
    Output("download-dataframe-csv", "data"),
    [Input("export_button", "n_clicks"),
     Input("type_filter", "value"),
     Input("scenario_number_filter", "value"),
     Input("scenario_letter_filter", "value")],
    prevent_initial_call=True,
)
def export_csv(n_clicks, filter_type, scenario_num_filter, scenario_letter_filter):
    triggered = callback_context.triggered
    if not triggered or "export_button" not in triggered[0]['prop_id']:
        return no_update
    df = load_orderbooks(filter_type, scenario_num_filter, scenario_letter_filter)
    if df.empty:
        return dcc.send_data_frame(pd.DataFrame().to_csv, "empty.csv", index=False)
    return dcc.send_data_frame(df.to_csv, "exported_data.csv", index=False)

if __name__ == "__main__":

    app.run(debug=True,port=5001)
