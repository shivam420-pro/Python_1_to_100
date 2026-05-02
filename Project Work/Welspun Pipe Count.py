"""
utility.py — Shared helpers and event-detection state machines for all Welspun stations.

This module is imported by main.py and contains:

  Data access helpers
  -------------------
  get_device_data()        — Fetch stored event records from MongoDB via the getRows3 API.
  delete_device_data()     — Remove a date-range of records from MongoDB before re-publishing.
  safe_float()             — Safely cast a value to float, returning a default on NaN/None.
  convert_to_json()        — Serialise a DataFrame into the createRows3 API envelope.
  publish_data()           — POST the JSON envelope to the createRows3 API route.
  get_shift()              — Map an IST datetime to a Welspun shift letter (A/B/C).
  calculate_energy()       — Compute energy consumed between two timestamps for a meter device.

  Multi-device fetch helpers
  --------------------------
  merged_multiple_device()       — Outer-join InfluxDB data from multiple devices on time (5 s resample).
  merged_asof_multiple_device()  — Nearest-time (asof) merge with 2-second tolerance.

  Event-detection state machines (one per station type)
  -------------------------------------------------------
  detect_events_JCO()                      — Original JCO logic (D62 0→1 + D104>150 start).
  detect_events_JCO_new()                  — Revised: D101 1→0 edges as start/end markers.
  detect_events_JCO_new_with_D64()         — Same as JCO_new but requires D64>3000 validation.
  detect_events_JCO_LHS_RHS()             — LHS+RHS ≥ target AND D68==0 end condition.
  detect_events_JCO_LHS_RHS_with_pipeout() — LHS+RHS variant with D101 0→1 end.
  detect_events_expander_1/2/1_test()      — D58>0 + D82==0 start; D79==D82 end.
  detect_events_hydro()                    — D59>10 + D64>50 start; D59<300 end; D63>100 required.
  detect_events_hydro_without_cylinder()   — D64>50 start; D59<10 end; D63>100 required.
  detect_events_crimping()                 — D43 0→1 start; D44 0→1 end.
  detect_events_crimping_with_pipeout()    — D44 0→1 transitions used as consecutive boundaries.
  detect_events_tack_welding()             — D17 0→1 / 1→0 edges; merges WELLSAWTW_A2 for PipeID.
  detect_events_ID1/2/3()                  — D41>100 + D19>500 start; D41<100 + D19<50 end.
  detect_events_OD1/2/3()                  — Wagon speed + direction start; limit-switch end.
  detect_events_OD1/2/3_with_welding_on()  — D26 0→1 start; limit-switch end (arc-on variant).
  detect_events_OD1/2/3_with_welding_on_new() — Uses consecutive D26 0→1 transitions as boundaries.
  detect_events_RPEMS()                    — Uses time gap ≤20 min between consecutive rows.
  detect_events_IUT()                      — D7 0→1 (D6==0) start; D7 1→0 (D6==1) end.
  detect_events_FUT()                      — D9 0→1 (D8==0) start; D9 1→0 (D8==1) end.

  Recipe utilities
  ----------------
  fetch_and_merge_devices()  — 5-minute resample + outer-join of multiple recipe-PLC devices.
  assign_recipe_id()         — Match user+client against existing records to reuse RECIPE_IDs.
  clean_column_names()       — Strip "(Dxx)" suffix from aliased InfluxDB column names.

  Configuration
  -------------
  mapping  — Dict keyed by station name; each entry holds INFLUX_DEVICE_ID, MONGO_DEVICE_ID,
             Energy_Device, SENSOR_LIST, and the Function to call for event detection.
"""

import re
import pytz
import requests
import constants as c
import pandas as pd
import numpy as np
import io_connect as io
import warnings
from functools import reduce
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
warnings.filterwarnings("ignore")

# Shared InfluxDB and MongoDB connection objects used by all helper and detection functions
connect = io.DataAccess(c.USER_ID, c.THIRD_PARTY_SERVER, "123", tz=pytz.timezone("Asia/Kolkata"), on_prem=True)
event = io.EventsHandler(c.USER_ID, c.THIRD_PARTY_SERVER, tz=pytz.timezone("Asia/Kolkata"), on_prem=True)

def get_device_data(devID: str, end_Time: str, limit: int):
    """
    Fetch the most recent event records for a station from MongoDB.

    Called at the start of each main() iteration to determine the last stored
    event's start time so the InfluxDB query window can be set correctly.

    Args:
        devID: MongoDB device ID (e.g. "WELLSAWFRM_CT").
        end_Time: Fetch records whose stored time is ≤ this value (IST string).
        limit: Maximum number of records to return (typically 3 — we only need the
               latest start time and the second-to-last PipeID).

    Returns:
        DataFrame with human-readable column names, or None on API failure.
    """
    url = c.GET_DATA_URL
    # rawData=True tells the API to return the D0–Dn fields directly rather than aliased names
    payload = {"devID": devID, "endTime": end_Time, "limit": limit, "rawData": True}
    header = {"userID": c.USER_ID}

    try:
        response = requests.put(url, json=payload, headers=header, verify=False)
        if response.status_code == 200:
            data = response.json()
            if data.get("data"):
                device_data = data["data"]
                flat_data = []
                # Each record has the shape {"_id": "...", "devID": "...", "data": {"D0":..., "D1":...}}
                # Flatten so _id and all Dx fields are at the top level for easy DataFrame creation
                for record in device_data:
                    flat_record = {"_id": record["_id"], "devID": record["devID"]}
                    flat_record.update(record["data"])  # Merge D0–D10 fields into the row dict
                    flat_data.append(flat_record)

                df = pd.DataFrame(flat_data)
                # Rename D0–D10 to human-readable column names matching what event detection produces
                df = df.rename(columns={
                    "D0": "Start Time",
                    "D1": "End Time",
                    "D2": "cycle_time_format",
                    "D3": "Energy",
                    "D4": "Production",
                    "D5": "Diameter",
                    "D6": "Thickness",
                    "D7": "Length",
                    "D8": "cycle_time",
                    "D9": "PipeID",
                    "D10": "client_name"
                })
                return df
        else:
            print(f"Failed with status code: {response.status_code}")
            print("Response text:", response.text)
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def delete_device_data(devID: str, start_time: str, end_time: str):
    """
    Delete all stored event records for a station within a date-range from MongoDB.

    Called just before re-publishing so that any previously stored partial cycles
    (where the end time was unknown on the last run) are replaced by the now-complete
    version.  This delete + re-insert pattern ensures no duplicate or stale rows remain.

    Args:
        devID: MongoDB device ID whose records to delete.
        start_time: Start of the deletion window (inclusive), IST string.
        end_time: End of the deletion window (inclusive), IST string.
    """
    url = c.DELETE_DATA_URL
    payload = {
        "devID": devID,
        "startTime": start_time,
        "endTime": end_time,
        "rawData": True,  # Required flag to indicate raw-table deletion (not aggregated data)
    }
    header = {"userID": c.USER_ID}
    try:
        response = requests.put(url, json=payload, headers=header, verify=False)
        if response.status_code == 200:
            print("Data Deleted Successfully")
        else:
            print(f"Failed with status code: {response.status_code}")
            print("Response text:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def safe_float(val, default=0.0):
    """Return float(val) safely, or default if val is NaN/None (avoids f-string formatting errors)."""
    return float(val) if not pd.isna(val) else default

def convert_to_json(device_id, df):
    """
    Serialise an events DataFrame into the createRows3 API envelope.

    Maps each DataFrame row to a fixed schema of D0–D19 sensor slots:
      D0  = Start Time (string)        D10 = client_name
      D1  = End Time (string)          D11 = Max hydro pressure (D63 max)
      D2  = cycle_time_format (MM:SS)  D12 = DW_min (minimum die width, JCO)
      D3  = Energy (kWh, 2dp)          D13 = bending_pressure max
      D4  = Production (kg, 2dp)       D14 = tonnage_force max
      D5  = Diameter (mm, 2dp)         D15 = pilot_pressure max
      D6  = Thickness (mm, 2dp)        D16 = min_pilot_pressure
      D7  = Length (mm, 2dp)           D17 = client_cycle_time
      D8  = cycle_time (seconds, 0dp)  D18 = D18_max (station-specific max)
      D9  = PipeID                     D19 = D19_max (station-specific max)

    Args:
        device_id: Target MongoDB device ID for all rows in this batch.
        df: DataFrame of detected events (timestamps already converted to strings).

    Returns:
        dict: {"data": {"rows": [...]}, "type": "SOODPOLY"} for the API.
    """
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "_id": str(row.get("_id", "")),  # Empty string for new rows; keeps API happy
                "devID": device_id,
                "data": {
                    "D0":  str(row.get("Start Time", "")),
                    "D1":  str(row.get("End Time", "")),
                    "D2":  str(row.get("cycle_time_format", "")),
                    "D3":  f"{safe_float(row.get('Energy')):.2f}",
                    "D4":  f"{safe_float(row.get('Production')):.2f}",
                    "D5":  f"{safe_float(row.get('Diameter')):.2f}",
                    "D6":  f"{safe_float(row.get('Thickness')):.2f}",
                    "D7":  f"{safe_float(row.get('Length')):.2f}",
                    "D8":  f"{safe_float(row.get('cycle_time')):.0f}",
                    "D9":  str(row.get("PipeID", "")),
                    "D10": str(row.get("client_name", "")),
                    "D11": f"{safe_float(row.get('Max D63')):.2f}",          # Hydro max pressure
                    "D12": f"{safe_float(row.get('DW_min')):.2f}",           # JCO minimum die width
                    "D13": f"{safe_float(row.get('bending_pressure')):.2f}", # JCO max bending pressure
                    "D14": f"{safe_float(row.get('tonnage_force')):.2f}",    # JCO max tonnage
                    "D15": f"{safe_float(row.get('pilot_pressure')):.2f}",   # JCO max pilot pressure
                    "D16": f"{safe_float(row.get('min_pilot_pressure')):.2f}",
                    "D17": f"{safe_float(row.get('client_cycle_time')):.2f}",
                    "D18": f"{safe_float(row.get('D18_max')):.2f}",
                    "D19": f"{safe_float(row.get('D19_max')):.2f}",
                },
                "rawData": True,  # Tell API to store values as-is, not as calculated metrics
            }
        )

    return {"data": {"rows": rows}, "type": "SOODPOLY"}

def publish_data(output):
    """
    POST an event-batch payload to the createRows3 API to store records in MongoDB.

    Called after delete_device_data() so the storage layer always has the most
    up-to-date version of each cycle's data.

    Args:
        output: Dict returned by convert_to_json() — the full API envelope.
    """
    url = c.PUBLISH_DATA_URL
    headers = {"userID": c.USER_ID}

    try:
        response = requests.put(url, headers=headers, json=output)

        if response.status_code == 200:
            print("Data successfully pushed!")
            print("Response:", response.json())
        else:
            print("Failed to push data.")
            print(f"Status Code: {response.status_code}")
            print(f"Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def get_shift(t):
    """
    Map an IST datetime to a Welspun shift letter.

    Welspun runs three 8-hour shifts per day:
      A: 07:00 – 14:59  (day shift)
      B: 15:00 – 22:59  (evening shift)
      C: 23:00 – 06:59  (night shift, crosses midnight)

    The C-shift date is adjusted back one calendar day for events before 07:00
    (handled separately in main.py via Shift_Date logic).

    Args:
        t: pandas Timestamp or datetime object in IST (no timezone needed — caller ensures IST).

    Returns:
        "A", "B", or "C".
    """
    hour = t.hour
    if 7 <= hour < 15:
        return "A"   # Day shift
    elif 15 <= hour < 23:
        return "B"   # Evening shift
    else:
        return "C"   # Night shift (23:00–06:59)

# def merged_multiple_device(start_time, end_time, device_configs):
#     try:
#         dfs = []
#         for dev_id, sensors in device_configs.items():
#             df = connect.data_query(
#                 device_id=dev_id,
#                 sensor_list=sensors,
#                 cal=True,
#                 alias=False,
#                 start_time=start_time,
#                 end_time=end_time
#             )

#             # --- Check if time column exists ---
#             if 'time' not in df.columns:
#                 print(f"❌ 'time' column missing in device {dev_id}")
#                 return pd.DataFrame()

#             # Ensure time is datetime and set as index
#             df['time'] = pd.to_datetime(df['time'])
#             df = df.set_index('time')

#             # Resample every 5 seconds using last value
#             df = df.resample('5S').last().reset_index()

#             dfs.append(df)

#         if not dfs:
#             return pd.DataFrame()

#         # --- Merge all on time ---
#         merged_df = reduce(lambda left, right: pd.merge(left, right, on='time', how='outer'), dfs)

#         # --- Sort by time ---
#         merged_df = merged_df.sort_values('time').reset_index(drop=True)
#         merged_df['time'] = pd.to_datetime(merged_df['time'])

#         return merged_df

#     except Exception as e:
#         print(f"⚠️ Error during merging: {e}")
#         return pd.DataFrame()

def merged_multiple_device(start_time, end_time, device_configs, process_type=""):
    """
    Fetch data from multiple InfluxDB devices and outer-join them on a common time axis.

    Used for stations where sensors live on different PLCs (e.g. Crimping, OD1/2/3,
    ID1/2/3) but need to be seen together for event detection.

    Steps:
      1. Query each device for its sensor list over the requested time window.
      2. Resample each device's data to a 5-second grid (take last value per bucket)
         so that timestamps across devices align for a clean join.
      3. Outer-join all device DataFrames on 'time'; gaps are left as NaN.

    Special handling for MERGE_WITH_JOC stations (e.g. Crimping): if a secondary
    device returns no 'time' column, that device is skipped rather than aborting the
    whole merge — the primary device data is still returned.

    Args:
        start_time: Start of the query window (IST string).
        end_time: End of the query window (IST string).
        device_configs: Dict of {device_id: [sensor_list]}.
        process_type: Station name — used to decide error-tolerance behaviour.

    Returns:
        Merged DataFrame sorted by time, or empty DataFrame on failure.
    """
    try:
        dfs = []
        for dev_id, sensors in device_configs.items():
            df = connect.data_query(
                device_id=dev_id,
                sensor_list=sensors,
                cal=True,
                alias=False,
                start_time=start_time,
                end_time=end_time
            )

            if 'time' not in df.columns:
                if process_type in c.MERGE_WITH_JOC:
                    # For Crimping, the JCO device data might be absent — continue with what we have
                    print(f"⚠️ Skipping {dev_id} — 'time' column missing (Crimping mode).")
                    continue
                else:
                    print(f"❌ 'time' column missing in device {dev_id}")
                    return pd.DataFrame()

            # Align all devices to a common 5-second grid so the outer join works cleanly
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
            df = df.set_index('time')
            df = df.resample('5S').last().reset_index()  # Take last value in each 5-second bucket

            dfs.append(df)

        if not dfs:
            print("⚠️ No valid dataframes to merge.")
            return pd.DataFrame()

        if len(dfs) == 1:
            merged_df = dfs[0]
        else:
            # Chain outer joins across all device DataFrames; reduce() applies merge left-to-right
            merged_df = reduce(lambda left, right: pd.merge(left, right, on='time', how='outer'), dfs)

        merged_df = merged_df.sort_values('time').reset_index(drop=True)
        merged_df['time'] = pd.to_datetime(merged_df['time'])

        return merged_df

    except Exception as e:
        print(f"⚠️ Error during merge: {e}")
        return pd.DataFrame()

def merged_asof_multiple_device(start_time, end_time, device_configs, process_type=""):
    """
    Fetch multiple InfluxDB devices and join them using the nearest-timestamp strategy.

    Unlike merged_multiple_device() which requires timestamps to align on a grid,
    this function uses pandas merge_asof with a 2-second tolerance.  This is appropriate
    for Crimping and ID1 where the secondary device (JCO forming machine) logs at a
    slightly different cadence and the rows would otherwise never align on an exact time.

    Steps:
      1. Query each device independently.
      2. Sort each DataFrame by time (required by merge_asof).
      3. Sequentially merge: for each row in the left DataFrame, find the closest
         row in the right DataFrame within 2 seconds and join their columns.
         Rows with no match within tolerance get NaN for the right-device columns.

    Args:
        start_time: Start of the query window (IST string).
        end_time: End of the query window (IST string).
        device_configs: Dict of {device_id: [sensor_list]}.
        process_type: Station name — used to decide error-tolerance behaviour.

    Returns:
        Merged DataFrame sorted by time, or empty DataFrame on failure.
    """
    try:
        dfs = []

        for dev_id, sensors in device_configs.items():
            df = connect.data_query(
                device_id=dev_id,
                sensor_list=sensors,
                cal=True,
                alias=False,
                start_time=start_time,
                end_time=end_time
            )

            if 'time' not in df.columns:
                if process_type in c.MERGE_WITH_JOC:
                    print(f"⚠️ Skipping {dev_id} — 'time' column missing.")
                    continue
                else:
                    print(f"❌ 'time' column missing in device {dev_id}")
                    return pd.DataFrame()

            df['time'] = pd.to_datetime(df['time'], errors='coerce')
            df = df.dropna(subset=['time'])
            df = df.sort_values('time')  # merge_asof requires sorted input

            dfs.append(df)

        if not dfs:
            print("⚠️ No valid dataframes to merge.")
            return pd.DataFrame()

        merged_df = dfs[0]  # Use first device as the left-side anchor

        # Sequentially join each additional device using nearest-time matching
        for df in dfs[1:]:
            merged_df = pd.merge_asof(
                merged_df.sort_values('time'),
                df.sort_values('time'),
                on='time',
                direction='nearest',          # Take the closest timestamp in either direction
                tolerance=pd.Timedelta('2s')  # Rows more than 2 s apart are not matched (NaN)
            )

        merged_df = merged_df.sort_values('time').reset_index(drop=True)

        return merged_df

    except Exception as e:
        print(f"⚠️ Error during merge: {e}")
        return pd.DataFrame()

# def detect_events_JCO(df):
#     events = []
#     in_event = False
#     start_time = None
#     n = 400  # lookahead window

#     for i in range(1, len(df)):
#         # --- Start Conditions (only if no active event) ---
#         if not in_event:
#             # cond1_start = df.loc[i-1, "D100"] == 1 and df.loc[i, "D100"] == 0
#             # if cond1_start:
#                 future_window = df.loc[i:min(i+n, len(df)-1)]
#                 match_start = future_window[future_window["D104"] > 150]
#                 if not match_start.empty:
#                     in_event = True
#                     start_time = match_start.iloc[0]["time"]

#         # --- End Conditions (only if event is active) ---
#         if in_event:
#             cond2_end = df.loc[i-1, "D101"] == 0 and df.loc[i, "D101"] == 1
#             if cond2_end:
#                 future_window = df.loc[i:min(i+n, len(df)-1)]
#                 match_end = future_window[future_window["D104"].between(50, 200)]
#                 if not match_end.empty:
#                     end_time = match_end.iloc[0]["time"]
#                     events.append({
#                         "device_id": "WELLSAWFRM_A1",
#                         "Start Time": start_time,
#                         "End Time": end_time,
#                         "Diameter": df.loc[i,"D53"],
#                         "Thickness": df.loc[i,"D52"],
#                         "Length": df.loc[i,"D54"],
#                         "client_name": df.loc[i,"D56"]
#                     })
#                     in_event = False
#                     start_time = None

#     # If an event started but never ended
#     if in_event:
#         events.append({
#             "device_id": "WELLSAWFRM_A1",
#             "Start Time": start_time,
#             "End Time": None
#         })

#     return pd.DataFrame(events)
def detect_events_JCO(df):
    """
    Original JCO forming-press event detection (legacy version, not currently active in mapping).

    Start: D62 goes 0→1 (pipe-in signal), then D104 > 150 within a 2000-row lookahead.
    End:   D101 goes 0→1 (pipe-out signal), then D104 between 50–200 in lookahead.

    The lookahead window (n=2000 rows) is needed because D104 (pressure) may not spike
    immediately at the moment the D62/D101 edge occurs — it lags by a few seconds.
    The actual start/end timestamp is taken from the FIRST row in the lookahead where
    the pressure condition is met, not from the edge row itself.
    """
    # Forward-fill slowly-changing sensor columns so gaps don't break the edge detection
    cols_to_ffill = ['D52', 'D53', 'D54', 'D56', "D100", "D101", "D62"]
    df[cols_to_ffill] = df[cols_to_ffill].ffill()
    events = []
    in_event = False
    start_time = None
    n = 400 * 5  # Lookahead window (2000 rows at ~1 row/s = ~33 minutes)

    for i in range(1, len(df)):
        if not in_event:
            cond_start = df.loc[i - 1, "D62"] == 0 and df.loc[i, "D62"] == 1  # Pipe-in signal rising edge
            if cond_start:
                future_window = df.loc[i:min(i + n, len(df) - 1)]
                match_start = future_window[future_window["D104"] > 150]  # Confirm actual forming has started
                if not match_start.empty:
                    in_event = True
                    start_time = match_start.iloc[0]["time"]  # Use first high-pressure row as start

        if in_event:
            cond2_end = df.loc[i - 1, "D101"] == 0 and df.loc[i, "D101"] == 1  # Pipe-out signal rising edge
            if cond2_end:
                future_window = df.loc[i:min(i + n, len(df) - 1)]
                match_end = future_window[future_window["D104"].between(50, 200)]  # Moderate pressure confirms pipe exit
                if not match_end.empty:
                    end_time = match_end.iloc[0]["time"]
                    events.append({
                        "device_id": "WELLSAWFRM_A1",
                        "Start Time": start_time,
                        "End Time": end_time,
                        "Diameter": df.loc[i, "D53"],
                        "Thickness": df.loc[i, "D52"],
                        "Length": df.loc[i, "D54"],
                        "client_name": df.loc[i, "D56"]
                    })
                    in_event = False
                    start_time = None

    if in_event:
        events.append({
            "device_id": "WELLSAWFRM_A1",
            "Start Time": start_time,
            "End Time": None
        })

    return pd.DataFrame(events)


def detect_events_JCO_new(df):
    """
    Revised JCO forming-press event detection using D101 falling edges as boundaries.

    Each JCO press cycle begins and ends when the pipe-out signal (D101) drops from 1 to 0:
      - The falling edge that ENDS the previous cycle simultaneously STARTS the next one.
      - This "immediate restart" approach ensures no gap between consecutive cycles and
        correctly handles back-to-back pipes without a manual reset signal.

    During each event the function tracks the peak values of:
      D6  → bending_pressure (max across the cycle)
      D77 → tonnage_force (max)
      D3  → pilot_pressure (max and min)

    Args:
        df: Merged sensor DataFrame from WELLSAWFRM_A1.

    Returns:
        DataFrame of events with Start Time, End Time, pipe dimensions, and pressure stats.
    """
    # Forward-fill slowly-updating sensor columns to prevent NaN mid-cycle
    cols_to_ffill = ['D52', 'D53', 'D54', 'D56', "D100", "D101", "D62", "D6", "D77", "D3", "D58"]
    df[cols_to_ffill] = df[cols_to_ffill].ffill()
    events = []
    device_id = "WELLSAWFRM_A1"

    in_event = False
    start_time = None
    start_index = None

    # Running peak/trough trackers reset at the start of each new event
    max_bending_pressure = 0   # D6 — hydraulic bending pressure
    max_tonnage_force = 0      # D77 — total applied tonnage
    max_pilot_pressure = 0     # D3 — pilot hydraulic pressure
    min_pilot_pressure = 0

    for i in range(1, len(df)):

        # -------------------------------
        # START CONDITION (D101: 1 → 0)
        # -------------------------------
        cond_start = df.loc[i - 1, "D101"] == 1 and df.loc[i, "D101"] == 0

        # If not in event → start event normally
        if not in_event and cond_start:
            in_event = True
            start_index = i
            start_time = df.loc[i, "time"]
            max_bending_pressure = df.loc[i, "D6"]
            max_tonnage_force = df.loc[i, "D77"]
            max_pilot_pressure = df.loc[i, "D3"]
            min_pilot_pressure = df.loc[i, "D3"]

        # -------------------------------
        # END CONDITION (D101: 1 → 0)
        # -------------------------------
        if in_event and i > start_index:

            max_bending_pressure = max(max_bending_pressure, df.loc[i, "D6"])
            max_tonnage_force = max(max_tonnage_force, df.loc[i, "D77"])
            max_pilot_pressure = max(max_pilot_pressure, df.loc[i, "D3"])
            min_pilot_pressure = min(min_pilot_pressure, df.loc[i, "D3"])

            cond_end = df.loc[i - 1, "D101"] == 1 and df.loc[i, "D101"] == 0

            if cond_end:
                # Close current event
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": df.loc[i, "D53"] if "D53" in df.columns else 0,
                    "Thickness": df.loc[i, "D52"] if "D52" in df.columns else 0,
                    "Length": df.loc[i, "D54"] if "D54" in df.columns else 0,
                    "client_name": df.loc[i, "D56"] if "D56" in df.columns else "",
                    "client_cycle_time": df.loc[i, "D58"] if "D58" in df.columns else 0,
                    "bending_pressure": max_bending_pressure,
                    "tonnage_force": max_tonnage_force,
                    "pilot_pressure":max_pilot_pressure,
                    "min_pilot_pressure":min_pilot_pressure
                })

                # --------------------------------------------
                # Immediately start the next event at THIS END
                # --------------------------------------------
                in_event = True
                start_index = i
                start_time = df.loc[i, "time"]

    # ----------------------------------
    # If an event started but never ended
    # ----------------------------------
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })

    return pd.DataFrame(events)

def detect_events_JCO_new_with_D64(df):
    """
    JCO event detection identical to detect_events_JCO_new but with an additional
    quality gate: D64 (die-width sensor, measured in 0.1 mm units) must exceed 3000
    (i.e. 300 mm) at least once during the cycle for it to be recorded.

    This validation filters out false starts where the press cycles but no real pipe
    is being formed (e.g. dry runs, maintenance strokes).  Cycles where D64 never
    exceeds 3000 are silently discarded even if the D101 boundary conditions are met.

    Currently the ACTIVE version used in the JCO mapping entry.
    """
    cols_to_ffill = ['D52', 'D53', 'D54', 'D56', "D100", "D101",
                     "D62", "D6", "D77", "D3", "D58", "D64"]
    df[cols_to_ffill] = df[cols_to_ffill].ffill()

    events = []
    device_id = "WELLSAWFRM_A1"

    in_event = False
    start_time = None
    start_index = None

    max_bending_pressure = 0
    max_tonnage_force = 0
    max_pilot_pressure = 0
    min_pilot_pressure = 0
    d64_above_30 = False  # Flag: did D64 exceed 3000 during this cycle?

    for i in range(1, len(df)):

        # -------------------------------
        # START CONDITION (D101: 1 → 0)
        # -------------------------------
        cond_start = df.loc[i - 1, "D101"] == 1 and df.loc[i, "D101"] == 0

        if not in_event and cond_start:
            in_event = True
            start_index = i
            start_time = df.loc[i, "time"]

            max_bending_pressure = df.loc[i, "D6"]
            max_tonnage_force = df.loc[i, "D77"]
            max_pilot_pressure = df.loc[i, "D3"]
            min_pilot_pressure = df.loc[i, "D3"]
            d64_above_30 = df.loc[i, "D64"] > 3000   # RESET + CHECK

        # -------------------------------
        # EVENT IN PROGRESS
        # -------------------------------
        if in_event and i > start_index:

            max_bending_pressure = max(max_bending_pressure, df.loc[i, "D6"])
            max_tonnage_force = max(max_tonnage_force, df.loc[i, "D77"])
            max_pilot_pressure = max(max_pilot_pressure, df.loc[i, "D3"])
            min_pilot_pressure = min(min_pilot_pressure, df.loc[i, "D3"])

            # Track D64 condition
            if df.loc[i, "D64"] > 3000:
                d64_above_30 = True

            # -------------------------------
            # END CONDITION (D101: 1 → 0)
            # -------------------------------
            cond_end = df.loc[i - 1, "D101"] == 1 and df.loc[i, "D101"] == 0

            if cond_end and d64_above_30:
                end_time = df.loc[i, "time"]

                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": df.loc[i, "D53"],
                    "Thickness": df.loc[i, "D52"],
                    "Length": df.loc[i, "D54"],
                    "client_name": df.loc[i, "D56"],
                    "client_cycle_time": df.loc[i, "D58"],
                    "bending_pressure": max_bending_pressure,
                    "tonnage_force": max_tonnage_force,
                    "pilot_pressure": max_pilot_pressure,
                    "min_pilot_pressure": min_pilot_pressure
                })

                # --------------------------------------------
                # Immediately start next event at THIS index
                # --------------------------------------------
                start_index = i
                start_time = df.loc[i, "time"]

                max_bending_pressure = df.loc[i, "D6"]
                max_tonnage_force = df.loc[i, "D77"]
                max_pilot_pressure = df.loc[i, "D3"]
                min_pilot_pressure = df.loc[i, "D3"]
                d64_above_30 = df.loc[i, "D64"] > 3000

    # ----------------------------------
    # If an event started but never ended
    # ----------------------------------
    if in_event and d64_above_30:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })

    return pd.DataFrame(events)


def detect_events_JCO_LHS_RHS(df):
    """
    JCO event detection for the LHS+RHS bending variant (legacy, not active in mapping).

    Start: D62 goes 0→1 AND D104 > 150 within the next 12 rows (1-minute lookahead).
    End:   Cumulative bending (max_D62 + max_D68) reaches the target (D60), AND
           D68 == 0 (RHS cylinder retracted), AND 50 ≤ D104 ≤ 200.

    The two-phase logic (first reach target, then wait for D68==0) ensures the event
    does not close until the second forming half has fully retracted.
    """
    cols_to_ffill = ['D52', 'D53', 'D54', 'D56', "D100", "D101", "D62"]
    df[cols_to_ffill] = df[cols_to_ffill].ffill()

    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWFRM_A1"
    lookahead = 12  # 12 rows × 5 s/row = 60 s pressure confirmation window

    max_D62 = 0        # Cumulative LHS bending position
    max_D68 = 0        # Cumulative RHS bending position
    reached_target = False  # True once LHS+RHS ≥ D60 target

    for i in range(1, len(df)):
        prev = df.loc[i-1]
        curr = df.loc[i]

        # ---------------- START CONDITION ----------------
        if not in_event:
            rhs_trigger = (prev["D62"] == 0 and curr["D62"] == 1)

            if rhs_trigger:
                future = df.loc[i:min(i + lookahead, len(df)-1)]
                pos_ok = (future["D104"] > 150).any()

                if pos_ok:
                    in_event = True
                    start_time = curr["time"]
                    max_D62 = 0
                    max_D68 = 0
                    reached_target = False
                    continue

        # ---------------- EVENT PROCESSING ----------------
        if in_event:
            max_D62 = max(max_D62, curr["D62"])
            max_D68 = max(max_D68, curr["D68"])

            bending_target = curr["D60"]

            if not reached_target:
                if (max_D62 + max_D68) >= bending_target:
                    reached_target = True
                continue

            # END CONDITION with new D104 range requirement
            if reached_target and curr["D68"] == 0 and 50 <= curr["D104"] <= 200:
                end_time = curr["time"]

                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": curr.get("D53", 0),
                    "Thickness": curr.get("D52", 0),
                    "Length": curr.get("D54", 0),
                    "client_name": curr.get("D56", "")
                })

                in_event = False
                max_D62 = 0
                max_D68 = 0
                reached_target = False
                start_time = None

    return pd.DataFrame(events)

def detect_events_JCO_LHS_RHS_with_pipeout(df):
    """
    JCO LHS+RHS variant that uses D101 0→1 (pipe-out signal) as the end condition.

    Unlike detect_events_JCO_LHS_RHS which waits for the bending target + D68==0,
    this version ends as soon as the pipe-out signal fires.  Also captures peak pressure
    and force values during the cycle (bending_pressure, tonnage_force, pilot_pressure).
    """
    cols_to_ffill = ['D52', 'D53', 'D54', 'D56', "D100", "D101", "D62", "D6", "D77", "D3", "D58"]
    df[cols_to_ffill] = df[cols_to_ffill].ffill()

    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWFRM_A1"
    lookahead = 12  # 12 rows × 5 s = 60-second confirmation window (not used for end but kept for symmetry)

    max_D62 = 0
    max_D68 = 0
    reached_target = False

    # NEW: max trackers
    max_bending_pressure = 0   # D6
    max_tonnage_force = 0
    max_pilot_pressure = 0
    min_pilot_pressure = 0

    for i in range(1, len(df)):
        prev = df.loc[i - 1]
        curr = df.loc[i]

        # ---------------- START CONDITION ----------------
        if not in_event:
            rhs_trigger = (prev["D62"] == 0 and curr["D62"] == 1)

            if rhs_trigger:
                in_event = True
                start_time = curr["time"]

                max_D62 = 0
                max_D68 = 0
                reached_target = False

                # NEW: reset max trackers
                max_bending_pressure = curr.get("D6", 0)
                max_tonnage_force = curr.get("D77", 0)
                max_pilot_pressure = curr.get("D3", 0)
                min_pilot_pressure = curr.get("D3", 0)

                continue

        # ---------------- EVENT PROCESSING ----------------
        if in_event:
            # NEW: Update maximum values while event is active
            max_bending_pressure = max(max_bending_pressure, curr.get("D6", 0))
            max_tonnage_force = max(max_tonnage_force, curr.get("D77", 0))
            max_pilot_pressure = max(max_pilot_pressure, curr.get("D3", 0))
            min_pilot_pressure = min(min_pilot_pressure, curr.get("D3", 0))

            # ---------------- END CONDITION ----------------
            end_condition = (prev["D101"] == 0 and curr["D101"] == 1)
            reached_target = True

            if reached_target and end_condition:
                end_time = curr["time"]

                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": curr.get("D53", 0),
                    "Thickness": curr.get("D52", 0),
                    "Length": curr.get("D54", 0),
                    "client_name": curr.get("D56", ""),
                    "client_cycle_time": curr.get("D58", ""),
                    # NEW: Use max values instead of current values
                    "bending_pressure": max_bending_pressure,
                    "tonnage_force": max_tonnage_force,
                    "pilot_pressure":max_pilot_pressure,
                    "min_pilot_pressure":min_pilot_pressure
                })

                in_event = False
                max_D62 = 0
                max_D68 = 0
                reached_target = False
                start_time = None

    return pd.DataFrame(events)


def detect_events_expander_1(df):
    """
    Detect pipe expansion cycles for Expander 1 (WELLSAWEXP1_A1).

    Start: D58 (gripper car position) > 0 AND D82 (steps completed) == 0,
           confirmed by at least one row in the next 10 rows where D82 > 0
           (lookahead prevents false starts when the car is moving but not yet expanding).
    End:   D79 (steps required) == D82 (steps completed) — all programmed expansion
           steps have been executed, meaning the pipe is fully expanded.

    Captures pipe dimensions (Diameter=D71, Thickness=D70, Length=D72), client name
    (D73), and minimum die-width DW_min (D55) at cycle end.
    """
    df['D73'] = df['D73'].ffill()  # client_name is logged infrequently — forward-fill across gaps
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWEXP1_A1"
    n = 10  # Lookahead window: 10 rows confirms D82 will actually increment
 
    for i in range(len(df)):
        # --- Start Conditions ---
        if not in_event:
            cond1 = df.loc[i, "D58"] > 0      # Gripper Car Position > 0
            cond2 = df.loc[i, "D82"] == 0     # Steps Completed == 0
 
            if cond1 and cond2:
                # Look ahead 10 rows to see if any "D82" > 0
                next_window = df.loc[i+1 : i+n, "D82"]
                if (next_window > 0).any():
                    # Confirmed start of event
                    in_event = True
                    start_time = df.loc[i, "time"]
 
        # --- End Conditions ---
        if in_event:
            cond_end = df.loc[i, "D79"] == df.loc[i, "D82"]   # Steps Required == Steps Completed
 
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": df.loc[i,"D71"],
                    "Thickness": df.loc[i,"D70"],
                    "Length": df.loc[i,"D72"],
                    "client_name": df.loc[i,"D73"],
                    "DW_min": df.loc[i,"D55"]
                })
                in_event = False
                start_time = None
 
    # --- Handle case where event starts but never ends ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
 
    return pd.DataFrame(events)

def detect_events_expander_1_test(df):
    """
    Experimental Expander 1 variant using D58 > 50 / D58 < 50 as boundaries.

    Simplified compared to detect_events_expander_1: uses the gripper-car position
    threshold directly as both start (>50) and end (<50) conditions, removing the
    D82 steps-completed requirement.  Used for testing alternative boundary logic.
    """
    df['D73'] = df['D73'].ffill()
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWEXP1_A1"
    n = 10  # Lookahead window (retained for consistency)
 
    for i in range(len(df)):
        # --- Start Conditions ---
        if not in_event:
            cond1 = df.loc[i, "D58"] > 50      # Gripper Car Position > 0
            # cond2 = df.loc[i, "D82"] == 0     # Steps Completed == 0
 
            if cond1:
                # Look ahead 10 rows to see if any "D82" > 0
                next_window = df.loc[i+1 : i+n, "D82"]
                if (next_window > 0).any():
                    # Confirmed start of event
                    in_event = True
                    start_time = df.loc[i, "time"]
 
        # --- End Conditions ---
        if in_event:
            cond_end = df.loc[i, "D58"] < 50   # Steps Required == Steps Completed
 
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": df.loc[i,"D71"],
                    "Thickness": df.loc[i,"D70"],
                    "Length": df.loc[i,"D72"],
                    "client_name": df.loc[i,"D73"],
                    "DW_min": df.loc[i,"D55"]
                })
                in_event = False
                start_time = None
 
    # --- Handle case where event starts but never ends ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
 
    return pd.DataFrame(events)

def detect_events_expander_2(df):
    """
    Detect pipe expansion cycles for Expander 2 (WELLSAWEXP2_A1).

    Identical logic to detect_events_expander_1 but targets a separate machine
    (WELLSAWEXP2_A1) with its own energy meter.  Same sensors D58, D79, D82 apply.
    """
    df['D73'] = df['D73'].ffill()
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWEXP2_A1"
    n = 10  # Lookahead window
    for i in range(len(df)):
        # --- Start Conditions ---
        if not in_event:
            cond1 = df.loc[i, "D58"] > 0      # Gripper Car Position > 0
            cond2 = df.loc[i, "D82"] == 0     # Steps Completed == 0
 
            if cond1 and cond2:
                # Look ahead 10 rows to see if any "D82" > 0
                next_window = df.loc[i+1 : i+n, "D82"]
                if (next_window > 0).any():
                    # Confirmed start of event
                    in_event = True
                    start_time = df.loc[i, "time"]
 
        # --- End Conditions ---
        if in_event:
            cond_end = df.loc[i, "D79"] == df.loc[i, "D82"]   # Steps Required == Steps Completed
 
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": df.loc[i,"D71"],
                    "Thickness": df.loc[i,"D70"],
                    "Length": df.loc[i,"D72"],
                    "client_name": df.loc[i,"D73"],
                    "DW_min": df.loc[i,"D55"]
                })
                # print(295,events)
                in_event = False
                start_time = None
 
    # --- Handle case where event starts but never ends ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    print(305,events)
    return pd.DataFrame(events)

def detect_events_hydro(df):
    """
    Detect hydrostatic testing cycles (WELLSAWHDRT_A1).

    Start: D59 (main cylinder position) > 10 AND D64 (hydraulic force) > 50.
    End:   D59 drops below 300 (cylinder retracting — test pressure released).
    Quality gate: D63 (water pressure) must exceed 100 bar at least once during the
    event, otherwise the cycle is discarded as a dry run or partial stroke.

    Records the maximum D63 value seen during the test for quality reporting.
    Pipe dimensions (Diameter=D66, Thickness=D65, Length=D67) are captured at cycle end.
    """
    cols_to_ffill = ['D65', "D66", "D67"]
    df[cols_to_ffill] = df[cols_to_ffill].ffill()  # Dimensions logged infrequently — carry forward
    events = []
    in_event = False
    start_time = None
    water_force_triggered = False  # True once D63 (water pressure) exceeds 100 bar during the test
    d63_values = []  # Collect all D63 readings during the event to find the peak test pressure
    device_id = "WELLSAWHDRT_A1"
 
    for i in range(len(df)):
        # --- Start Conditions ---
        if not in_event:
            cond1 = df.loc[i, "D59"] > 10    # Main Cylinder Position > 10
            cond2 = df.loc[i, "D64"] > 50    # Hydraulic Force > 50
 
            if cond1 and cond2:
                in_event = True
                start_time = df.loc[i, "time"]
                water_force_triggered = False
                d63_values = []  # start collecting D63 values
 
        # --- During Event ---
        if in_event:
            d63_val = df.loc[i, "D63"]
            d63_values.append(d63_val)
 
            if d63_val > 100:
                water_force_triggered = True
 
        # --- End Condition ---
        if in_event and df.loc[i, "D59"] < 300:
            end_time = df.loc[i, "time"]
 
            # Only keep event if D63 > 100 occurred
            if water_force_triggered:
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": df.loc[i,"D66"],
                    "Thickness": df.loc[i,"D65"],
                    "Length": df.loc[i,"D67"],
                    "Max D63": max(d63_values)
                })
 
            # reset for next event
            in_event = False
            start_time = None
            water_force_triggered = False
            d63_values = []
 
    # --- Handle unclosed event ---
    if in_event and water_force_triggered:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None,
            "Max D63": max(d63_values) if d63_values else None
        })
 
    return pd.DataFrame(events)

def detect_events_hydro_without_cylinder(df):
    """
    Hydrotester event detection variant that does NOT require the cylinder position sensor.

    Used when D59 (main cylinder position) is not available or unreliable.
    Start: D64 (hydraulic force) > 50.
    End:   D59 < 10 (cylinder retracted — same end signal, just different start logic).
    Quality gate: D63 > 100 still required to count as a real test.

    Captures the same fields as detect_events_hydro.
    """
    cols_to_ffill = ['D65', "D66", "D67"]
    df[cols_to_ffill] = df[cols_to_ffill].ffill()
    events = []
    in_event = False
    start_time = None
    water_force_triggered = False
    d63_values = []
    device_id = "WELLSAWHDRT_A1"
 
    for i in range(len(df)):
        # --- Start Conditions ---
        if not in_event:
            # cond1 = df.loc[i, "D59"] > 10    # Main Cylinder Position > 10
            cond2 = df.loc[i, "D64"] > 50    # Hydraulic Force > 50
 
            if cond2:
                in_event = True
                start_time = df.loc[i, "time"]
                water_force_triggered = False
                d63_values = []  # start collecting D63 values
 
        # --- During Event ---
        if in_event:
            d63_val = df.loc[i, "D63"]
            d63_values.append(d63_val)
 
            if d63_val > 100:
                water_force_triggered = True
 
        # --- End Condition ---
        if in_event and df.loc[i, "D59"] < 10:
            end_time = df.loc[i, "time"]
 
            # Only keep event if D63 > 100 occurred
            if water_force_triggered:
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": df.loc[i,"D66"],
                    "Thickness": df.loc[i,"D65"],
                    "Length": df.loc[i,"D67"],
                    "Max D63": max(d63_values)
                })
 
            # reset for next event
            in_event = False
            start_time = None
            water_force_triggered = False
            d63_values = []
 
    # --- Handle unclosed event ---
    if in_event and water_force_triggered:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None,
            "Max D63": max(d63_values) if d63_values else None
        })
 
    return pd.DataFrame(events)


def detect_events_crimping(df):
    """
    Detect crimping-machine cycles using the clamp open/close signals (legacy version).

    Start: D43 goes 0→1 (clamp closes on the pipe end).
    End:   D44 goes 0→1 (crimp stroke completed and clamp opens).

    Pipe dimensions (D52=Thickness, D53=Diameter, D54=Length) come from the merged JCO
    forming machine data (WELLSAWFRM_A1) that is outer-joined onto this device.
    Not the active version — detect_events_crimping_with_pipeout is used instead.
    """
    df[["D52", "D53", "D54"]] = df[["D52", "D53", "D54"]].ffill()  # Dimensions from JCO device — forward-fill
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"
 
    for i in range(1, len(df)):
        # --- Start Condition: D43 changes from 0 → 1 ---
        if not in_event:
            cond_start = df.loc[i - 1, "D43"] == 0 and df.loc[i, "D43"] == 1
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]
 
        # --- End Condition: D44 changes from 1 → 0 ---
        if in_event:
            cond_end = df.loc[i - 1, "D44"] == 0 and df.loc[i, "D44"] == 1
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Diameter": df.loc[i, "D53"] if "D53" in df.columns and not pd.isna(df.loc[i, "D53"]) else 0,
                    "Thickness": df.loc[i, "D52"] if "D52" in df.columns and not pd.isna(df.loc[i, "D52"]) else 0,
                    "Length": df.loc[i, "D54"] if "D54" in df.columns and not pd.isna(df.loc[i, "D54"]) else 0
                })
                in_event = False
                start_time = None
 
    # --- Handle case where event starts but never ends ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
 
    return pd.DataFrame(events)

def detect_events_crimping_with_pipeout(df):
    """
    Detect crimping cycles using consecutive D44 0→1 transitions as event boundaries.

    Rather than tracking start and end as separate signals, this version treats every
    D44 rising edge (pipe-out / crimp-complete) as BOTH the end of the previous cycle
    AND the start of the next one.  Pairs of consecutive rising edges bracket one cycle.

    This avoids false starts from D43 and handles back-to-back crimping without gaps.
    Also captures max values of D18 and D19 (station-specific force sensors) during
    each cycle window.

    Pipe dimensions come from the merged JCO device (D52/D53/D54 forward-filled).
    """
    df = df.copy()
    df[["D52", "D53", "D54"]] = df[["D52", "D53", "D54"]].ffill()

    events = []
    device_id = "WELLSAWCRMP_A1"

    # Collect all row indices where D44 transitions 0→1 (pipe-out signal)
    transition_indices = []

    for i in range(1, len(df)):
        if df.loc[i - 1, "D44"] == 0 and df.loc[i, "D44"] == 1:
            transition_indices.append(i)

    # Consecutive transition pairs define one complete crimp cycle
    for j in range(len(transition_indices) - 1):
        start_idx = transition_indices[j]
        end_idx = transition_indices[j + 1]
        event_slice = df.loc[start_idx:end_idx]  # All rows between the two transitions
        events.append({
            "device_id": device_id,
            "Start Time": df.loc[start_idx, "time"],
            "End Time": df.loc[end_idx, "time"],
            "Diameter": df.loc[end_idx, "D53"] if "D53" in df.columns and not pd.isna(df.loc[end_idx, "D53"]) else 0,
            "Thickness": df.loc[end_idx, "D52"] if "D52" in df.columns and not pd.isna(df.loc[end_idx, "D52"]) else 0,
            "Length": df.loc[end_idx, "D54"] if "D54" in df.columns and not pd.isna(df.loc[end_idx, "D54"]) else 0,
            "D18_max": event_slice["D18"].max() if "D18" in df.columns else None,
            "D19_max": event_slice["D19"].max() if "D19" in df.columns else None
        })

    return pd.DataFrame(events)

def detect_events_tack_welding(df):
    """
    Detect tack-welding cycles on the tack-welding station (WELLSAWTW_A1).

    Start: D17 goes 0→1 (welding arc on).
    End:   D17 goes 1→0 (welding arc off).

    After detecting all raw start/end pairs, the function enriches the events by
    querying WELLSAWTW_A2 (the companion device that stores operator-entered pipe
    specs: PipeID=D0, Length=D1, Diameter=D2, Thickness=D3, timestamp=D4).

    The pipe spec is joined using merge_asof (backward direction): for each cycle's
    end time, the most recent WELLSAWTW_A2 record ≤ end_time is matched.  If no
    match is found within the time range, the last known datapoint (get_dp) is used
    as a fallback and broadcast to all rows.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWTW_A1"
 
    for i in range(1, len(df)):
        # --- Start Condition: D43 changes from 0 → 1 ---
        if not in_event:
            cond_start = df.loc[i - 1, "D17"] == 0 and df.loc[i, "D17"] == 1
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]
 
        # --- End Condition: D44 changes from 1 → 0 ---
        if in_event:
            cond_end = df.loc[i - 1, "D17"] == 1 and df.loc[i, "D17"] == 0
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None
 
    # --- Handle case where event starts but never ends ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        print(events_df['Start Time'].iloc[0]) 
        last_dp = connect.data_query(device_id="WELLSAWTW_A2",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        print(last_dp)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWTW_A2",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
            events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill()    
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_ID1(df):
    """
    Detect internal-diameter (ID) inspection cycles for ID Machine 1 (WELLSAWID_CT).

    Input: merged DataFrame from WELLSAWID_A1 (boom sensor) + WELLSAWID_A5 (direction).
    Start: D41 (boom load) > 100 AND D19 (wagon speed) > 500.
    End:   D41 < 100 AND D19 < 50 (boom unloaded AND wagon stopped).

    After detection, pipe specs are joined from WELLSAWID_A7 (companion spec device
    with PipeID=D0, Length=D1, Diameter=D2, Thickness=D3, timestamp=D4) using the same
    merge_asof backward lookup used by tack_welding.  No cycle_time filter applied
    here (unlike ID2 which filters >300 s).
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWID_CT"

    for i in range(len(df)):
        # --- Start Condition ---
        if not in_event:
            # cond_start = df.loc[i, "D41"] > 100  # Boom Load > 100
            cond_start = (df.loc[i, "D41"] > 100) and (df.loc[i, "D19"] > 500)
            if cond_start:
                # print(1071)
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition ---
        if in_event:
            # cond_end = (df.loc[i, "D19"] > 500) and (df.loc[i, "D27"] == 1)  # Wagon Speed > 500 & Direction Reverse
            cond_end = (df.loc[i, "D41"] < 100) and (df.loc[i, "D19"] < 50)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    print(events_df)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        # events_df = events_df[events_df['cycle_time'] > 300.0]
        last_dp = connect.data_query(device_id="WELLSAWID_A7",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
            print(events_df)
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWID_A7",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
        events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill().bfill()    
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_ID2(df):
    """
    Detect ID inspection cycles for ID Machine 2 (WELLSAWID_A2CT).

    Identical start/end conditions to detect_events_ID1 (D41/D19 thresholds) but:
      - Sources data from WELLSAWID_A2 + WELLSAWID_A5 (D29 direction sensor).
      - Applies a cycle_time > 300 s filter (removes short traversals that are not full scans).
      - Joins pipe specs from WELLSAWID_A8 (the ID2 companion spec device).
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWID_CT"

    for i in range(len(df)):
        # --- Start Condition ---
        if not in_event:
            # cond_start = df.loc[i, "D41"] > 100  # Boom Load > 100
            cond_start = (df.loc[i, "D41"] > 100) and (df.loc[i, "D19"] > 500)
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition ---
        if in_event:
            # cond_end = (df.loc[i, "D19"] > 500) and (df.loc[i, "D29"] == 1)  # Wagon Speed > 500 & Direction Reverse
            cond_end = (df.loc[i, "D41"] < 100) and (df.loc[i, "D19"] < 50)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)   
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        events_df = events_df[events_df['cycle_time'] > 300.0]
        last_dp = connect.data_query(device_id="WELLSAWID_A8",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWID_A8",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
        events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill().bfill()   

        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_ID3(df):
    """
    Detect ID inspection cycles for ID Machine 3 (WELLSAWID_A3CT).

    Same logic as detect_events_ID2 but sources data from WELLSAWID_A3 + WELLSAWID_A6
    and joins specs from WELLSAWID_A9.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWID_CT"

    for i in range(len(df)):
        # --- Start Condition ---
        if not in_event:
            # cond_start = df.loc[i, "D41"] > 100  # Boom Load > 100
            cond_start = (df.loc[i, "D41"] > 100) and (df.loc[i, "D19"] > 500)
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition ---
        if in_event:
            # cond_end = (df.loc[i, "D19"] > 500) and (df.loc[i, "D27"] == 1)  # Wagon Speed > 500 & Direction Reverse
            cond_end = (df.loc[i, "D41"] < 100) and (df.loc[i, "D19"] < 50)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)
        if not events_df.empty:    
            events_df['cycle_time'] = events_df.apply(
                        lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                        if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                        else pd.NA,
                        axis=1
                    )
            end_time_new = events_df['End Time'].iloc[-1]
            if pd.isna(end_time_new):
                if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                    end_time_new = events_df['End Time'].iloc[-2]
                else:
                    # Use current IST time (UTC+5:30) without timezone awareness
                    end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

            # Convert to string (no timezone info)
            end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
            events_df = events_df[events_df['cycle_time'] > 300.0]
            last_dp = connect.data_query(device_id="WELLSAWID_A9",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
            if not last_dp.empty:
                last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
                events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
            else:
                # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
                last_dp = connect.get_dp(device_id="WELLSAWID_A9",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
                for col in ['D0', 'D1', 'D2', 'D3']:
                    events_df[col] = last_dp[col]
            events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill().bfill()    
            events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
            events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_OD1(df):
    """
    Detect OD (outer-diameter) inspection cycles for OD Machine 1 (legacy logic).

    Start: Wagon speed D19 > 500 AND direction flag D53 == 1 (forward traverse).
    End:   Limit switch D34 transitions 1→0 (end-of-stroke, wagon has reached the pipe end).

    After detection joins pipe specs from WELLSAWOD_A7 via merge_asof.
    Not the active version — detect_events_OD1_with_welding_on_new is used in mapping.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"

    for i in range(1, len(df)):
        # --- Start Condition: Wagon speed > 500 and direction forward ---
        if not in_event:
            cond_start = (df.loc[i, "D19"] > 500) and (df.loc[i, "D53"] == 1)
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition: Limit switch D34 goes from 1 → 0 ---
        if in_event:
            cond_end = (df.loc[i - 1, "D34"] == 1) and (df.loc[i, "D34"] == 0)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    events_df.to_csv("OD1.csv")
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A7",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A7",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
            events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill()    
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_OD2(df):
    """
    OD2 inspection cycle detection (legacy wagon-speed logic, not active in mapping).

    Start: D19 > 500 AND D55 == 1 (OD2 uses D55 direction instead of D53).
    End:   Limit switch D35 transitions 1→0.
    Specs joined from WELLSAWOD_A8.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"

    for i in range(1, len(df)):
        # --- Start Condition: Wagon speed > 500 and direction forward ---
        if not in_event:
            cond_start = (df.loc[i, "D19"] > 500) and (df.loc[i, "D55"] == 1)
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition: Limit switch D34 goes from 1 → 0 ---
        if in_event:
            cond_end = (df.loc[i - 1, "D35"] == 1) and (df.loc[i, "D35"] == 0)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A8",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A8",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            print(last_dp)
            for col in ['D0', 'D1', 'D2', 'D3']:
                if col not in last_dp.columns:
                    events_df[col] = np.nan
                else:    
                    events_df[col] = last_dp[col]
            events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill()        
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_OD3(df):
    """
    OD3 inspection cycle detection (legacy wagon-speed logic, not active in mapping).

    Start: D19 > 500 AND D53 == 1.
    End:   Limit switch D36 transitions 1→0 (OD3 uses D36 as its end limit).
    Specs joined from WELLSAWOD_A9.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"

    for i in range(1, len(df)):
        # --- Start Condition: Wagon speed > 500 and direction forward ---
        if not in_event:
            cond_start = (df.loc[i, "D19"] > 500) and (df.loc[i, "D53"] == 1)
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition: Limit switch D34 goes from 1 → 0 ---
        if in_event:
            cond_end = (df.loc[i - 1, "D36"] == 1) and (df.loc[i, "D36"] == 0)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A9",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A9",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
            events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill()    
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_RPEMS(df):
    """
    Detect RPEMS (Radiographic/Process Event Management System) processing events.

    RPEMS logs a record each time a pipe enters the radiographic inspection station.
    Unlike the sensor-based machines, this station does not have a clear start/end
    signal — instead, the gap between consecutive log entries indicates whether two
    records belong to the same continuous batch.

    Logic:
      - Compute the time gap (in minutes) between each row and the next (shift(-1)).
      - Keep only rows where the gap is ≤ 20 minutes (rows further apart are separate batches).
      - Treat each kept row's timestamp as Start Time and its next timestamp as End Time.
      - PipeID comes directly from D0 (written by the RPEMS system, not generated here).

    Note: This approach does NOT use the standard in_event state machine pattern.
    """
    df['time'] = pd.to_datetime(df['time'])
    df['next_time'] = df['time'].shift(-1)                                          # Timestamp of the following record
    df['diff'] = (df['next_time'] - df['time']).dt.total_seconds() / 60            # Gap in minutes

    filtered = df[df['diff'] <= 20].copy()  # Discard gaps > 20 min (different inspection batches)
    filtered.to_csv("907.csv")              # Debug export — kept for inspection

    events_df = filtered[['time', 'next_time', 'D0']].rename(
        columns={'time': 'Start Time', 'next_time': 'End Time', 'D0': 'PipeID'}
    )

    return events_df

def detect_events_IUT(df):
    """
    Detect Internal Ultrasonic Testing (IUT) cycles (WELLSAWIUT_A3).

    Start: D7 goes 0→1 (probe in / test starts) AND D6 == 0 (pipe not yet flagged as tested).
    End:   D7 goes 1→0 AND D6 == 1 (test complete, pipe marked as done).

    The D6 check prevents false starts when the probe re-enters a pipe that has already
    been tested (D6 would be 1, so the start condition is not met again).
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"

    for i in range(1, len(df)):
        # --- Start Condition: D7 changes from 0 → 1 and D6 == 0 ---
        if not in_event:
            cond_start = (
                df.loc[i - 1, "D7"] == 0 and
                df.loc[i, "D7"] == 1 and
                df.loc[i, "D6"] == 0
            )
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition: D7 changes from 1 → 0 and D6 == 1 ---
        if in_event:
            cond_end = (
                df.loc[i - 1, "D7"] == 1 and
                df.loc[i, "D7"] == 0 and
                df.loc[i, "D6"] == 1
            )
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                })
                in_event = False
                start_time = None

    # --- Handle case where event starts but never ends ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })

    return pd.DataFrame(events)

def detect_events_FUT(df):
    """
    Detect Full Ultrasonic Testing (FUT) cycles (WELLSAWFUT_A3).

    Uses D9/D8 sensor pair instead of D7/D6, but identical logic to detect_events_IUT:
    Start: D9 goes 0→1 AND D8 == 0.
    End:   D9 goes 1→0 AND D8 == 1.

    Also used for PUT (Pressure/Ultrasonic Testing) — see the PUT entry in mapping.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"

    for i in range(1, len(df)):
        # --- Start Condition: D7 changes from 0 → 1 and D6 == 0 ---
        if not in_event:
            cond_start = (
                df.loc[i - 1, "D9"] == 0 and
                df.loc[i, "D9"] == 1 and
                df.loc[i, "D8"] == 0
            )
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition: D7 changes from 1 → 0 and D6 == 1 ---
        if in_event:
            cond_end = (
                df.loc[i - 1, "D9"] == 1 and
                df.loc[i, "D9"] == 0 and
                df.loc[i, "D8"] == 1
            )
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time,
                })
                in_event = False
                start_time = None

    # --- Handle case where event starts but never ends ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })

    return pd.DataFrame(events)

def detect_events_OD1_with_welding_on(df):
    """
    OD1 inspection cycle detection using the welding-arc-on signal D26 (original version).

    Start: D26 (welding arc status on OD1 weld machine) goes 0→1.
    End:   Limit switch D34 transitions 1→0 (wagon reached end of pipe).
    Specs joined from WELLSAWOD_A7 via merge_asof.

    Not the active version — detect_events_OD1_with_welding_on_new is used in mapping.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"

    for i in range(1, len(df)):
        prev = df.loc[i - 1]
        curr = df.loc[i]
        # --- Start Condition: Welding arc signal D26 rising edge ---
        if not in_event:
            cond_start = (prev["D26"] == 0 and curr["D26"] == 1)
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition: Limit switch D34 goes from 1 → 0 ---
        if in_event:
            cond_end = (df.loc[i - 1, "D34"] == 1) and (df.loc[i, "D34"] == 0)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        print(events_df)
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A7",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A7",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
            events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill()    
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_OD1_with_welding_on_new(df):
    """
    Active OD1 inspection cycle detection using consecutive D26 rising edges.

    Instead of tracking a start and an independent end condition, this function
    treats every D26 0→1 transition (welding arc fires) as BOTH the end of the
    previous cycle AND the start of the next, identical to the crimping_with_pipeout
    approach.  Consecutive rising-edge pairs define one complete weld pass.

    Specs joined from WELLSAWOD_A7 via merge_asof after event list is built.
    """
    events = []
    device_id = "WELLSAWCRMP_A1"

    # Collect every row index where D26 transitions 0→1 (arc-on signal)
    transition_indices = []

    for i in range(1, len(df)):
        if df.loc[i - 1, "D26"] == 0 and df.loc[i, "D26"] == 1:
            transition_indices.append(i)

    # Consecutive transition pairs form one complete weld pass (pipe inspection cycle)
    for j in range(len(transition_indices) - 1):
        start_idx = transition_indices[j]
        end_idx = transition_indices[j + 1]

        events.append({
            "device_id": device_id,
            "Start Time": df.loc[start_idx, "time"],
            "End Time": df.loc[end_idx, "time"]
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A7",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A7",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
        events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill().bfill()    
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_OD2_with_welding_on_new(df):
    """
    Active OD2 inspection cycle detection — same consecutive D26 rising-edge approach as OD1_new.
    Sources data from WELLSAWOD_A2 and joins specs from WELLSAWOD_A8.
    """
    events = []
    device_id = "WELLSAWCRMP_A1"

    transition_indices = []

    for i in range(1, len(df)):
        if df.loc[i - 1, "D26"] == 0 and df.loc[i, "D26"] == 1:
            transition_indices.append(i)

    # Consecutive transition pairs form one complete weld pass
    for j in range(len(transition_indices) - 1):
        start_idx = transition_indices[j]
        end_idx = transition_indices[j + 1]

        events.append({
            "device_id": device_id,
            "Start Time": df.loc[start_idx, "time"],
            "End Time": df.loc[end_idx, "time"]
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A8",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A8",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            print(last_dp)
            for col in ['D0', 'D1', 'D2', 'D3']:
                if col not in last_dp.columns:
                    events_df[col] = np.nan
                else:    
                    events_df[col] = last_dp[col]
        events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill().bfill()        
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_OD3_with_welding_on_new(df):
    """
    Active OD3 inspection cycle detection — same consecutive D26 rising-edge approach as OD1_new.
    Sources data from WELLSAWOD_A3 and joins specs from WELLSAWOD_A9.
    """
    events = []
    device_id = "WELLSAWCRMP_A1"

    transition_indices = []

    for i in range(1, len(df)):
        if df.loc[i - 1, "D26"] == 0 and df.loc[i, "D26"] == 1:
            transition_indices.append(i)

    # Consecutive transition pairs form one complete weld pass
    for j in range(len(transition_indices) - 1):
        start_idx = transition_indices[j]
        end_idx = transition_indices[j + 1]

        events.append({
            "device_id": device_id,
            "Start Time": df.loc[start_idx, "time"],
            "End Time": df.loc[end_idx, "time"]
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A9",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A9",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
        events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill().bfill()    
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_OD2_with_welding_on(df):
    """
    OD2 cycle detection using D26 arc-on start + D35 limit-switch end (original version).

    Not the active version — detect_events_OD2_with_welding_on_new is used in mapping.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"

    for i in range(1, len(df)):
        prev = df.loc[i - 1]
        curr = df.loc[i]
        # --- Start Condition: Welding arc signal D26 rising edge ---
        if not in_event:
            cond_start = (prev["D26"] == 0 and curr["D26"] == 1)
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition: Limit switch D34 goes from 1 → 0 ---
        if in_event:
            cond_end = (df.loc[i - 1, "D35"] == 1) and (df.loc[i, "D35"] == 0)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A8",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A8",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            print(last_dp)
            for col in ['D0', 'D1', 'D2', 'D3']:
                if col not in last_dp.columns:
                    events_df[col] = np.nan
                else:    
                    events_df[col] = last_dp[col]
            events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill()        
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def detect_events_OD3_with_welding_on(df):
    """
    OD3 cycle detection using D26 arc-on start + D36 limit-switch end (original version).

    Not the active version — detect_events_OD3_with_welding_on_new is used in mapping.
    """
    events = []
    in_event = False
    start_time = None
    device_id = "WELLSAWCRMP_A1"

    for i in range(1, len(df)):
        prev = df.loc[i - 1]
        curr = df.loc[i]
        # --- Start Condition: Welding arc signal D26 rising edge ---
        if not in_event:
            cond_start = (prev["D26"] == 0 and curr["D26"] == 1)
            if cond_start:
                in_event = True
                start_time = df.loc[i, "time"]

        # --- End Condition: Limit switch D34 goes from 1 → 0 ---
        if in_event:
            cond_end = (df.loc[i - 1, "D36"] == 1) and (df.loc[i, "D36"] == 0)
            if cond_end:
                end_time = df.loc[i, "time"]
                events.append({
                    "device_id": device_id,
                    "Start Time": start_time,
                    "End Time": end_time
                })
                in_event = False
                start_time = None

    # --- Handle event that started but never ended ---
    if in_event:
        events.append({
            "device_id": device_id,
            "Start Time": start_time,
            "End Time": None
        })
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df.dropna(subset=['End Time'], inplace=True)    
        events_df['cycle_time'] = events_df.apply(
                    lambda row: (row['End Time'] - row['Start Time']).total_seconds()
                    if pd.notnull(row['End Time']) and pd.notnull(row['Start Time'])
                    else pd.NA,
                    axis=1
                )
        end_time_new = events_df['End Time'].iloc[-1]
        if pd.isna(end_time_new):
            if len(events_df) > 1 and not pd.isna(events_df['End Time'].iloc[-2]):
                end_time_new = events_df['End Time'].iloc[-2]
            else:
                # Use current IST time (UTC+5:30) without timezone awareness
                end_time_new = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Convert to string (no timezone info)
        end_time_new_str = end_time_new.strftime("%Y-%m-%d %H:%M:%S")
        last_dp = connect.data_query(device_id="WELLSAWOD_A9",sensor_list=["D0","D1","D2","D3","D4","D5"],start_time=events_df['Start Time'].iloc[0],end_time=end_time_new_str,cal=True,alias=False)
        if not last_dp.empty:
            last_dp['D4'] = pd.to_datetime(last_dp['D4']).dt.tz_localize('Asia/Kolkata')
            events_df =pd.merge_asof(events_df,last_dp,left_on="End Time",right_on="D4",direction="backward")
        else:
            # end_time = datetime.strftime(events_df['End Time'].iloc[-1], "%Y-%m-%d %H:%M:%S")
            last_dp = connect.get_dp(device_id="WELLSAWOD_A9",sensor_list=["D0","D1","D2","D3","D4","D5"],end_time=end_time_new_str)
            for col in ['D0', 'D1', 'D2', 'D3']:
                events_df[col] = last_dp[col]
            events_df[['D0', 'D1', 'D2', 'D3']] = events_df[['D0', 'D1', 'D2', 'D3']].ffill()    
        events_df = events_df.rename(columns={'D0': 'PipeID', 'D1': 'Length','D2': 'Diameter', 'D3': 'Thickness'})
        events_df = events_df.dropna(subset=['PipeID'])
    return events_df

def calculate_energy(start_time, end_time, device):
    """
    Calculate energy consumed (kWh) by a station's energy meter for one pipe cycle.

    Queries the InfluxDB consumption endpoint which returns the cumulative kWh counter
    for sensor D16 (energy meter total).  The diff().iloc[-1] trick takes the difference
    between the last two readings in the window, giving the incremental energy consumed
    during the cycle rather than the absolute counter value.

    Returns 0.0 if the energy meter has no data for the requested window (e.g. meter
    offline or cycle outside the available history).

    Args:
        start_time: Cycle start timestamp (datetime or IST string).
        end_time: Cycle end timestamp (datetime or IST string).
        device: Energy meter InfluxDB device ID (e.g. "WELLSAWFRM_EM1").

    Returns:
        float: kWh consumed during [start_time, end_time], or 0.0 on no data.
    """
    consumption_df = connect.consumption(
        device_id=device, start_time=str(start_time), end_time=str(end_time), sensor="D16"
    )
    if consumption_df is None or consumption_df.empty:
        return 0.0
    # diff() gives per-row increments; iloc[-1] is the last increment = total change over the window
    energy_consumption = consumption_df['D16'].diff().iloc[-1]

    return energy_consumption

# ---------------------------------------------------------------------------
# Station configuration mapping
#
# Each key is a station name used in main.py to iterate the pipeline.
# Each value is a dict with:
#   Energy_Device   — InfluxDB energy meter device ID for kWh calculation
#   MONGO_DEVICE_ID — MongoDB device ID where processed events are stored
#   INFLUX_DEVICE_ID — Either a single device ID string, or a dict of
#                      {device_id: [sensor_list]} for multi-device stations
#   SENSOR_LIST     — Sensor list (used only when INFLUX_DEVICE_ID is a string)
#   Function        — The detect_events_* function to call for this station
# ---------------------------------------------------------------------------
mapping = {
    "JCO": {"Energy_Device": "WELLSAWFRM_EM1",
        "MONGO_DEVICE_ID":"WELLSAWFRM_CT",
        "INFLUX_DEVICE_ID":"WELLSAWFRM_A1",
        "SENSOR_LIST":['D104','D100','D101','D53','D52','D54','D56','D62',"D68","D60","D6","D77","D3","D58","D64"],
        "Function": detect_events_JCO_new_with_D64},
 
        "Expander_1":{"Energy_Device": "WELLSAWEXP1_EM1",
        "MONGO_DEVICE_ID":"WELLSAWEXP1_CT",
        "INFLUX_DEVICE_ID":"WELLSAWEXP1_A1",
        "SENSOR_LIST":["D58","D79","D82","D70","D71","D72","D73","D55"],
        "Function": detect_events_expander_1},
        
        "Expander_2":{"Energy_Device": "WELLSAWEXP2_EM1",
        "MONGO_DEVICE_ID":"WELLSAWEXP2_CT",
        "INFLUX_DEVICE_ID":"WELLSAWEXP2_A1",
        "SENSOR_LIST":["D58","D79","D82","D70","D71","D72","D73","D55"],
        "Function": detect_events_expander_2},

        "Hydrotester":{"Energy_Device": "WELLSAWHDRT_EM1",
        "MONGO_DEVICE_ID":"WELLSAWHDRT_CT",
        "INFLUX_DEVICE_ID":"WELLSAWHDRT_A1",
        "SENSOR_LIST":["D59","D64","D63","D66","D65","D67"],
        "Function": detect_events_hydro},

        # "Crimping":{"Energy_Device": "WELLSAWCRMP_EM1",
        # "MONGO_DEVICE_ID":"WELLSAWCRMP_CT",
        # "INFLUX_DEVICE_ID":"WELLSAWCRMP_A1",
        # "SENSOR_LIST":["D43","D44"],
        # "Function": detect_events_crimping},

        "Crimping":{"Energy_Device": "WELLSAWCRMP_EM1",
        "MONGO_DEVICE_ID":"WELLSAWCRMP_CT",
        "INFLUX_DEVICE_ID":{
                        "WELLSAWCRMP_A1": ["D43","D44","D18","D19"],
                        "WELLSAWFRM_A1": ['D53','D52','D54']
                    },
        "SENSOR_LIST":["D43","D44"],
        "Function": detect_events_crimping_with_pipeout},

        "tack_welding":{"Energy_Device": "WELLSAWTCW_EM1",
        "MONGO_DEVICE_ID":"WELLSAWTW_CT",
        "INFLUX_DEVICE_ID":"WELLSAWTW_A1",
        "SENSOR_LIST":["D17"],
        "Function": detect_events_tack_welding},

        "OD1":{"Energy_Device": "WELLSAWOD_EM1",
        "MONGO_DEVICE_ID":"WELLSAWOD_A1CT",
        "INFLUX_DEVICE_ID": {
                        "WELLSAWOD_A1": ["D26"],
                        # "WELLSAWOD_A5": ["D53"],
                        # "WELLSAWPF_A1": ["D34"]
                    },
        "SENSOR_LIST":["D17"],
        "Function": detect_events_OD1_with_welding_on_new},

        "OD2":{"Energy_Device": "WELLSAWOD_EM1",
        "MONGO_DEVICE_ID":"WELLSAWOD_A2CT",
        "INFLUX_DEVICE_ID": {
                        "WELLSAWOD_A2": ["D26"],
                        # "WELLSAWOD_A5": ["D55"],
                        # "WELLSAWPF_A1": ["D35"]
                    },
        "SENSOR_LIST":["D17"],
        "Function": detect_events_OD2_with_welding_on_new},

        "OD3":{"Energy_Device": "WELLSAWOD_EM2",
        "MONGO_DEVICE_ID":"WELLSAWOD_A3CT",
        "INFLUX_DEVICE_ID": {
                        "WELLSAWOD_A3": ["D26"],
                        # "WELLSAWOD_A6": ["D53"],
                        # "WELLSAWPF_A1": ["D36"]
                    },
        "SENSOR_LIST":["D17"],
        "Function": detect_events_OD3_with_welding_on_new},

        "RPEMS":{"Energy_Device": "WELLSAWRPEMS_EM1",
        "MONGO_DEVICE_ID":"WELLSAWRPEMS_CT",
        "INFLUX_DEVICE_ID": 'WELLSAWRPEMS_A1',
        "SENSOR_LIST":["D0"],
        "Function": detect_events_RPEMS},

        "ID1":{"Energy_Device": "WELLSAWID_EM1",
        "MONGO_DEVICE_ID":"WELLSAWID_CT",
        "INFLUX_DEVICE_ID": {
                        "WELLSAWID_A1": ["D41","D19"],
                        "WELLSAWID_A5": ["D27"]
                    },
        "SENSOR_LIST":["D17"],
        "Function": detect_events_ID1},

        "ID2":{"Energy_Device": "WELLSAWID_EM1",
        "MONGO_DEVICE_ID":"WELLSAWID_A2CT",
        "INFLUX_DEVICE_ID": {
                        "WELLSAWID_A2": ["D41","D19"],
                        "WELLSAWID_A5": ["D29"]
                    },
        "SENSOR_LIST":["D17"],
        "Function": detect_events_ID2},

        "ID3":{"Energy_Device": "WELLSAWID_EM2",
        "MONGO_DEVICE_ID":"WELLSAWID_A3CT",
        "INFLUX_DEVICE_ID": {
                        "WELLSAWID_A3": ["D41","D19"],
                        "WELLSAWID_A6": ["D27"]
                    },
        "SENSOR_LIST":["D17"],
        "Function": detect_events_ID3},

        "JCO_new" :{"Energy_Device": "WELLSAWFRM_EM1",
        "MONGO_DEVICE_ID":"WELLSAWFRM_CT_Test",
        "INFLUX_DEVICE_ID":"WELLSAWFRM_A1",
        "SENSOR_LIST":['D104','D100','D101','D53','D52','D54','D56','D62',"D68","D60","D58","D64",'D6', 'D77', 'D3'],
        "Function": detect_events_JCO_new},

        "IUT":{"Energy_Device": "WELLSAWIUT_EM1",
        "MONGO_DEVICE_ID":"WELLLSAWIUT_CT",
        "INFLUX_DEVICE_ID": 'WELLSAWIUT_A3',
        "SENSOR_LIST":["D6","D7"],
        "Function": detect_events_IUT},

        "FUT":{"Energy_Device": "WELLSAWFUT_EM1",
        "MONGO_DEVICE_ID":"WELLLSAWFUT_CT",
        "INFLUX_DEVICE_ID": 'WELLSAWFUT_A3',
        "SENSOR_LIST":["D8","D9"],
        "Function": detect_events_FUT},

        "PUT":{"Energy_Device": "WELLSAWPUT_EM1",
        "MONGO_DEVICE_ID":"WELLSAWPUT_CT",
        "INFLUX_DEVICE_ID": 'WELLSAWPUT_A2',
        "SENSOR_LIST":["D8","D9"],
        "Function": detect_events_FUT},

        # "OD1_old_logic":{"Energy_Device": "WELLSAWOD_EM1",
        # "MONGO_DEVICE_ID":"WELLSAWOD_A1CT_test",
        # "INFLUX_DEVICE_ID": {
        #                 "WELLSAWOD_A1": ["D19"],
        #                 "WELLSAWOD_A5": ["D53"],
        #                 "WELLSAWPF_A1": ["D34"]
        #             },
        # "SENSOR_LIST":["D17"],
        # "Function": detect_events_OD1},

        "Expander_1_test":{"Energy_Device": "WELLSAWEXP2_EM1",
        "MONGO_DEVICE_ID":"WELLSAWEXP1_CT_TEST",
        "INFLUX_DEVICE_ID":"WELLSAWEXP1_A1",
        "SENSOR_LIST":["D58","D79","D82","D70","D71","D72","D73","D55"],
        "Function": detect_events_expander_1_test},

        }

def fetch_and_merge_devices(
    connect,
    device_ids,
    start_time,
    end_time,
    merge_on="time",
    how="outer"
):
    """
    Fetch recipe sensor data from multiple PLCs and merge them into one DataFrame.

    Used exclusively by pipe_recipe.py to combine data from three JCO press devices:
      WELLSAWFRM_A3 — recipe parameter PLC (sword positions, angles, pressures)
      WELLSAWFRM_A4 — recipe-save trigger (RECEIPE SAVED D48)
      WELLSAWFRM_A1 — pipe dimension inputs (D53=diameter, D54=length, D57=thickness)

    Steps:
      1. Query each device with aliased column names (alias=True so human-readable names
         like "PLATE WIDTH (D4)" come through instead of raw D4).
      2. Resample each device to 5-minute buckets using "last" value for most sensors,
         and "max" for RECEIPE SAVED (D48) on WELLSAWFRM_A4 so a save event within the
         5-minute window is not lost if it doesn't land exactly on the bucket boundary.
      3. Outer-join all resampled DataFrames on the 'time' column.
      4. Drop duplicate status/RSSI columns that appear from multiple devices.
      5. Filter to rows where RECEIPE SAVED (D48) == 1 so only confirmed recipe saves
         are published.

    Args:
        connect: io_connect.DataAccess instance.
        device_ids: List of InfluxDB device IDs to query and merge.
        start_time: Query window start (IST string).
        end_time: Query window end (IST string).
        merge_on: Column to merge on (always "time").
        how: pandas merge type — "outer" to keep all timestamps, "inner" for intersect.

    Returns:
        Merged DataFrame filtered to recipe-save rows, or empty DataFrame if no saves.
    """
    dfs = []

    for device_id in device_ids:
        sensor_list = None
        if device_id == "WELLSAWFRM_A1":
            # Only need dimensions from A1 — pulling all sensors would add too many columns
            sensor_list = ['D53', 'D54', 'D57']

        df = connect.data_query(
            device_id=device_id,
            sensor_list=sensor_list,
            cal=True,
            alias=True,  # Use human-readable column names (e.g. "PLATE WIDTH (D4)")
            start_time=start_time,
            end_time=end_time
        ).copy()

        df[merge_on] = pd.to_datetime(df[merge_on])
        df = df.sort_values(merge_on).set_index(merge_on)

        # Build per-column aggregation: max for the recipe-save flag (to catch events
        # that fall within a 5-minute bucket); "last" for everything else
        agg_dict = {
            col: (
                "max"
                if device_id == "WELLSAWFRM_A4" and col == "RECEIPE SAVED (D48)"
                else "last"
            )
            for col in df.columns
        }

        df = df.resample("5min").agg(agg_dict).reset_index()  # 5-minute grid alignment

        dfs.append(df)

    # Sequentially outer-join all device DataFrames on the time column
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=merge_on, how=how)

    # Drop duplicate metadata columns that appear under _x/_y suffixes after the merge
    cols_to_drop = [
        'Network Strength (RSSI)_y',
        'Status (Status)_y',
        'Network Strength (RSSI)_x',
        'Status (Status)_x',
    ]
    merged_df = merged_df.drop(
        columns=[c for c in cols_to_drop if c in merged_df.columns],
        errors="ignore"
    )

    # Keep only rows where the operator explicitly saved a recipe (D48 == 1)
    if "RECEIPE SAVED (D48)" in merged_df.columns:
        merged_df = merged_df[merged_df["RECEIPE SAVED (D48)"] == 1.0]

    return merged_df

def assign_recipe_id(df_old, df_new):
    """
    Assign or reuse RECIPE_IDs for new recipe rows.

    Rules:
      - If df_old is empty (first run): generate a new RECIPE_ID for every row
        as "YYYYMMDD_HHMMSS_<user>_<client>".
      - If df_old has data: left-join df_new onto df_old on (USER NAME, CLIENT NAME).
        Rows where the combination already exists in df_old get the existing RECIPE_ID;
        rows with no match get a newly generated ID.

    Args:
        df_old: Previously stored recipe DataFrame (empty on first run).
        df_new: Freshly fetched and merged recipe DataFrame to be published.

    Returns:
        df_new enriched with a RECIPE_ID column.
    """
    compare_cols = ["USER NAME (D0)", "CLIENT NAME (D1)"]  # Columns used to identify a unique recipe

    df_new = df_new.copy()
    df_new["time"] = pd.to_datetime(df_new["time"])

    # Case 1: No previous data — generate fresh IDs for everything
    if df_old is None or df_old.empty:
        df_new["RECIPE_ID"] = (
            df_new["time"].dt.strftime("%Y%m%d_%H%M%S")
            + "_" + df_new["USER NAME (D0)"].astype(str)
            + "_" + df_new["CLIENT NAME (D1)"].astype(str)
        )
        return df_new

    # Case 2: Previous data exists — reuse existing IDs where user+client match
    df_result = pd.merge(
        df_new,
        df_old[compare_cols + ["RECIPE_ID"]],  # Only bring in the ID column from old data
        on=compare_cols,
        how="left"  # Keep all new rows; unmatched rows get NaN in RECIPE_ID
    )

    # For rows with no match (NaN RECIPE_ID), generate a new unique ID
    mask = df_result["RECIPE_ID"].isna()
    df_result.loc[mask, "RECIPE_ID"] = (
        df_result.loc[mask, "time"].dt.strftime("%Y%m%d_%H%M%S")
        + "_" + df_new["USER NAME (D0)"].astype(str)
        + "_" + df_result.loc[mask, "CLIENT NAME (D1)"].astype(str)
    )

    return df_result

def clean_column_names(df):
    """
    Removes '(Dxxx)' patterns from column names.
    
    Example:
        'USER NAME (D0)' → 'USER NAME'
        'PLATE WIDTH (D10)' → 'PLATE WIDTH'
    """
    df = df.copy()
    df.columns = [re.sub(r"\s*\(D\d+\)", "", col) for col in df.columns]
    return df