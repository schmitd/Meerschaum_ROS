#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Example script for syncing NOAA weather data
"""

### NOTE: this adds the parent Meerschaum directory to PATH to import the development version
### instead of the system version.
###
### If you are running this on Windows or don't have the development version cloned,
### simply comment out the below line.
import sys; sys.path.insert(0, '../../')

import meerschaum as mrsm
from meerschaum.utils.misc import parse_df_datetimes
from meerschaum.utils.warnings import warn
from meerschaum.utils.pool import get_pool

import requests, pandas as pd, json, pytz, sys, argparse
parser = argparse.ArgumentParser(description="Sync weather station data into a Pipe")
parser.add_argument('-d', '--debug', action='store_true')
parser.add_argument('-I', '--mrsm-instance', type=str)
args = parser.parse_args()

def main():
    global args
    debug = args.debug
    ### NOTE: You may either use a SQL connector or an API connector for the Meerschaum instance connector.
    ###       If the instance is not specified when creating the Pipe, then the configured instance
    ###       connector will be used. See `mrsm edit config` to change your default instance connector.

    ### NOTE: For some reason, SQL connectors break Pool but not ThreadPool.
    ###       API connectors can handle either Pool or ThreadPool (tested with gevent as well)
    #  conn = mrsm.get_connector('api', 'mrsm', host="mrsm.io")
    pipe = mrsm.Pipe('noaa', 'weather', mrsm_instance=args.mrsm_instance)

    ### Specify the columns in case Pipe is not registered
    pipe.columns = {
        "datetime" : "timestamp",
        "id" : "station",
    }

    ### NOAA weather stations
    ### NOTE: the Pipe's columns' types are determined by the first dataframe encountered
    stations = {
        'KATL' : 'Atlanta',
        'KGGE' : 'Georgetown',
        'KCUB' : 'Columbia',
        'KCHS' : 'Charleston',
        'KCLT' : 'Charlotte',
        'KCEU' : 'Clemson',
    }
    ### fetch data from the stations
    pool = get_pool('ThreadPool')
    args = [(stationID, location, pipe) for stationID, location in stations.items()]
    dataframes = dict(pool.starmap(do_fetch, args))
    pool.close()
    pool.join()

    ### only keep the common columns (skipping empty dataframes)
    common_cols = None
    for stationID, df in dataframes.items():
        if df is None: continue
        if len(df.columns) == 0: continue
        if common_cols is None:
            common_cols = set(df.columns)
            continue
        try:
            common_cols = common_cols & set(df.columns)
        except Exception as e:
            warn(str(e))
    ### in case all dataframes are empty
    if common_cols is None: common_cols = set()

    ### will cast all but these to floats
    non_float_cols = {'label', 'timestamp', 'station', 'location'}
    float_cols = common_cols - non_float_cols

    ### pipe.sync returns a tuple of success bool and message. E.g. (True, "Success") or (False, "Error message")
    _dataframes = dict()
    for stationID, df in dataframes.items():
        if df is not None:
            try:
                df = df[common_cols]
                df[list(float_cols)] = df[list(float_cols)].astype('float')
            except Exception as e:
                warn(str(e))
                df = None
            _dataframes[stationID] = df
    dataframes = _dataframes

    ### Make sure Pipe exists.
    ### Normally this is handled when syncing for the first time, but threading breaks things.
    if not pipe.exists(debug=debug):
        for stationID, df in dataframes.items():
            if df is not None:
                if len(df) > 0:
                    pipe.sync(df.head(1), force=True, debug=debug)
                    break

    ### finally, time to sync the dataframes
    success_dict = dict()
    for stationID, df in dataframes.items():
        success = pipe.sync(
            df,
            blocking = False,
            check_existing = True,
            force = True,
            debug = debug
        )[0] if df is not None else False
        success_dict[stationID] = success

    for stationID, success in success_dict.items():
        if not success: print(f"Failed to sync from station '{stationID}' ({stations[stationID]})")

def do_fetch(stationID, location, pipe):
    """
    Wrapper for fetch_station_data (below)
    """
    try:
        df = fetch_station_data(stationID, location, pipe)
    except Exception as e:
        msg = str(e)
        warn(f"Failed to sync station '{stationID}' ({location}). Error:\n{msg}")
        df = None

    return stationID, df

def fetch_station_data(stationID : str, location : str, pipe : mrsm.Pipe):
    """
    Sync a weather station's data into a Meerschaum Pipe.
    """
    ### Get the latest sync time for this station so we don't request duplicate data.
    try:
        start = pipe.get_sync_time(
            {"station" : stationID}
        ).astimezone(
            pytz.timezone('UTC')
        ).isoformat()
    except Exception as e:
        start = None

    ### fetch JSON from NOAA since the start time (sync_time for this stationID)
    if start: print(f"Fetching data newer than {start} for station '{stationID}' ({location})...", flush=True)
    else: print(f"Fetching all possible data for station '{stationID}' ({location})...", flush=True)
    url = f"https://api.weather.gov/stations/{stationID}/observations/"
    try:
        data = json.loads(requests.get(url, params={"start":start}).text)
    except Exception as e:
        print(f"\nError: {e}", flush=True)
        return stationID, None
    print(f"Done fetching data for station '{stationID}' ({location}).", flush=True)

    ### build a dictionary from the JSON response (flattens JSON)
    d = dict()
    if 'features' not in data:
        print(data)
        return None
    for record in data['features']:
        properties = record['properties']

        if 'location' not in d: d['location'] = []
        d['location'].append(location)

        for col, v in properties.items():
            ### specific to this API; filter out features we don't want.
            if not v: continue
            ### at this point, the timestamp is a string. It will get casted below in `parse_df_datetimes`
            if col == 'timestamp': val = v
            ### we could just use the stationID provided, but it's given in the JSON so we might as well use it
            elif col == 'station': val = v.split('/')[-1]

            ### skip features that don't contain a simple 'value' key.
            ### NOTE: this will need to be tweaked if we want more information that doesn't apply here.
            elif not isinstance(v, dict): continue
            elif 'value' not in v: continue
            else: val = v['value']

            ### if possible, append units to column name.
            if isinstance(v, dict):
                if 'unitCode' in v:
                    col += " (" + v['unitCode'].replace('unit:', '') + ")"

            ### grow the lists in the dictionary.
            ### e.g. { 'col1' : [ 1, 2, 3 ], 'col2' : [ 4, 5, 6 ] }
            if col not in d: d[col] = []
            d[col].append(val)

    ### create a pandas DataFrame from the dictionary and parse for datetimes
    return parse_df_datetimes(pd.DataFrame(d))

if __name__ == "__main__": main()
