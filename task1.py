

import pandas as pd
from sqlalchemy import create_engine, text
# openpyxl and pymssql also needed

server = '@10.156.101.181:1433'
database = '/STG' 
username = 'py_user' 
password = 'Mycomplexpassword^1' 


def connect_to_xls(path, sheet_name, skip_rows, columns_names, dtypes):
    try:
        mtc_df = pd.read_excel(path, sheet_name=sheet_name, skiprows=skip_rows, header=None,
                               names=columns_names, dtype=dtypes)
    except Exception as e:
        print('Failed to read xls file')
        raise e

    return mtc_df


def load_into_df():
    columns = [
        'action_indicator_ad',
        'station_arr',
        'station_dep',
        'connection_status',
        'time_hhmm',
        'arrival_carrier',
        'arrival_carrier_code_indicator',
        'arrival_carrier_code_operator',
        'arrival_flight_num_rng_start',
        'arrival_flight_num_rng_end',
        'departure_carrier',
        'departure_carrier_code_indicator',
        'departure_carrier_code_operator',
        'departure_flight_num_rng_start',
        'departure_flight_num_rng_end',
        'terminal_arr',
        'terminal_dep',
        'station_prev',
        'station_next',
        'state_prev',
        'state_next',
        'country_prev',
        'country_next',
        'region_prev',
        'region_next',
        'aircraft_type_arr',
        'aircraft_type_dep',
        'aircraft_body_wn_arr',
        'aircraft_body_wn_dep',
        'suppression_indicator_yn',
        'suppression_region',
        'suppression_country',
        'suppression_state',
        'date_effective_from',
        'date_effective_to',
        'info_filing_date',
        'info_submitting_carrier'
    ]
    dtypes = {col: str for col in columns}
    path = 'src/EI-Report-202109131630.xlsx'
    sheet_name = 'OAG'
    skip_rows = 2
    mtc_df = connect_to_xls(path, sheet_name, skip_rows, columns, dtypes)

    return mtc_df


def load_into_db(server,database,username,password):

    mtc_df = load_into_df()

    engine = create_engine('mssql+pymssql://' + username + ':' + password + server + database)

    try:

        row_inserted = mtc_df.to_sql('mct_xls', engine, schema = 'land', if_exists = 'append', index = False)
        print('row_inserted: ' + row_inserted)

    except Exception as e:

        print('cannot insert mtc_df')
        raise e


load_into_db(server, database, username, password)
