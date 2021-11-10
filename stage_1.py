import psycopg2
import csv
import shutil
import logging
import toml
import os
import pymssql

from dotenv import load_dotenv
from datetime import datetime
from nhs_tracing import adhoc

### ----- Config ----- ###

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, ".env"))

config = toml.load("config.toml")

logging.basicConfig(
    filename="radar_tracing.log",
    level=logging.INFO,
    format="%(levelname)s:%(asctime)s - %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S",
)

### ----- DB Connection ----- ###

MSUSER = os.environ.get("MSUSER")
MSPASSWORD = os.environ.get("MSPASSWORD")
MSDATABASE = os.environ.get("MSDATABASE")
MSHOST = os.environ.get("MSHOST")
MSSERVER = os.environ.get("MSSERVER")

try:
    rr_conn = pymssql.connect(
        host=MSHOST,
        server=MSSERVER,
        user=MSUSER,
        password=MSPASSWORD,
        database=MSDATABASE,
    )
    rr_cursor = rr_conn.cursor()
except:
    logging.info("Stage 1: Connection to RR unsuccessful")
    raise

try:
    radar_conn = psycopg2.connect("")
    radar_cursor = radar_conn.cursor()
except:
    logging.info("Stage 1: Connection to Radar unsuccessful")

### ----- Functions ----- ###


def set_file_names():
    """
    Builds a path string using todays date for audit and tracing files

    Returns:
        audit_file (str): audit file name as string
        trace_file (str): trace file name as string
    """
    file_string = "radar_audit_file"
    today = str(datetime.today())
    audit_file = f"{file_string}_{today[:10]}.csv"
    trace_file = f"{file_string}_{today[:10]}_to_trace.csv"
    return audit_file, trace_file


def get_patients():
    """
    Uses a query from radar_pats.sql to gather a list of
    radar patients and associated demographic data. This
    specifically look for source type RADAR (manual entry).
    Strips some problamtic commas.

    Returns:
        striped_radar_patients (list): radar patients demographic data
    """

    query_file = open("radar_pats.sql", "r")
    tracing_query = query_file.read()
    query_file.close()

    radar_cursor.execute(tracing_query)
    radar_patients = radar_cursor.fetchall()
    striped_radar_patients = []

    for patient in radar_patients:
        patient_list = list(patient)
        for n, item in enumerate(patient_list):
            if isinstance(item, str):
                patient_list[n] = item.replace(",", "")
        striped_radar_patients.append(patient_list)

    radar_cursor.close()
    radar_conn.close()

    return striped_radar_patients


def add_patients_to_audit_file(audit_file: str, radar_patients: list):
    """
    Build an audit csv which is used to build a tracing file. In stage 2
    this will be converted to an xlsx.

    Parameters:
        audit_file (str): path as a string
        radar_patients (list): list of radar patients and demographics
    """
    with open(audit_file, "w", newline="") as tracing_file:
        tracing_writer = csv.writer(tracing_file)
        tracing_writer.writerow(config["sheet_settings"]["audit_csv_headings"])
        tracing_writer.writerows(radar_patients)


def get_request_number():

    rr_cursor.execute("SELECT next value FOR SEQ_NHS_Tracing_Batch")
    result = rr_cursor.fetchone()
    rr_cursor.close()
    rr_conn.close()

    return result[0]


if __name__ == "__main__":
    # Prepare audit file
    audit_file, trace_file = set_file_names()
    radar_patients = get_patients()
    add_patients_to_audit_file(audit_file, radar_patients)
    # Prepare trace file
    request_number = get_request_number()
    column_map = adhoc.parse_columns(config["sheet_settings"]["tracing_columns"])
    reader = adhoc.create_reader(audit_file)
    adhoc.skip_header(reader)
    adhoc.prepare_file(
        request_number,
        column_map,
        config["formatting"]["date_format"],
        config["formatting"]["patients_per_file"],
        trace_file,
        reader,
    )
    # Send file to trace
    shutil.move(trace_file, config["paths"]["tracing_inbox"])
    logging.info(trace_file)
