import csv
import logging
import os
import re
import shutil

import toml
from dotenv import load_dotenv
from rr_connection_manager.classes.postgres_connection import \
    PostgresConnection

from classes.audit_book import Audit_workbook

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

try:
    radar_conn = PostgresConnection(app="radar_live", tunnel=True)
except:
    logging.info("Stage 2: Connection to Radar unsuccessful")

### ----- SQL ----- ###

dod_update_query = """
    UPDATE
        patient_demographics
    SET
        date_of_death = %s,
        modified_user_id = %s,
        modified_date = NOW()
    WHERE
        patient_id = %s
    AND
        source_type = 'RADAR'
"""

### ----- Functions ----- ###


def find_file_names():
    """
    Search the log file to get the name of the file sent for tracing.
    Use that to create the name of the audit file and to search
    for the traced file name.

    Returns:
        audit_file (str): radar_audit_file + date
        traced_file (str): name of traced file
    """

    with open("radar_tracing.log") as log_file:
        file_name_found = False
        lines = list(log_file)
        # Work from the end
        line_index = -1
        while not file_name_found:
            line = lines[line_index]
            line_index -= 1
            if "radar_audit_file" in line:
                file_name_found = True
        audit_file = line[27:-14]

    outbox_files = [
        f
        for f in os.listdir(config["paths"]["tracing_outbox"])
        if os.path.isfile(os.path.join(config["paths"]["tracing_outbox"], f))
    ]

    for trace_file in outbox_files:
        if audit_file in trace_file:
            audit_file = f"{audit_file}.csv"
            return audit_file, trace_file


def set_paths(audit_file: str, traced_file: str):
    """
    Use the file names to create paths to the files

    Parameters:
        audit_file (str): audit file name as string
        traced_file (str): traced file name as string

    Returns:
        audit_file_path (str): radar_audit_file + date
        traced_file_path (str): name of traced file
    """
    audit_file_path = os.path.join(os.getcwd(), audit_file)
    traced_file_path = os.path.join(os.getcwd(), traced_file)
    return audit_file_path, traced_file_path


def create_audit_xlsx():
    """
    Use the file names to create paths to the files

    Returns:
        audit_xlsx (obj): an Audit_workbook which inherits from openpyxl.Workbook
    """
    audit_xlsx = Audit_workbook()
    audit_xlsx.make_sheets()
    return audit_xlsx


def combine_audit_with_traced(
    audit_csv_path: str, traced_file_path: str, audit_xlsx: Audit_workbook
):
    """
    Takes the original audit csv and the traced csv and combines them into a
    xlsx file. Calls combine_lines().

    Parameters:
        audit_csv_path (str): audit file path as string
        traced_file_path (str): traced file path as string
        audit_xlsx (obj): an Audit_workbook object
    """

    audit_sheet = audit_xlsx["TRACED DATA"]
    audit_data = {}

    with open(audit_csv_path, "r") as audit_csv:

        # Add nones to form gap to distinguish between audit and traced data
        headers = [
            *csv.DictReader(audit_csv).fieldnames,
            *[None, None],
            *config["sheet_settings"]["audit_traced_headers"],
        ]
        audit_sheet.append(headers)
        for audit_row in csv.reader(audit_csv):
            audit_data[audit_row[0]] = audit_row

    with open(traced_file_path, "r") as traced_file:
        # First row is an identifier require for tracing not a record
        next(traced_file)
        reader = csv.reader(traced_file)
        csv_data = list(reader)
        # Last row is an identifier require for tracing not a record
        rows = len(csv_data) - 1
        for n, traced_row in enumerate(csv_data):
            if n != rows:
                audit_line = audit_data[traced_row[1]]
                combined_line = combine_lines(audit_line, traced_row)
                audit_sheet.append(combined_line)


def combine_lines(audit_line: list, traced_line: list):
    """
    Reorder and combine two list ready to be appended to a worksheet.
    order_for_tracing is a list of index numbers used to reorder the traced data.
    Formats dates returned by tracing to include a hyphen between year, month, day

    Parameters:
        audit_line (list): audit file data
        traced_line (list): dirty, unordered traced file data

    Returns:
        combined_line (list): combination of audit data and cleaned ordered trace data
    """

    # Nones add a divide between audit and traced data
    audit_line.extend([None, None])
    traced_line = [
        traced_line[n] for n in config["sheet_settings"]["order_for_tracing"]
    ]

    combined_line = [*audit_line, *traced_line]

    # Format dates
    bad_dates_index = [19, 20]

    for index in bad_dates_index:
        autofilled = False
        bad_date = combined_line[index]

        if bad_date:
            # Strip non numerics in case traced data format ever changes
            bad_date = re.sub("[^0-9]", "", bad_date)
            bad_date_len = len(bad_date)

            # Dates sometimes come back missing days or months default to 01
            if bad_date_len == 4:
                message = "Auto filled month and day"
                bad_date = f"{bad_date}0101"
                autofilled = True

            if bad_date_len == 6:
                message = "Auto filled day"
                bad_date = f"{bad_date}01"
                autofilled = True

            good_date = "-".join([bad_date[:4], bad_date[4:6], bad_date[6:]])
            combined_line[index] = good_date

            if autofilled and index == 19:
                dob_line = build_line(combined_line, message, None, index)
                audit_xlsx["DOB DIFF"].append(dob_line)

            if autofilled and index == 20:
                dod_line = build_line(combined_line, message, None, index)
                audit_xlsx["DOD DIFF"].append(dod_line)

    return combined_line


def build_line(
    row: object,
    message: str,
    radar_value_index: int = None,
    traced_value_index: int = None,
    *args,
):
    """
    A function to build error lines in their various formats in the audit file.

    Parameters:
        row (object): a row from the audit file
        message (str): message for specific problem
        traced_value_index (int): the index of the traced data value in row
        radar_value_index (int): the index of the radar data value in row
        args: included to deal with cases where multiple data points are required

    Returns:
        built_line (list): a list of the entries required for the line in the error sheet
    """

    specific_line_index = [index for index in config["sheet_settings"]["basic_line"]]

    if radar_value_index:
        specific_line_index.insert(5, radar_value_index)
    else:
        specific_line_index.insert(5, -1)
    if traced_value_index:
        specific_line_index.append(traced_value_index)

    # Deals with the fact these sheets have a different number of headers
    if "NHS" in message or "name" in message:
        specific_line_index.pop(5)

    built_line = []
    for index in specific_line_index:
        if index >= 0:
            built_line.append(row[index])
        else:
            built_line.append(None)

    if args:
        for index in args:
            # If CHI or HSC add to specific index otherwise append
            if index == 2:
                built_line.insert(5, row[index])
            elif index == 3:
                built_line.insert(6, row[index])
            else:
                built_line.append(row[index])

    built_line.append(message)
    return built_line


def update_dod(patient_id, dod):
    radar_conn.session.execute(
        dod_update_query, [dod, config["radar_trace_user"]["id"], patient_id]
    )


def find_differences(audit_xlsx: Audit_workbook):
    """
    A load of checks to see if the tracing data matches the radar
    data and to see if any data is missing from radar but present in tracing.
    Where a problem is detected a line is added to the relevent errors page in the
    audit workbook with a message to explain the issue. Lines are built with build_line()

    Parameters:
        audit_xlsx (Audit_workbook): audit file
    """

    for row in audit_xlsx["TRACED DATA"].iter_rows(min_row=2, values_only=True):

        # NHS numbers
        if row[14] and not row[1]:
            message = "NHS number missing in Radar"
            nhs_num_line = build_line(row, message, None, 14, 2, 3)
            audit_xlsx["NHS NUM DIFF"].append(nhs_num_line)

        if row[1] and row[14] and row[1] != row[14]:
            message = "NHS number different"
            nhs_num_line = build_line(row, message, 1, 14, 2, 3)
            audit_xlsx["NHS NUM DIFF"].append(nhs_num_line)

        # Dates of birth
        if row[19] and not row[6]:
            message = "Date of birth missing in Radar"
            dob_line = build_line(row, message, None, 19)
            audit_xlsx["DOB DIFF"].append(dob_line)

        if row[6] and row[19] and row[6] != row[19]:
            message = "Date of birth different"
            dob_line = build_line(row, message, 6, 19)
            audit_xlsx["DOB DIFF"].append(dob_line)

        # Dates of death
        if row[20] and not row[7]:
            message = "Date of death missing in Radar"
            dod_line = build_line(row, message, None, 20)
            audit_xlsx["DOD DIFF"].append(dod_line)
            update_dod(row[0], row[20])

        if row[7] and row[20] and row[7] != row[20]:
            message = "Date of death different"
            dod_line = build_line(row, message, 7, 20)
            audit_xlsx["DOD DIFF"].append(dod_line)

        # Gender
        if row[21] and not row[8]:
            message = "Gender missing in Radar"
            gender_line = build_line(row, message, None, 21)
            audit_xlsx["SEX DIFF"].append(gender_line)

        if row[8] and row[21] and row[8] != row[21]:
            message = "Gender different"
            gender_line = build_line(row, message, 8, 21)
            audit_xlsx["SEX DIFF"].append(gender_line)

        # Postcode
        if row[22] and not row[9]:
            message = "Postcode missing in Radar"
            postcode_line = build_line(row, message, None, 22)
            audit_xlsx["POSTCODE DIFF"].append(postcode_line)

        if row[9] and row[22] and row[9] != row[22]:
            message = "Postcode different"
            postcode_line = build_line(row, message, 9, 22)
            audit_xlsx["POSTCODE DIFF"].append(postcode_line)

        # Names
        radar_first_name = str(row[4]).upper()
        radar_last_name = str(row[5]).upper()
        traced_first_name = str(row[15]).upper()
        traced_last_name = str(row[16]).upper()

        if traced_first_name and traced_last_name:

            if (
                radar_first_name != traced_first_name
                and radar_last_name != traced_last_name
            ):
                message = "Both names different"
                name_line = build_line(row, message, None, 15, 16, 17, 18)
                audit_xlsx["NAME DIFF"].append(name_line)


if __name__ == "__main__":
    audit_csv, traced_file = find_file_names()
    audit_csv_path, traced_file_path = set_paths(audit_csv, traced_file)
    # Get the completed trace file
    shutil.copyfile(
        f"{config['paths']['tracing_outbox']}{traced_file}", traced_file_path
    )
    audit_xlsx = create_audit_xlsx()
    combine_audit_with_traced(audit_csv_path, traced_file_path, audit_xlsx)
    find_differences(audit_xlsx)
    audit_xlsx.set_column_widths()
    audit_xlsx.center_align()
    audit_xlsx.save(f"{audit_csv_path[-31:-4]}.xlsx")
    radar_conn.commit()
    radar_conn.close()
