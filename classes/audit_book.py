import toml
from openpyxl import Workbook
from openpyxl.styles import Alignment


class Audit_workbook(Workbook):
    """
    Audit_work extends openpyxl's Workbook and adds a couple of functions
    to automate formatting and sheet creation
    """

    def __init__(self) -> None:
        super().__init__()
        self.config = toml.load("config.toml")

    def make_sheets(self):
        """
        Creates all the required sheets for the audit data
        """
        self.active.title = "TRACED DATA"

        for sheet_name in self.config["sheet_settings"]["sheet_names"]:
            # formatted to match key in toml file
            config_key = sheet_name.replace(" ", "_").lower()
            self.create_sheet(sheet_name)
            headers = self.config["sheet_settings"][config_key]
            self[sheet_name].append(headers)

    def set_column_widths(self):
        """
        Sets all column widths based off the longest piece of
        text in each column. Added 2 width for padding
        """
        sheets = self.config["sheet_settings"]["sheet_names"]
        sheets.append("TRACED DATA")
        for sheet in sheets:
            ws = self[sheet]
            for column_cells in ws.columns:
                length = max(len(cell.value or "") for cell in column_cells)
                if length == 0:
                    length = 5
                ws.column_dimensions[column_cells[0].column_letter].width = length + 2

    def center_align(self):
        """
        Sets each cell to center aligned purely for aesthetics.
        """
        sheets = self.config["sheet_settings"]["sheet_names"]
        sheets.append("TRACED DATA")
        for sheet in sheets:
            ws = self[sheet]
            for row in ws:
                for cell in row:
                    cell.alignment = Alignment(horizontal="center")
