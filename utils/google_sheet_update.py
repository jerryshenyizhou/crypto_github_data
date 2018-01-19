# appending scripts
import gspread
import string
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds']
headers = gspread.httpsession.HTTPSession(headers={'Connection': 'Keep-Alive'})
credentials = ServiceAccountCredentials.from_json_keyfile_name('./utils/China_marketing_analytics-60a29bb30485.json', scope)

gc = gspread.authorize(credentials)
# look up the column numbers
global di
di = dict(zip(string.letters, [ord(c) % 32 for c in string.letters]))


def numberToLetters(q):
    q = q - 1
    result = ''
    while q >= 0:
        remain = q % 26
        result = chr(remain + 65) + result;
        q = q // 26 - 1
    return result


def write_cells(data_fill, sheet_key, tab, cell_col, cell_row, transpose=0):
    """
    Args:
    data_fill: the the data that needs to be written, in a form of a pandas DataFrame or Series
    sheet_key: the key of the google sheet
    tab: tab of the google sheet,
    cell_col: column of the starting cell
    cell_row: row of the starting cell
    transpose: if this needs to be transposed. If the data_fill is series, it's going to be written vertically unless transpose = 1
    """
    spread_sheet = gc.open_by_key(sheet_key)
    sheet = spread_sheet.worksheet(tab)
    if type(data_fill) in ([int, np.float64, str]):
        sheet.update_acell(cell_col + str(cell_row), data_fill)
    else:
        if transpose == 0:
            data_fill = pd.DataFrame(data_fill)
        else:
            data_fill = pd.DataFrame(data_fill).T
        cell_col_num = di[cell_col]
        num_lines, num_columns = data_fill.shape
        cell_list = sheet.range(cell_col + str(cell_row) + ':' + numberToLetters(num_columns + cell_col_num - 1) + str(
            num_lines + cell_row - 1))
        for cell in cell_list:
            val = data_fill.iloc[cell.row - cell_row, cell.col - cell_col_num]
            if type(val) is str:
                val = val.decode('utf-8')
            elif isinstance(val, (int, long, float, complex)):
                # note that we round all numbers
                val = val
            cell.value = val
        sheet.update_cells(cell_list)


# configuration of googlesheet specs
def get_googlesheet_data(sheet_key, tab):
    """
    Args:
    sheet_key: the key of the google sheet
    tab: tab of the google sheet
    returns a pandas dataframe
    """
    sht1 = gc.open_by_key(sheet_key)
    worksheet = sht1.worksheet(tab)
    data = pd.DataFrame(worksheet.get_all_records())

    return (data)
