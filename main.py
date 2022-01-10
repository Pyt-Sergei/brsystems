import numpy as np
import pandas as pd
from fast_bitrix24 import Bitrix


webhook = "https://b24-zr7lcg.bitrix24.ru/rest/1/uyrpw0fz9byfg0tw/"
bitrix = Bitrix(webhook, verbose=False)

leads = bitrix.list_and_get('crm.lead')

df = pd.DataFrame(leads.values())
df = df.astype({
    'ID': 'int64',
    'COMPANY_ID': 'int64',
    'CONTACT_ID': 'int64',
    'ASSIGNED_BY_ID': 'int64',
    'CREATED_BY_ID': 'int64',
    'MODIFY_BY_ID': 'int64',
    'ORIGINATOR_ID': 'int64',
    'ORIGIN_ID': 'int64',
    'ADDRESS_LOC_ADDR_ID': 'int64',
    'OPPORTUNITY': 'float64',
    'BIRTHDATE': 'datetime64',
    'DATE_CREATE': 'datetime64',
    'DATE_MODIFY': 'datetime64',
    'DATE_CLOSED': 'datetime64',
}, errors='ignore')

bool_values = (
    'IS_RETURN_CUSTOMER',
    'IS_MANUAL_OPPORTUNITY',
    'HAS_PHONE',
    'HAS_EMAIL',
    'HAS_IMOL',
    'OPENED'
)

for bool_value in bool_values:
    df[bool_value] = df[bool_value].apply(lambda val: True if val == 'Y' else False)

'''
Пустые строки заполняются значениями nan, чтобы в таблицах BigQuery пустые ячейки заполнялись типом null
Значения в столбцах 'PHONE', 'EMAIL', 'WEB', 'IM' изначально являют списками, поэтому они преобразуются в str, чтобы
сохранить их в BQ как STRING
'''
df = df.replace('', np.nan)
for field in ('PHONE', 'EMAIL', 'WEB', 'IM'):
    for i in df.index:
        if pd.notna(df.loc[i, field]):
            df.loc[i, field] = str(df.loc[i, field])
