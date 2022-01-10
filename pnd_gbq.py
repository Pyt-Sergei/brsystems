"""
pandas-gbq - библиотека с открытым исходным кодом, поддерживаемая PyData и добровольными участниками
pandas-gbq предоставляет простой интерфейс для выполнения запросов и загрузки датафреймов pandas в BigQuery.
Эта тонкая оболочка клиентской библиотеки BigQuery, google-cloud-bigquery
выполняет запросы и сохраняет данные из данных pandas в таблицы, но не предоставляет
полную функциональность API BigQuery. В целях данной задачи, можно воспользоваться и библиотекой pandas-gbq
и полнофункциональным BigQuery API.
"""

import pandas_gbq
from google.oauth2 import service_account

from main import df


credentials = service_account.Credentials.from_service_account_file(
    'my-first-project-335519-ab0d42878c02.json',
)

project_id = "my-first-project-335519"
table_id = "bitrix24.leads"

pandas_gbq.to_gbq(df, table_id, if_exists='replace', credentials=credentials)
