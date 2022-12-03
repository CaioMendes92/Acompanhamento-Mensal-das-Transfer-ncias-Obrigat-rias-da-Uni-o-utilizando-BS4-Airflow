from airflow import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup
import urllib.request as urllib_request
from urllib.request import urlopen
import pandas as pd
import numpy  as np


with DAG(
        "transferencia_obrigatoria_uniao",
        start_date=pendulum.datetime(2022, 8, 2, tz="UTC"),
        schedule_interval = '0 11 2 * *' # rodar as 11AM todo dia 2 do mês.
) as dag:

    tarefa_1 = BashOperator(
            task_id = 'cria_pasta',
            bash_command = 'mkdir -p "/home/caiomendes/Documents/airflow/mes={{data_interval_end.strftime("%Y-%m-%d")}}"'
            )

    def webscrapping(data_interval_end):
        url = 'https://www.tesourotransparente.gov.br/ckan/dataset/transferencias-obrigatorias-da-uniao'
        response = urlopen(url)
        html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for item in soup.findAll('a', {"class": "resource-url-analytics"}):
            links.append(item.get('href'))
        df_final = pd.DataFrame()
        for i in range(2,18):
            df = pd.read_csv(links[i], encoding='ISO8859',skiprows=7, on_bad_lines='skip', sep=';')
            df = df.iloc[1:28]
            df['CSV Nome'] = links[i].split('/')[9].split('.')[0]
            
            df_final = pd.concat([df_final,df])
            file_path = f'/home/caiomendes/Documents/airflow/mes={data_interval_end}/'
            df_final.to_csv(file_path + f'dados_brutos_mes={data_interval_end}.csv')

    tarefa_2 = PythonOperator(
                task_id = 'extrai_dados',
                python_callable = webscrapping,
                op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
        )

    def tratamento_dados(data_interval_end):
        file_path = f'/home/caiomendes/Documents/airflow/mes={data_interval_end}/'
        df_final = pd.read_csv(file_path + f'dados_brutos_mes={data_interval_end}.csv')

        df_final['ESTADOS'].fillna(df_final['Capitais'], inplace=True)
        df_final = df_final.replace(' -   ', '0')
        dft = df_final[(df_final['ESTADOS'] == 'Alagoas') ]\
        .melt(id_vars=['CSV Nome', 'ESTADOS','UF'], 
            value_vars = [col for col in df_final.columns 
                            if col not in ['CSV Nome', 'ESTADOS','UF']])
        mes_numbers = {'janeiro':1,'fevereiro':2,'março':3,'abril':4,'maio':5,
                        'junho':6,'julho':7,'agosto':8,'setembro':9,'outubro':10,
                        'novembro':11,'dezembro':12}

        init_years_range = 17
        final_years_range = 22

        df_all = ''

        for year in range(init_years_range,final_years_range+1):
            cols = ['CSV Nome', 'ESTADOS','UF'] + [col for col in df_final.columns if 
                                                col.endswith(f'/{year}')]
            dft  = df_final.loc[:,cols]

            dft = dft.melt(id_vars=['CSV Nome', 'ESTADOS','UF'], 
                value_vars = [col for col in dft.columns 
                                if col not in ['CSV Nome', 'ESTADOS','UF']])

            dft[['Mês','Ano']] = dft['variable'].str.split('/',expand=True)
            
            dft.drop(['variable','Ano'],inplace=True,axis=1)
            dft.rename(columns={'value':f'20{year}'},inplace=True)
            dft['Mês'] = dft['Mês'].replace(mes_numbers)
            dft = dft[['CSV Nome', 'ESTADOS','UF','Mês', f'20{year}']]
            
            if isinstance(df_all,str) == True:
                df_all = dft
            else:
                df_all = df_all.merge(dft,on=['CSV Nome','ESTADOS','Mês','UF'],how='left')
            df_all.to_csv(file_path + f'dados_tratados_alagoas_mes={data_interval_end}.csv')
    
    tarefa_3 = PythonOperator(
                task_id = 'trata_dados',
                python_callable = tratamento_dados,
                op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}


    )

    tarefa_1 >> tarefa_2
    tarefa_2 >> tarefa_3