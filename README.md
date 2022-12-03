# Acompanhamento Mensal das Transferências Obrigatórias da União Utilizando BS4 + Airflow

![alt text](https://github.com/CaioMendes92/Acompanhamento-Mensal-das-Transfer-ncias-Obrigat-rias-da-Uni-o-utilizando-BS4-Airflow/blob/main/imgs/airflow%2Btesouro_nacional.png?raw=true)

## 1. Problema de Negócio
Uma empresa de Alagoas precisa investigar o quanto foi enviado por mês de transferências obrigatórias para a união. Pensando nisso pediu que sua equipe de dados criasse uma solução que captasse e entregasse de uma forma clara o quanto foi enviado do ano de 2017 até 2022 e que seja atualizado mensalmente.

## 2. Premissas do Negócio
* Selecionou-se apenas o estado de Alagoas;
* Os anos selecionados foram de 2017 até 2022.

### 2.1 Descrição das variáveis
* **CSV Nome**: Nome da transferência obrigatória;
* **ESTADOS**: Nome do estado de onde foi transferido. Neste caso, apenas Alagoas;
* **UF**: Sigla do estado;
* **Mês**: Mês da transferência;
* **2017-2022**: Colunas com os anos da transferência.

## 3. Planejamento da Solução
Para começar, é necessário fazer uma raspagem dos dados no site do Tesouro Transparente. Para isso, utilizou-se o BS4. Em seguida, é feito um tratamento dos dados, limpando e ajustando variáveis.

A aquisição dos dados vem do Tesouro Transparente: https://www.tesourotransparente.gov.br/ckan/dataset/transferencias-obrigatorias-da-uniao

## 4. Perpectivas Futuras
* Aprimorar os dados de forma a selecionar todos os estados (caso seja necessário);
* Definir períodos anteriores;
* Cruzar informações com outras fontes para obter resultados diferentes.
