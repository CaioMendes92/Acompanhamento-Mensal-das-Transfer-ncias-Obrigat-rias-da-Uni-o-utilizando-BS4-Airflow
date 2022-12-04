# Acompanhamento Mensal das Transferências Obrigatórias da União Utilizando BS4 + Airflow

![alt text](https://github.com/CaioMendes92/Acompanhamento-Mensal-das-Transfer-ncias-Obrigat-rias-da-Uni-o-utilizando-BS4-Airflow/blob/main/imgs/airflow%2Btesouro_nacional.png?raw=true)

## 1. Problema de Negócio
É necessário captar e entregar de uma forma clara as transferências obrigatórias realizadas para a união no estado de Alagoas no período de 2017 até 2022. Como bônus, é importante que seja atualizado mensalmente.

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

## 4. Resultado Final
![alt text](https://github.com/CaioMendes92/Acompanhamento-Mensal-das-Transfer-ncias-Obrigat-rias-da-Uni-o-utilizando-BS4-Airflow/blob/main/imgs/resultado_final.png?raw=true)

Para uma atualização recorrente, foi utilizado o Apache Airflow com uma atualização todos os dia 2 de cada mês às 11am. A quesito de teste, foi considerado que a primeira data foi 02/08/2022, de forma que é criado uma página por mês com os dados mais atuais. A imagem abaixo exibe que as atualizações foram realizadas com sucesso.

![alt text](https://github.com/CaioMendes92/Acompanhamento-Mensal-das-Transfer-ncias-Obrigat-rias-da-Uni-o-utilizando-BS4-Airflow/blob/main/imgs/tela_airflow.jpeg?raw=true)

A documentação com maiores explicações de cada função utilizada para o resultado pode ser encontrada [aqui](https://caiomendes92.atlassian.net/wiki/spaces/CAIO/pages/163841/Documenta+o+do+projeto+Acompanhamento+Mensal+das+Transfer+ncias+Obrigat+rias+da+Uni+o+Utilizando+BS4+Airflow). Vale salientar que é necessário um cadastro para acessar o site do Confluence e ler a documentação, entretanto, é possível baixar o PDF [aqui](https://drive.google.com/file/d/143SLUqG6PXJoAzZdoZGQz2Aalsz25j-1/view?usp=share_link). Para uma melhor experiência, é aconselhado realizar o cadastro.

## 5. Perpectivas Futuras
* Refinar as funções, sobretudo a de tratamento. Com mais tempo, possivelmente testaria se a primeira função melt é realmente necessária.
* Melhoria nos nomes dos DataFrames, estão muito genéricos.
* Melhoria na forma de organizar o código
* Melhor adaptação para o Airflow.
* Entender se a criação de pastas é a melhor solução, uma vez que os dados podem ser sobrepostos e diminuiria o espaço utilizado em um banco de dados.
* Melhor explicação das funções e de uma forma mais concisa na documentação.
* Conectar os dados ao Power BI e criar um Dashboard com a evolução das transferências ao longo dos meses.
* Cruzar outras informações públicas para criar insights.
