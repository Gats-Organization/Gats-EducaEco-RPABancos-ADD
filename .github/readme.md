# RPA de Sincronização de Dados entre Bancos de Dados

Este projeto é um RPA desenvolvido em Python que realiza a captura de novos registros em um banco de dados PostgreSQL e sincroniza automaticamente esses dados em outro banco de dados PostgreSQL. O processo é automatizado utilizando Selenium, psycopg2 e outras dependências descritas no arquivo `requirements.txt`.

## Estrutura do Projeto

- **`rpa_sync.py`**: Script principal do RPA que executa a captura e sincronização dos dados entre os bancos de dados. Ele monitora o banco de origem, detecta novos inserts e insere esses registros no banco de destino.
- **`requirements.txt`**: Arquivo de dependências do projeto, que lista as bibliotecas necessárias para executar o RPA.
- **`rpa_log.log`**: Arquivo de log que registra todas as atividades realizadas pelo RPA, incluindo eventuais erros, o que facilita a monitoração e a análise do funcionamento do processo.