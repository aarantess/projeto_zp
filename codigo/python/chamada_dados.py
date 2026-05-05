
# Código para ler um arquivo Excel usando a biblioteca pandas e imprimir seu conteúdo.

import pandas as pd

# Define o caminho do arquivo Excel a ser lido.

caminho = r"C:\teste_git\dados\não_processado\tabela_vendas_ZePequeno (1).xlsx"

# Lê o arquivo Excel e armazena os dados em um DataFrame chamado 'zp'.

# zp = pd.read_excel(caminho, sheet_name="Planilha1")  # Especifica a planilha a ser lida, se necessário.

zp = pd.read_excel(caminho)

print(zp)

print(zp.columns) # Imprime os nomes das colunas do DataFrame.

print(zp.head()) # Imprime as primeiras 5 linhas do DataFrame para uma visão geral dos dados.

print(zp.info()) # Imprime informações sobre o DataFrame, como o número de entradas, tipos de dados e uso de memória.

print(zp.describe()) # Imprime estatísticas descritivas para as colunas numéricas do DataFrame, como média, desvio padrão, valores mínimos e máximos.
print(zp['Canal'].unique()) # Imprime os valores únicos presentes na coluna 'Canal' do DataFrame.
