
# Bibliotecas

import pandas as pd
import numpy as np
from datetime import datetime

"""
    SCRIPT DE ETL E LIMPEZA DE DADOS - EMPRESA ZÉ PEQUENO
    Objetivo: Padronizar e limpar os dados de vendas da empresa, garantindo a qualidade e consistência para análises futuras.

"""

# Carregamento dos dados

caminho_arquivo = 'C:\\teste_git\\dados\\não_processado\\tabela_vendas_ZePequeno (1).xlsx'

df = pd.read_excel(caminho_arquivo)

total_original = len(df)

print(f'Total de linhas no arquivo: {total_original}')

print(df.head())

# 2. Saneamento Financeiro (valor_venda)
#objetivo: Limpar e padronizar a coluna 'valor_venda', garantindo que os valores sejam numéricos e consistentes para análises financeiras.
# Conversão de valores negativos para positivos

df = df.dropna(how='all')

df["Valor_Venda"] = df["Valor_Venda"].abs()

VALOR_MINIMO = 100
VALOR_MAXIMO = 5000

# Filtro de integridade: Valores de venda iguais a zero ou fora do intervalo definido são considerados inválidos e substituídos por NaN para posterior tratamento.
df.loc[df["Valor_Venda"] == 0, "Valor_Venda"] = np.nan
df.loc[(df["Valor_Venda"] < VALOR_MINIMO) | (df["Valor_Venda"] > VALOR_MAXIMO), "Valor_Venda"] = np.nan

# 3. Normalização de Textos
colunas_texto = ['Região', 'Canal', 'Produto']
for coluna in colunas_texto:
    df[coluna] = df[coluna].astype(str).str.strip().str.title()
    df[coluna] = df[coluna].replace('Nan', np.nan)

# 4. Tratamento de Dicionários e Precedência
#Objetivo: Unificar nomes de cidades, corrigir erros de digitação e aplicar a regra de precedência para produtos mistos.

correcao_produtos ={
   "Rol": "Rolhas", "Rolhhas": "Rolhas", "Rolha": "Rolhas",
    "Pacoca": "Paçoca", "Paçocca": "Paçoca",
    "Paçoca E Rolha": "Paçoca", "Paçoca E Rolhas": "Paçoca",
    "Rolha E Paçoca": "Rolhas", "Rolhas E Paçoca": "Rolhas"
}

df['Produto'] = df['Produto'].replace(correcao_produtos)

mapeamento_regiao = {
    'Joinville': 'Joinville', 'Blumenau':'Blumenau',  'Florianópolis':'Florianópolis',
    'Sudeste' : 'São Paulo','BLUMENAU' : 'Blumenau',  
    'florianopolis' : 'Florianópolis',    'Norte' : "Palmas",      
    'JoinVille': 'Joinville', 'São Paulo': 'São Paulo',       
    'joinvile': 'Joinville', 'blumenau': 'Blumenau',  'FLORIANÓPOLIS': 'Florianópolis',       
    'Nordeste': 'Salvador', 'joinville': 'Joinville', 'Sul': 'Florianopolis',      
    'JOINVILLE': 'Joinville', 'Centro-Oeste': ' Campo Grande',
    'Curitiba': 'Curitiba'
}
df['Região'] = df['Região'].replace(mapeamento_regiao)

correcao_canal = {
    "E-Commerc": "E-Commerce", "Loja Fisica": "Loja Física", "Loja Fisíca": "Loja Física"
}

df['Canal'] = df['Canal'].replace(correcao_canal)

# 5. Regras de Imputação (Preenchimento Automático)
# Objetivo: Garantir que nenhuma linha válida seja descartada por falta de informação básica.

# Data: Omissões são preenchidas com o primeiro dia do ano corrente
ano_atual = datetime.now().year
data_padrao = pd.to_datetime(f"01/01/{ano_atual}", dayfirst=True)
df["Data"] = pd.to_datetime(df["Data"], errors="coerce").fillna(data_padrao)

# Atribuição de padrões para campos nulos
df["Região"] = df["Região"].fillna("Florianópolis")

df["Canal"] = df["Canal"].fillna("E-Commerce")

df["Produto"] = df["Produto"].fillna("Paçoca")

# 6. FILTRO DE ESCOPO E CONSOLIDAÇÃO FINAL
#    Objetivo: Restringir os dados apenas às áreas de atuação autorizadas
#    e remover registros que restaram com campos nulos críticos.
regioes_validas = [
    "Joinville", "Blumenau", "Florianópolis", 
    "São Paulo", "Palmas", "Salvador", 
    "Santa Catarina", "Campo Grande", "Curitiba"
]

# Regiões fora da lista (ex: Curitiba) são invalidadas para remoção
df.loc[~df["Região"].isin(regioes_validas), "Região"] = np.nan

# Filtro Final: Remove qualquer linha que possua NaN em colunas essenciais
colunas_essenciais = ["Data", "Região", "Canal", "Produto", "Valor_Venda"]
df_limpo = df.dropna(subset=colunas_essenciais).copy()
df_limpo = df_limpo.reset_index(drop=True)

# 7. AUDITORIA E RELATÓRIO
#    Objetivo: Apresentar métricas de qualidade do processo de limpeza.
total_limpo = len(df_limpo)
eliminados = total_original - total_limpo
taxa_descarte = (eliminados / total_original) * 100

print("\n" + "=" * 48)
print("          RELATÓRIO DE LIMPEZA")
print("=" * 48)
print(f"  Linhas originais  : {total_original}")
print(f"  Linhas aprovadas  : {total_limpo}")
print(f"  Linhas removidas  : {eliminados}")
print(f"  Taxa de descarte  : {taxa_descarte:.1f}%")
print("=" * 48)
print(f"  Regiões válidas   : {sorted(df_limpo['Região'].unique())}")
print(f"  Canais válidos    : {sorted(df_limpo['Canal'].unique())}")
print(f"  Produtos válidos  : {sorted(df_limpo['Produto'].unique())}")
print(f"  Período           : {df_limpo['Data'].min().date()} → {df_limpo['Data'].max().date()}")
print(f"  Valor mínimo      : R$ {df_limpo['Valor_Venda'].min():,.2f}")
print(f"  Valor máximo      : R$ {df_limpo['Valor_Venda'].max():,.2f}")
print(f"  Valor médio       : R$ {df_limpo['Valor_Venda'].mean():,.2f}")
print("=" * 48 + "\n")

# 8. PERSISTÊNCIA DOS DADOS (CARGA)
#    Objetivo: Salvar o resultado final em diferentes formatos e versões.
pasta_clean = "C:\\dados_empresa_zp\\dados\\clean\\"
agora = datetime.now().strftime("%Y%m%d_%H%M")

# Exportação para Excel e CSV padronizado (UTF-8-SIG para Excel Brasil)
caminho_saida = r"C:\teste_git\dados\processados\\"
print(f"Excel salvo : tabela_vendas_ZePequeno_LIMPA.xlsx")

df_limpo.to_excel(f"{caminho_saida}tabela_vendas_ZePequeno_LIMPA.xlsx", index=False)
print(f"Excel salvo : tabela_vendas_ZePequeno_LIMPA.xlsx")