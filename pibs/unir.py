import pandas

pibs = pandas.read_csv('pibs_ibge.csv', sep=',', encoding='utf-8')
tse = pandas.read_csv('codigos_tse.csv', sep=',', encoding='utf-8')

pibs['BUSCA'] = pibs['UF'].map(str) + "_" + pibs['CIDADE'].map(str)
tse['BUSCA'] = tse['UF_TSE'].map(str) + "_" + tse['NOME_TSE'].map(str) 

saida = pandas.merge(pibs, tse, on='BUSCA', how='left')
saida = saida.fillna('-')

saida.to_csv('saida.csv', sep=',', encoding='utf-8')
