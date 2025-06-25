import os
import pandas as pd
import gcsfs

# ================== FUNÇÕES DE TRANSFORMAÇÃO ==================
def decodificar_variaveis(df):
    """Decodifica variáveis categóricas com foco nas análises solicitadas"""
    mapeamentos = {
        'TP_SEXO': {'M': 'Masculino', 'F': 'Feminino'},
        'TP_FAIXA_ETARIA': {
            1: 'Menor de 17 anos',
            2: '17 anos',
            3: '18 anos',
            4: '19 anos',
            5: '20 anos',
            6: '21 anos',
            7: '22 anos',
            8: '23 anos',
            9: '24 anos',
            10: '25 anos',
            11: '26-30 anos',
            12: '31-35 anos',
            13: '36-40 anos',
            14: '41-45 anos',
            15: '46-50 anos',
            16: '51-55 anos',
            17: '56-60 anos',
            18: '61-65 anos',
            19: '66-70 anos',
            20: 'Maior de 70 anos'
        },
        'TP_COR_RACA': {
            1: 'Não declarado',
            2: 'Branca',
            3: 'Preta',
            4: 'Parda',
            5: 'Amarela',
            6: 'Indígena'
        },
        'TP_NACIONALIDADE': {
            1: 'Brasileiro(a)',
            2: 'Brasileiro(a) Naturalizado(a)',
            3: 'Estrangeiro(a)',
            4: 'Brasileiro(a) Nato(a) no exterior'
        },
        'TP_ESTADO_CIVIL': {
            1: 'Solteiro(a)',
            2: 'Casado(a)/União Estável',
            3: 'Divorciado(a)/Separado(a)',
            4: 'Viúvo(a)'
        },
        'TP_ST_CONCLUSAO': {
            1: 'Concluído',
            2: 'Concluirá em 2023',
            3: 'Concluirá após 2023',
            4: 'Não cursando'
        },
        'TP_ANO_CONCLUIU': {
            0: 'Não informado',
            1: '2022',
            2: '2021',
            3: '2020',
            4: '2019',
            5: '2018',
            6: '2017',
            7: '2016',
            8: '2015',
            9: '2014',
            10: '2013',
            11: '2012',
            12: '2011',
            13: '2010',
            14: '2009',
            15: '2008',
            16: '2007',
            17: 'Antes de 2007'
        },
        'TP_ESCOLA': {
            1: 'Não informado',
            2: 'Pública',
            3: 'Privada'
        },
        'TP_ENSINO': {
            1: 'Ensino Regular',
            2: 'Educação Especial'
        },
        'IN_TREINEIRO': {
            0: 'Não',
            1: 'Sim'
        },
        'TP_DEPENDENCIA_ADM_ESC': {
            1: 'Federal',
            2: 'Estadual',
            3: 'Municipal',
            4: 'Privada'
        },
        'TP_LOCALIZACAO_ESC': {
            1: 'Urbana',
            2: 'Rural'
        },
        'TP_PRESENCA_CN': {
            0: 'Faltou',
            1: 'Presente',
            2: 'Eliminado'
        },
        'TP_PRESENCA_CH': {
            0: 'Faltou',
            1: 'Presente',
            2: 'Eliminado'
        },
        'TP_PRESENCA_LC': {
            0: 'Faltou',
            1: 'Presente',
            2: 'Eliminado'
        },
        'TP_PRESENCA_MT': {
            0: 'Faltou',
            1: 'Presente',
            2: 'Eliminado'
        },
        'TP_LINGUA': {
            0: 'Inglês',
            1: 'Espanhol'
        },
        'TP_STATUS_REDACAO': {
            1: 'Sem problemas',
            2: 'Anulada',
            3: 'Cópia texto motivador',
            4: 'Em branco',
            6: 'Fuga ao tema',
            7: 'Não atendimento ao tipo',
            8: 'Texto insuficiente',
            9: 'Parte desconectada'
        },
        'Q001': {
            'A': 'Nunca estudou',
            'B': 'Fundamental incompleto',
            'C': 'Fundamental completo',
            'D': 'Médio incompleto',
            'E': 'Médio completo',
            'F': 'Superior incompleto',
            'G': 'Superior completo',
            'H': 'Não sei'
        },
        'Q002': {
            'A': 'Nunca estudou',
            'B': 'Fundamental incompleto',
            'C': 'Fundamental completo',
            'D': 'Médio incompleto',
            'E': 'Médio completo',
            'F': 'Superior incompleto',
            'G': 'Superior completo',
            'H': 'Não sei'
        },
        'Q006': {
            'A': 'Nenhuma renda',
            'B': 'Até R$ 1.320,00',
            'C': 'R$ 1.320,01 - 1.980,00',
            'D': 'R$ 1.980,01 - 2.640,00',
            'E': 'R$ 2.640,01 - 3.300,00',
            'F': 'R$ 3.300,01 - 3.960,00',
            'G': 'R$ 3.960,01 - 5.280,00',
            'H': 'R$ 5.280,01 - 6.600,00',
            'I': 'R$ 6.600,01 - 7.920,00',
            'J': 'R$ 7.920,01 - 9.240,00',
            'K': 'R$ 9.240,01 - 10.560,00',
            'L': 'R$ 10.560,01 - 11.880,00',
            'M': 'R$ 11.880,01 - 13.200,00',
            'N': 'R$ 13.200,01 - 15.840,00',
            'O': 'R$ 15.840,01 - 19.800,00',
            'P': 'R$ 19.800,01 - 26.400,00',
            'Q': 'Acima de R$ 26.400,00'
        },
        'Q025': {
            'A': 'Não',
            'B': 'Sim'
        }
    }
    
    for col, mapa in mapeamentos.items():
        if col in df.columns:
            df[col] = df[col].map(mapa).fillna('Não informado')
    
    return df

def calcular_capital_economico(df):
    """Cria índice de capital econômico baseado na posse de bens"""
    if 'Q006' in df.columns:
        # Mapeamento de renda para valores numéricos
        renda_map = {
            'A': 0, 'B': 660, 'C': 1650, 'D': 2310, 'E': 2970, 
            'F': 3630, 'G': 4620, 'H': 5940, 'I': 7260, 'J': 8580,
            'K': 9900, 'L': 11220, 'M': 12540, 'N': 14520,
            'O': 17820, 'P': 23100, 'Q': 33000
        }
        df['RENDA_FAMILIAR'] = df['Q006'].map(renda_map)
    
    # Lista de bens para cálculo do índice
    bens = ['Q010', 'Q011', 'Q012', 'Q013', 'Q016', 'Q019', 'Q021', 'Q022', 'Q024']
    for col in bens:
        if col in df.columns:
            # Considera qualquer posse como 1 ponto
            df[col + '_SCORE'] = df[col].apply(lambda x: 1 if x in ['B','C','D','E'] else 0)
    
    if 'Q010_SCORE' in df.columns:
        df['CAPITAL_ECONOMICO'] = (
            df[[col + '_SCORE' for col in bens]].sum(axis=1) +
            (df['RENDA_FAMILIAR'] / 1000 if 'RENDA_FAMILIAR' in df.columns else 0)
        )
    
    return df

def extrair_componentes_geograficos(df):
    """Extrai UF e município de códigos compostos"""
    if 'CO_MUNICIPIO_ESC' in df.columns:
        # Converte para string e extrai os componentes
        df['CO_MUNICIPIO_ESC'] = df['CO_MUNICIPIO_ESC'].astype(str).str.zfill(7)
        df['CO_UF_ESC'] = df['CO_MUNICIPIO_ESC'].str[0:2]
        df['CO_MUNICIPIO_ESC'] = df['CO_MUNICIPIO_ESC'].str[2:6]  # 4 dígitos do município
    
    return df

def validar_notas(df):
    """Garante que notas estejam no intervalo válido (0-1000)"""
    colunas_notas = [col for col in df.columns if col.startswith('NU_NOTA_')]
    for col in colunas_notas:
        if col in df.columns:
            # Converte para float e limita entre 0 e 1000
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].clip(0, 1000)
    return df

def tratar_valores_especiais(df):
    """Substitui valores especiais por nulos"""
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace(['*', '.'], pd.NA)
    return df

# ================== PROCESSAMENTO PRINCIPAL ==================
def processar_dados_gcs(bucket_name, chave_json, formato_entrada, caminho_particionado,
                        limpar_colunas, pasta_saida_local, grupos, particionado=False):
    # Autenticação no GCS
    fs = gcsfs.GCSFileSystem(token=chave_json)

    # Listar arquivos
    if particionado:
        arquivos = fs.glob(f'{bucket_name}/{caminho_particionado}*.{formato_entrada}')
    else:
        arquivos = [f'{bucket_name}/{caminho_particionado}.{formato_entrada}']

    for caminho_completo in arquivos:
        print(f'Processando {caminho_completo}...')
        nome_base = os.path.splitext(os.path.basename(caminho_completo))[0]

        # Ler dados
        with fs.open(caminho_completo, 'rb') as f:
            df = pd.read_parquet(f)

        # Remover colunas especificadas
        for col in limpar_colunas:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        # Aplicar transformações
        df = decodificar_variaveis(df)
        df = extrair_componentes_geograficos(df)
        df = validar_notas(df)
        df = tratar_valores_especiais(df)
        df = calcular_capital_economico(df)

        # Salvar grupos
        for grupo, colunas in grupos.items():
            colunas_existentes = [col for col in colunas if col in df.columns]
            if not colunas_existentes:
                continue

            df_subset = df[colunas_existentes]
            caminho_saida = f"{bucket_name}/{pasta_saida_local}/{grupo}/{nome_base}_{grupo}.parquet"
            print(f'Salvando {caminho_saida}...')

            with fs.open(caminho_saida, 'wb') as f_out:
                df_subset.to_parquet(f_out, index=False)

    print("Processamento finalizado.")

if __name__ == "__main__":
    # Configurações
    bucket_name = 'dados_enem-bucket'
    chave_json = '/home/raissa/Downloads/teak-amphora-460722-b2-4e7c2fe25adb.json'
    formato_entrada = 'parquet'
    caminho_particionado = 'bronze/parquet/MICRODADOS_ENEM_2023_chunk_'
    
    # Colunas a remover (ajustar conforme necessidade)
    remover_colunas = ['TX_RESPOSTAS_CN', 'TX_RESPOSTAS_CH', 'TX_RESPOSTAS_LC', 'TX_RESPOSTAS_MT',
                      'TX_GABARITO_CN', 'TX_GABARITO_CH', 'TX_GABARITO_LC', 'TX_GABARITO_MT']

    # Definição dos grupos de dados focados nas análises
    grupos_dados = {
        "fato_principal": [
            # Identificadores
            "NU_INSCRICAO", "NU_ANO",
            
            # Dados demográficos
            "TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA", "TP_NACIONALIDADE", "TP_ESTADO_CIVIL",
            
            # Dados educacionais
            "TP_ST_CONCLUSAO", "TP_ANO_CONCLUIU", "TP_ESCOLA", "TP_DEPENDENCIA_ADM_ESC",
            "TP_LOCALIZACAO_ESC", "TP_ENSINO", "IN_TREINEIRO",
            
            # Questionário socioeconômico (foco nas análises solicitadas)
            "Q001", "Q002", "Q003", "Q004", "Q005", "Q006", "Q007", "Q008", "Q009", "Q025",
            
            # Dados de desempenho
            "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT",
            "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
            "TP_LINGUA", "TP_STATUS_REDACAO",
            "NU_NOTA_COMP1", "NU_NOTA_COMP2", "NU_NOTA_COMP3", "NU_NOTA_COMP4", "NU_NOTA_COMP5",
            
            # Índice calculado
            "CAPITAL_ECONOMICO"
        ],
        
        "itens_patrimonio": [
            # Tabela adicional para análise detalhada de bens
            "NU_INSCRICAO",
            "Q010", "Q011", "Q012", "Q013", "Q014", "Q015", "Q016", "Q017", "Q018",
            "Q019", "Q020", "Q021", "Q022", "Q023", "Q024"
        ]
    }

    # Executar processamento
    processar_dados_gcs(
        bucket_name=bucket_name,
        chave_json=chave_json,
        formato_entrada=formato_entrada,
        caminho_particionado=caminho_particionado,
        limpar_colunas=remover_colunas,
        pasta_saida_local='silver/parquet',
        grupos=grupos_dados,
        particionado=True
    )