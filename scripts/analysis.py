from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os

# Inicializar sessão Spark
spark = SparkSession.builder \
    .appName("ENEM2023_Analysis") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Configurações do GCS
bucket_name = "ifnmg-enem"
chave_json = "chave/fine-slice-304523-378cca0bed61.json"
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", chave_json)

# 1. CARREGAR DADOS DA CAMADA SILVER
def load_silver_data(table_name):
    path = f"gs://{bucket_name}/silver/parquet/{table_name}"
    return spark.read.parquet(path)

# Carregar todas as tabelas
tables = {
    "participante": load_silver_data("participante"),
    "escola": load_silver_data("escola"),
    "local_prova": load_silver_data("local_prova"),
    "prova_objetiva": load_silver_data("prova_objetiva"),
    "redacao": load_silver_data("redacao"),
    "socioeconomico": load_silver_data("socioeconomico")
}

# Juntar todos os DataFrames
df = tables["participante"] \
    .join(tables["escola"], "NU_INSCRICAO", "left") \
    .join(tables["local_prova"], "NU_INSCRICAO", "left") \
    .join(tables["prova_objetiva"], "NU_INSCRICAO", "left") \
    .join(tables["redacao"], "NU_INSCRICAO", "left") \
    .join(tables["socioeconomico"], "NU_INSCRICAO", "left")

# 2. ANÁLISES DEMOGRÁFICAS
def analise_demografica(df):
    # Idade vs Notas
    idade_notas = df.groupBy("TP_FAIXA_ETARIA").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Sexo vs Notas
    sexo_notas = df.groupBy("TP_SEXO").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Cor/Raça vs Notas
    cor_notas = df.groupBy("TP_COR_RACA").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Nacionalidade vs Notas
    nacionalidade_notas = df.groupBy("TP_NACIONALIDADE").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Estado Civil vs Notas
    estado_civil_notas = df.groupBy("TP_ESTADO_CIVIL").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    return {
        "idade_notas": idade_notas,
        "sexo_notas": sexo_notas,
        "cor_notas": cor_notas,
        "nacionalidade_notas": nacionalidade_notas,
        "estado_civil_notas": estado_civil_notas
    }

# 3. ANÁLISES EDUCACIONAIS
def analise_educacional(df):
    # Situação de Conclusão vs Notas
    conclusao_notas = df.groupBy("TP_ST_CONCLUSAO", "IN_TREINEIRO").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Ano de Conclusão vs Notas
    ano_conclusao_notas = df.groupBy("TP_ANO_CONCLUIU").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Tipo de Escola vs Notas
    escola_type = when(
        ((col("TP_ESCOLA") == 2) | (col("TP_DEPENDENCIA_ADM_ESC").isin(1, 2, 3))), "Pública"
    ).otherwise("Privada")
    
    escola_notas = df.withColumn("TIPO_ESCOLA", escola_type) \
        .groupBy("TIPO_ESCOLA") \
        .agg(
            count("*").alias("Total_Participantes"),
            mean("NU_NOTA_CN").alias("Media_CN"),
            mean("NU_NOTA_CH").alias("Media_CH"),
            mean("NU_NOTA_LC").alias("Media_LC"),
            mean("NU_NOTA_MT").alias("Media_MT"),
            mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
        )
    
    # Localização da Escola vs Notas
    localizacao_notas = df.groupBy("TP_LOCALIZACAO_ESC").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Tipo de Ensino vs Notas
    ensino_notas = df.groupBy("TP_ENSINO").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    return {
        "conclusao_notas": conclusao_notas,
        "ano_conclusao_notas": ano_conclusao_notas,
        "escola_notas": escola_notas,
        "localizacao_notas": localizacao_notas,
        "ensino_notas": ensino_notas
    }

# 4. ANÁLISES SOCIOECONÔMICAS
def analise_socioeconomica(df):
    # Escolaridade dos Pais vs Notas
    escolaridade_pai_notas = df.groupBy("Q001").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    escolaridade_mae_notas = df.groupBy("Q002").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Ocupação dos Pais vs Notas
    ocupacao_pai_notas = df.groupBy("Q003").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    ocupacao_mae_notas = df.groupBy("Q004").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Renda Familiar vs Notas
    renda_notas = df.groupBy("Q006").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Moradores vs Notas
    moradores_notas = df.groupBy("Q005").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Empregado Doméstico vs Notas
    domestico_notas = df.groupBy("Q007").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Condições de Moradia vs Notas
    banheiros_notas = df.groupBy("Q008").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    quartos_notas = df.groupBy("Q009").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    # Acesso a Bens vs Notas
    bens_cols = [f"Q{str(i).zfill(3)}" for i in range(10, 26)]
    bens_notas = {}
    
    for bem in bens_cols:
        bem_df = df.groupBy(bem).agg(
            count("*").alias("Total_Participantes"),
            mean("NU_NOTA_CN").alias("Media_CN"),
            mean("NU_NOTA_CH").alias("Media_CH"),
            mean("NU_NOTA_LC").alias("Media_LC"),
            mean("NU_NOTA_MT").alias("Media_MT"),
            mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
        )
        bens_notas[bem] = bem_df
    
    # Índice Socioeconômico (Soma de bens)
    bens_cols = [f"Q{str(i).zfill(3)}" for i in range(10, 26)]
    for col_name in bens_cols:
        df = df.withColumn(
            f"{col_name}_score", 
            when(col(col_name) != "A", 1).otherwise(0)
        )
    
    df = df.withColumn(
        "INDICE_BENS", 
        sum([col(f"{c}_score") for c in bens_cols])
    )
    
    indice_bens_notas = df.groupBy("INDICE_BENS").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_CN").alias("Media_CN"),
        mean("NU_NOTA_CH").alias("Media_CH"),
        mean("NU_NOTA_LC").alias("Media_LC"),
        mean("NU_NOTA_MT").alias("Media_MT"),
        mean("NU_NOTA_REDACAO").alias("Media_REDACAO")
    )
    
    return {
        "escolaridade_pai_notas": escolaridade_pai_notas,
        "escolaridade_mae_notas": escolaridade_mae_notas,
        "ocupacao_pai_notas": ocupacao_pai_notas,
        "ocupacao_mae_notas": ocupacao_mae_notas,
        "renda_notas": renda_notas,
        "moradores_notas": moradores_notas,
        "domestico_notas": domestico_notas,
        "banheiros_notas": banheiros_notas,
        "quartos_notas": quartos_notas,
        "bens_notas": bens_notas,
        "indice_bens_notas": indice_bens_notas
    }

# 5. ANÁLISE DE DESEMPENHO E PARTICIPAÇÃO
def analise_desempenho(df):
    # Taxa de Presença por Área
    presenca_cols = ["TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT"]
    presenca_analise = {}
    
    for col_name in presenca_cols:
        presenca_df = df.groupBy(col_name).agg(
            count("*").alias("Total_Participantes")
        ).withColumn(
            "Taxa_Presenca", 
            when(col(col_name) == 1, 1).otherwise(0)
        )
        presenca_analise[col_name] = presenca_df
    
    # Distribuição de Notas
    nota_cols = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    distribuicao_notas = {}
    
    for col_name in nota_cols:
        stats = df.select(
            mean(col_name).alias("Media"),
            stddev(col_name).alias("Desvio_Padrao"),
            min(col_name).alias("Minimo"),
            expr("percentile_approx(" + col_name + ", 0.25)").alias("Q1"),
            expr("percentile_approx(" + col_name + ", 0.5)").alias("Mediana"),
            expr("percentile_approx(" + col_name + ", 0.75)").alias("Q3"),
            max(col_name).alias("Maximo")
        )
        distribuicao_notas[col_name] = stats
    
    # Língua Estrangeira vs Nota de LC
    lingua_notas = df.groupBy("TP_LINGUA").agg(
        count("*").alias("Total_Participantes"),
        mean("NU_NOTA_LC").alias("Media_LC")
    )
    
    # Status da Redação
    status_redacao = df.groupBy("TP_STATUS_REDACAO").agg(
        count("*").alias("Total")
    )
    
    # Correlação entre Competências da Redação
    competencias = ["NU_NOTA_COMP1", "NU_NOTA_COMP2", "NU_NOTA_COMP3", 
                   "NU_NOTA_COMP4", "NU_NOTA_COMP5", "NU_NOTA_REDACAO"]
    
    corr_matrix = []
    for i, col1 in enumerate(competencias):
        for j, col2 in enumerate(competencias):
            if i < j:
                corr = df.stat.corr(col1, col2)
                corr_matrix.append((col1, col2, corr))
    
    corr_schema = StructType([
        StructField("Variavel1", StringType()),
        StructField("Variavel2", StringType()),
        StructField("Correlacao", DoubleType())
    ])
    correlacao_redacao = spark.createDataFrame(corr_matrix, corr_schema)
    
    return {
        "presenca_analise": presenca_analise,
        "distribuicao_notas": distribuicao_notas,
        "lingua_notas": lingua_notas,
        "status_redacao": status_redacao,
        "correlacao_redacao": correlacao_redacao
    }

# 6. EXECUTAR TODAS AS ANÁLISES
analises = {
    "demografica": analise_demografica(df),
    "educacional": analise_educacional(df),
    "socioeconomica": analise_socioeconomica(df),
    "desempenho": analise_desempenho(df)
}

# 7. SALVAR RESULTADOS NO GCS
def save_results(results, output_path):
    for category, analyses in results.items():
        for analysis_name, df in analyses.items():
            if isinstance(df, dict):
                for sub_name, sub_df in df.items():
                    path = f"{output_path}/{category}/{analysis_name}/{sub_name}"
                    sub_df.write.mode("overwrite").parquet(path)
            else:
                path = f"{output_path}/{category}/{analysis_name}"
                df.write.mode("overwrite").parquet(path)

output_path = f"gs://{bucket_name}/gold/analise_enem_2023"
save_results(analises, output_path)

# Encerrar sessão Spark
spark.stop()