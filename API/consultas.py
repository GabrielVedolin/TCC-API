import psycopg2
import pandas as pd

# config db
host = 'ec2-50-19-255-190.compute-1.amazonaws.com'
dbname = 'de67a2stf4tsc2'
user = 'gadpffmonctxbl'
password = '8908b69b11b078c19ee8e791042bd7ee0a24fc9729062c77fea0791c91dc6c02'
ssl = 'require'

# string conexao
conn_string = 'host={0} user={1} dbname={2} password={3} sslmode={4}'.format(host, user, dbname, password, ssl)


def conectar():
    conn = psycopg2.connect(conn_string)
    return conn

def desconectar(conn):
    conn.close()

def consulta(query):
    con = conectar()
    cursor = con.cursor()
    cursor.execute(query)
    lista = cursor.fetchall()
    desconectar(con)
    return lista

def consultaFeedProf(user_id, user_tipo):
    query_prof = 'select * from shae_db.v_obterConteudosPorUsuarioFeed where user_id = {0} and user_tipo = {1}'.format(
            user_id, user_tipo)
    conteudosProf = consulta(query_prof)
    return conteudosProf

def montaDataFrame(query):
    con = conectar()
    dtFrame = pd.read_sql(query, con)
    desconectar(con)
    return dtFrame

def inserirUltimoFeed(id_feed,user_id,conteudosFiltrados):
    con = conectar()
    cursor = con.cursor()
    for indice in conteudosFiltrados.index:
            query_insert = """INSERT INTO shae_db.ultimo_feed (id_feed, id_aprendiz, id_conteudo, consumido)
                                          VALUES({0}, {1}, {2}, false);""".format(id_feed, user_id, conteudosFiltrados["idConteudo"][indice])
            cursor.execute(query_insert)
            con.commit()
    desconectar(con) 

def excluirUltimoFeed():
    con = conectar()
    cursor = con.cursor()
    query_delete_feed = """ DELETE FROM shae_db.ultimo_feed;"""
    cursor.execute(query_delete_feed)
    con.commit()
    desconectar(con)    

def inserirHistoricoFeed(id_feed,user_id,feed_disp_consumido):
    con = conectar()
    cursor = con.cursor()
    for indice in feed_disp_consumido.index:
            query_insert = """INSERT INTO shae_db.historico_feed(id_aprendiz, id_feed, id_conteudo, consumido, data_criacao)
                                    VALUES ({0}, {1},{2}, {3}, NOW());""".format(user_id, id_feed,
                                    feed_disp_consumido["id_conteudos"][indice],
                                    feed_disp_consumido["consumo"][indice].astype(bool))

            cursor.execute(query_insert)
            con.commit()
    desconectar(con)

def obterConteudoFiltradoFeed(qtdTexto,qtdQuestionario,qtdAudio,qtdVideo):
    query = 'select * from shae_db.v_obterConteudosComProfessores'
    dfConteudos = montaDataFrame(query)
    dfConteudos.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url","id_especialista","nome_especialista","user_tipo","id_feed"]
    
    conteudosTexto = dfConteudos.where(dfConteudos.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdTexto)
    conteudosTeste = dfConteudos.where(dfConteudos.tipo == "questionario").dropna(subset=["idConteudo"]).head(qtdQuestionario)
    conteudosAudio = dfConteudos.where(dfConteudos.tipo == "imagem").dropna(subset=["idConteudo"]).head(qtdAudio)
    conteudosVideo = dfConteudos.where(dfConteudos.tipo == "video").dropna(subset=["idConteudo"]).head(qtdVideo)

    ### JSON com a relação de contéudos
    conteudosFiltrados = pd.concat([conteudosTexto, conteudosVideo, conteudosTeste, conteudosAudio])
    conteudosFiltrados = conteudosFiltrados.sample(frac=1)
    
    return conteudosFiltrados
