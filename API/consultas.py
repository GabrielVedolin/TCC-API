import pandas as pd
import conexao as cn

def consulta(query):
    con = cn.conectar()
    cursor = con.cursor()
    cursor.execute(query)
    lista = cursor.fetchall()
    cn.desconectar(con)
    return lista

def consultaFeedProf(user_id, user_tipo):
    query_prof = 'select * from shae_db.v_obterConteudosPorUsuarioFeed where user_id = {0} and user_tipo = {1}'.format(
            user_id, user_tipo)
    conteudosProf = consulta(query_prof)
    return conteudosProf

def montaDataFrame(query):
    con = cn.conectar()
    dtFrame = pd.read_sql(query, con)
    cn.desconectar(con)
    return dtFrame

def inserirUltimoFeed(id_feed,user_id,conteudosFiltrados):
    con = cn.conectar()
    cursor = con.cursor()
    listaConteudoId = conteudosFiltrados["idConteudo"].values.tolist()
    args_str = ''
    if(listaConteudoId):
        for i in listaConteudoId:
            args_str += '({0}, {1}, {2}, False),'.format(id_feed, user_id, i)
        args_str = args_str[:-1]
        cursor.execute("INSERT INTO shae_db.ultimo_feed VALUES " + args_str) 
        con.commit()
        cn.desconectar(con)

def excluirUltimoFeed():
    con = cn.conectar()
    cursor = con.cursor()
    query_delete_feed = """ DELETE FROM shae_db.ultimo_feed;"""
    cursor.execute(query_delete_feed)
    con.commit()
    cn.desconectar(con)    

def inserirHistoricoFeed(id_feed,user_id,feed_disp_consumido):
    con = cn.conectar()
    cursor = con.cursor()
    listaConteudoId = feed_disp_consumido[["id_conteudos","consumo"]].values.tolist()
    args_str = ''
    if(listaConteudoId):
        for i in listaConteudoId:
            args_str += '({0}, {1}, {2}, {3}),'.format( user_id, id_feed, i[0], bool(i[1]))
        args_str = args_str[:-1]        
        cursor.execute("INSERT INTO shae_db.ultimo_feed VALUES " + args_str) 
        con.commit()
        cn.desconectar(con)

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
