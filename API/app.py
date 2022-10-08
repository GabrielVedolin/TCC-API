import psycopg2
import pandas as pd
import random
from flask import Flask, jsonify
# from flask_cors import CORS


app = Flask(__name__)
# CORS(app)

# config db
host = 'ec2-50-19-255-190.compute-1.amazonaws.com'
dbname = 'de67a2stf4tsc2'
user = 'gadpffmonctxbl'
password = '8908b69b11b078c19ee8e791042bd7ee0a24fc9729062c77fea0791c91dc6c02'
ssl = 'require'

# string conexao
conn_string = 'host={0} user={1} dbname={2} password={3} sslmode={4}'.format(host, user, dbname, password, ssl)

# #config db
# host = 'localhost'
# dbname = 'postgres'
# user = 'postgres'
# password = '1234'

# #string conexao
# conn_string = 'host={0} user={1} dbname={2} password={3}'.format(host, user, dbname, password)

conn = psycopg2.connect(conn_string)


@app.route('/')
def main():
    cursor = conn.cursor()
    cursor.execute('select * from shae_db.conteudo')
    lista = cursor.fetchall()
    return jsonify(lista)
    # return '<h1>API Iniciada</h1>'


@app.route('/obter_recomendacao/<int:user_id>/<int:user_tipo>')
def recomendacao(user_id, user_tipo):
    cursor = conn.cursor()

    if user_tipo == 1:
        query = 'select * from shae_db.v_obterConteudosPorUsuarioFeed where user_id = {0} and user_tipo = {1}'.format(
            user_id, user_tipo)
        cursor.execute(query)
        conteudosProf = cursor.fetchall()
        return jsonify(conteudosProf)
    else:

    ############################### Motor de Adaptação - 1º Acesso - Questionário #######################
    #####################################################################################################

        qt_conteudo_feed = 16

        query_quest = 'select * from ( '
        query_quest += 'select qp.user_id_aprendiz,qp.tipo_alternativa,sum(qp.peso) as somaDosPesos '
        query_quest += 'from shae_db.questionario_pedagogico qp '
        query_quest += 'where id in (1,15,16,17,18,19,20,21,22)'
        query_quest += 'group by user_id_aprendiz, tipo_alternativa '
        query_quest += ') as soma_resp_questionario where user_id_aprendiz = {0}'.format(user_id)

        dfquest = pd.read_sql(query_quest, conn)

        dfquest.columns = ["user_id_aprendiz", "tipo_alternativa", "peso"]

        dfquest['percent_peso'] = dfquest.peso / (dfquest.peso.sum())

        dfquest['prox_feed'] = dfquest.percent_peso * qt_conteudo_feed

        dfquest['prox_feed'] = round(dfquest['prox_feed'])

        dfquest['prox_feed'] = dfquest['prox_feed'].astype(int)

        ## Qtd de conteúdos por tipo
        qtdTexto = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "texto"].iloc[0]
        qtdTeste = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "teste"].iloc[0]
        qtdAudio = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "audio"].iloc[0]
        qtdVideo = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "video"].iloc[0]


        ########################### Esquema de seleção de conteúdo no banco #################################
        #####################################################################################################


        query = 'select * from shae_db.v_obterConteudosComProfessores'

        dfConteudos = pd.read_sql(query, conn)
        dfConteudos.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url","id_especialista","nome_especialista","user_tipo"]

        conteudosTexto = dfConteudos.where(dfConteudos.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdTexto)
        conteudosTeste = dfConteudos.where(dfConteudos.tipo == "questionario").dropna(subset=["idConteudo"]).head(qtdTeste)
        conteudosAudio = dfConteudos.where(dfConteudos.tipo == "imagem").dropna(subset=["idConteudo"]).head(qtdAudio)
        conteudosVideo = dfConteudos.where(dfConteudos.tipo == "video").dropna(subset=["idConteudo"]).head(qtdVideo)

        ### JSON com a relação de contéudos
        conteudosFiltrados = pd.concat([conteudosTexto, conteudosVideo, conteudosTeste, conteudosAudio])
        conteudosFiltrados = conteudosFiltrados.sample(frac=1)



        # print(conteudosFiltrados)
        for indice in conteudosFiltrados.index:
            query = """INSERT INTO shae_db.ultimo_feed (id_feed, id_aprendiz, id_conteudo, consumido)
                    VALUES({0}, {1}, {2}, false);""".format(1, user_id, conteudosFiltrados["idConteudo"][indice])
            cursor.execute(query)
            conn.commit()

        return conteudosFiltrados.to_json(orient="records", force_ascii=False)

            # .to_json(orient="records", force_ascii=False)


@app.route('/obter_feed/<int:user_id>/<int:user_tipo>')
def feed(user_id, user_tipo):
    cursor = conn.cursor()

    if user_tipo == 1:
        query = 'select distinct * from shae_db.v_obterConteudosPorUsuarioFeed where user_id = {0} and user_tipo = {1}'.format(
            user_id, user_tipo)
        cursor.execute(query)
        conteudosProf = cursor.fetchall()
        return jsonify(conteudosProf)
    else:

        qt_conteudo_feed = 16

        ################################ Base armazenada - com o último feed ################################
        #####################################################################################################

        query_feed_disp = 'SELECT DISTINCT ult.id_feed, ult.id_aprendiz, ult.id_conteudo,cont.tipo, ult.consumido '
        query_feed_disp += 'FROM shae_db.ultimo_feed as ult '
        query_feed_disp += 'left join shae_db.conteudo as cont on cont.id_conteudo = ult.id_conteudo'

        df_feed_disp = pd.read_sql(query_feed_disp, conn)

        df_feed_disp['consumido'] = df_feed_disp['consumido'].astype(int)

        feed_disp_consumido = pd.DataFrame({'id_conteudos': df_feed_disp.id_conteudo,
                                            'tipo': df_feed_disp.tipo,
                                            'consumo': df_feed_disp.consumido})

        ## Selecionando conteúdos que não foram disponibilizados

        conteudos_disp_ult = feed_disp_consumido['id_conteudos']
        conteudos_disp_ult_list = conteudos_disp_ult.values.tolist()

        tam_list = int(len(conteudos_disp_ult_list)/2)
        conteudos_disp_ult_list = random.sample(conteudos_disp_ult_list, tam_list)

        conteudos_consumo = feed_disp_consumido['id_conteudos'].loc[feed_disp_consumido['consumo'] == 1]
        conteudos_consumo_list = conteudos_consumo.values.tolist()



        if len(conteudos_consumo_list) == 0:
            query_prox_conteudo = 'select distinct * from shae_db.v_obterConteudosComProfessores'
            query_prox_conteudo += ' where id_Conteudo not in ({0})'.format(str(conteudos_disp_ult_list)[1:-1])

        else:
            conteudos_disp_ult_list = conteudos_disp_ult_list+conteudos_consumo_list


            query_prox_conteudo = 'select distinct * from shae_db.v_obterConteudosComProfessores'
            query_prox_conteudo += ' where id_Conteudo not in ({0})'.format(str(conteudos_disp_ult_list)[1:-1])

            print(conteudos_disp_ult_list)




        consumo = pd.DataFrame({'qt_consumo': feed_disp_consumido.groupby('tipo')['consumo'].sum()}).reset_index()

        feed_disp_arm = pd.DataFrame({'qt_disp': feed_disp_consumido.groupby(['tipo'])['tipo'].size()}).reset_index()

        merge_feed = pd.merge(feed_disp_arm, consumo, how="left", on='tipo')

        ################################ Motor de Adaptação - do prox feed-inicial 1 ###############
        #####################################################################################################

        ## % De consumo
        percent_consumo = (merge_feed['qt_consumo'].sum() / merge_feed['qt_disp'].sum())

        merge_feed['Disp_vs_consumo'] = (merge_feed['qt_disp']*percent_consumo)

        ## % De consumo real
        merge_feed['consumo_real'] = (merge_feed['qt_consumo']/merge_feed['Disp_vs_consumo'])

        ## Caso o consumo real for 0, feed não consumido pegar a distribuição anterior
        merge_feed['consumo_real'] = (merge_feed['qt_consumo'] / merge_feed['Disp_vs_consumo'])


        ## Aplicando o consumo real ao que foi disponibilizado, gerando a próxima disponibilização
        merge_feed['prox_feed'] = (merge_feed['qt_disp'] * merge_feed['consumo_real'])

        merge_feed['prox_feed'].loc[merge_feed['prox_feed'].isnull()] = merge_feed['qt_disp']

        ## Caso o consumo de algum dos tipos tenha sido zero, é atribuido o que falta para totalizar o próx fedd
        merge_feed['prox_feed'].loc[merge_feed['prox_feed'] == 0] = (qt_conteudo_feed - merge_feed['prox_feed'].sum())

        print(merge_feed)

        ################### Esquema de seleção de conteúdo no banco - prox feed - 1 #########################
        #####################################################################################################

        dfConteudos_prox = pd.read_sql(query_prox_conteudo, conn)
        dfConteudos_prox.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url","id_especialista","nome_especialista","user_tipo","id_feed"]

        qtdtexto_prox = (merge_feed['prox_feed'].loc[merge_feed['tipo'] == "texto"]).tolist()
        if len(qtdtexto_prox) == 0:
            qtdtexto_prox = 1
        else:
            qtdtexto_prox = int(float(str(qtdtexto_prox)[1:-1]))

        qtdteste_prox = (merge_feed['prox_feed'].loc[merge_feed['tipo'] == "questionario"]).tolist()
        if len(qtdteste_prox) == 0:
            qtdteste_prox = 1
        else:
            qtdteste_prox = int(float(str(qtdteste_prox)[1:-1]))

        qtdaudio_prox = (merge_feed['prox_feed'].loc[merge_feed['tipo'] == "imagem"]).tolist()
        if len(qtdaudio_prox) == 0:
            qtdaudio_prox = 1
        else:
            qtdaudio_prox = int(float(str(qtdaudio_prox)[1:-1]))

        qtdvideo_prox = merge_feed['prox_feed'].loc[merge_feed['tipo'] == "video"].tolist()
        if len(qtdvideo_prox) == 0:
            qtdvideo_prox = 1
        else:
            qtdvideo_prox = int(float(str(qtdvideo_prox)[1:-1]))

        conteudosTexto_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdtexto_prox)
        conteudosTeste_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "questionario").dropna(subset=["idConteudo"]).head(qtdteste_prox)
        conteudosAudio_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "imagem").dropna(subset=["idConteudo"]).head(qtdaudio_prox)
        conteudosVideo_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "video").dropna(subset=["idConteudo"]).head(qtdvideo_prox)

        #
        conteudosFiltrados_prox = pd.concat([conteudosTexto_prox, conteudosVideo_prox, conteudosTeste_prox, conteudosAudio_prox])
        conteudosFiltrados_prox = conteudosFiltrados_prox.sample(frac=1)

        ################################ Salvando bases ################################
        #####################################################################################################

        query_delete_feed = """ DELETE FROM shae_db.ultimo_feed;"""
        cursor.execute(query_delete_feed)
        conn.commit()

        for indice in conteudosFiltrados_prox.index:
            query = """INSERT INTO shae_db.ultimo_feed (id_feed, id_aprendiz, id_conteudo, consumido)
                      VALUES({0}, {1}, {2}, false);""".format(1, user_id, conteudosFiltrados_prox["idConteudo"][indice])

            query2 = """INSERT INTO shae_db.ultimo_feed (id_feed, id_aprendiz, id_conteudo, consumido)
                                   VALUES({0}, {1}, {2}, false);""".format(1, user_id,
                                                                           conteudosFiltrados_prox["idConteudo"][indice])

            cursor.execute(query)
            conn.commit()

    return conteudosFiltrados_prox.to_json(orient="records", force_ascii=False)



# @app.route('/obter_feed/<int:user_id>/<int:user_tipo>')
# def feed(user_id, user_tipo):
#     return #.to_json(orient="records", force_ascii=False)

if __name__ == '__main__':
    app.run(host='127.0.0.2', port=5000)
