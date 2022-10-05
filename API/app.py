import psycopg2
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
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

        query = 'select * from ( '
        query += 'select qp.user_id_aprendiz,qp.tipo_alternativa,sum(qp.peso) as somaDosPesos '
        query += 'from shae_db.questionario_pedagogico qp '
        query += 'group by user_id_aprendiz, tipo_alternativa '
        query += ') as soma_resp_questionario where user_id_aprendiz = {0}'.format(user_id)

        cursor.execute(query)

        respQuestionario = cursor.fetchall()
        somaPesos = 0

        for indice in respQuestionario:
            print(indice)
            somaPesos += indice[2]

        # // (4, 'texto', 4)
        # // (4, 'teste', 3)
        # // (4, 'audio', 2)
        # // (4, 'video', 1)
        qtdConteudo = [['texto', 0.0], ['teste', 0.0], ['audio', 0.0], ['video', 0.0]]
        print(somaPesos)
        print(qtdConteudo)
        indice = 0

        for x in respQuestionario:
            print(qtdConteudo[indice])
            qtdConteudo[indice][1] = x[2] / somaPesos
            indice += 1

        print(qtdConteudo)
        ##[['texto', 0.4],
        #  ['teste', 0.3],
        #  ['audio', 0.2],
        #  ['video', 0.1]]

        nTotalConteudos = 20
        indice = 0
        for y in qtdConteudo:
            qtdConteudo[indice][1] = y[1] * nTotalConteudos
            indice += 1

        print(qtdConteudo)


        ##[['texto', 8.0],
        #  ['teste', 6.0],
        #  ['audio', 4.0],
        #  ['video', 2.0]]

        # query = 'select * from shae_db.conteudo'
        # cursor.execute(query)
        # listaConteudos = cursor.fetchall()
        # print(listaConteudos)

        ## saida json
        # [
        #     1,
        #     "video Algebra",
        #     "video",
        #     1,
        #     1,
        #     null,
        #     "https://www.youtube.com/watch?v=i7MZpiRht2E"
        # ]

        ## valor conteudos
        qtdTexto = int(qtdConteudo[0][1])
        qtdTeste = int(qtdConteudo[1][1])
        qtdAudio = int(qtdConteudo[2][1])
        qtdVideo = int(qtdConteudo[3][1])



        # print('||||||||||||||||||||||||')
        # print(qtdTexto)
        # print(qtdTeste)
        # print(qtdAudio)
        # print(qtdVideo)
        # print('||||||||||||||||||||||||')

        # resp_questionario = pd.read_sql(query,conn)
        # resp_questionario.columns = ["aluno_id","tipo","peso"]
        # print(resp_questionario)


        # query = 'select * from shae_db.conteudo'
        
        query = 'select * from shae_db.v_obterConteudosComProfessores'

        dfConteudos = pd.read_sql(query, conn)
        dfConteudos.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url","id_especialista","nome_especialista","user_tipo"]
        conteudosTexto = dfConteudos.where(dfConteudos.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdTexto)
        conteudosTeste = dfConteudos.where(dfConteudos.tipo == "teste").dropna(subset=["idConteudo"]).head(qtdTeste)
        conteudosAudio = dfConteudos.where(dfConteudos.tipo == "audio").dropna(subset=["idConteudo"]).head(qtdAudio)
        conteudosVideo = dfConteudos.where(dfConteudos.tipo == "video").dropna(subset=["idConteudo"]).head(qtdVideo)

        print(conteudosTexto)
        print("||||||||||||||||||||||||||")
        print(conteudosTeste)
        print("||||||||||||||||||||||||||")
        print(conteudosAudio)
        print("||||||||||||||||||||||||||")
        print(conteudosVideo)

        conteudosFiltrados = pd.concat([conteudosTexto, conteudosVideo])
        print("||||||||||||||||||||||||||")
        print(conteudosFiltrados)

        return conteudosFiltrados.to_json(orient="records", force_ascii=False)

            # .to_json(orient="records", force_ascii=False)


@app.route('/obter_feed/<int:user_id>/<int:user_tipo>')
def feed(user_id, user_tipo):
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

        qt_conteudo_feed = 20



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
        qtdtexto = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "texto"].iloc[0]
        qtdteste = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "teste"].iloc[0]
        qtdaudio = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "audio"].iloc[0]
        qtdvideo = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "video"].iloc[0]


        ########################### Esquema de seleção de conteúdo no banco #################################
        #####################################################################################################

        query_conteudo_quest = 'select * from shae_db.conteudo'

        dfConteudos = pd.read_sql(query_conteudo_quest, conn)
        dfConteudos.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url"]

        conteudosTexto = dfConteudos.where(dfConteudos.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdtexto)
        conteudosTeste = dfConteudos.where(dfConteudos.tipo == "questionario").dropna(subset=["idConteudo"]).head(qtdteste)
        conteudosAudio = dfConteudos.where(dfConteudos.tipo == "imagem").dropna(subset=["idConteudo"]).head(qtdaudio)
        conteudosVideo = dfConteudos.where(dfConteudos.tipo == "video").dropna(subset=["idConteudo"]).head(qtdvideo)

        ### JSON com a relação de contéudos
        conteudosFiltrados = pd.concat([conteudosTexto, conteudosVideo, conteudosTeste, conteudosAudio])
        conteudosFiltrados = conteudosFiltrados.sample(frac=1)


        ################################ Base armazenada - com o último feed ################################
        #####################################################################################################

        feed_disp = pd.DataFrame({'id_conteudos': conteudosFiltrados.idConteudo,
                                  'tipo': conteudosFiltrados.tipo})

        feed_disp_arm = pd.DataFrame({'qt_disp': feed_disp.groupby(['tipo'])['tipo'].size()}).reset_index()



        ################################ Base armazenada - com o Disponibilizado e o Consumo  ###############
        #####################################################################################################

        feed_disp_consumido = pd.DataFrame({'id_conteudos': conteudosFiltrados.idConteudo,
                                            'tipo': conteudosFiltrados.tipo,
                                            'consumo': (0, 1, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 0, 1, 0, 0)})

        consumo = pd.DataFrame({'qt_consumo': feed_disp_consumido.groupby('tipo')['consumo'].sum()}).reset_index()

        merge_feed = pd.merge(feed_disp_arm, consumo, how="left", on='tipo')



        ################################ Motor de Adaptação - do prox feed-inicial 1 ###############
        #####################################################################################################

        ## % De consumo
        percent_consumo = (merge_feed['qt_consumo'].sum() / merge_feed['qt_disp'].sum())

        merge_feed['Disp_vs_consumo'] = (merge_feed['qt_disp']*percent_consumo)

        ## % De consumo real
        merge_feed['consumo_real'] = (merge_feed['qt_consumo']/merge_feed['Disp_vs_consumo'])


        ## Aplicando o consumo real ao que foi disponibilizado, gerando a próxima disponibilização
        merge_feed['prox_feed'] = (merge_feed['qt_disp'] * merge_feed['consumo_real'])

        ## Caso o consumo de algum dos tipos tenha sido zero, é atribuido o que falta para totalizar o próx fedd
        merge_feed['prox_feed'].loc[merge_feed['prox_feed'] == 0] = (qt_conteudo_feed - merge_feed['prox_feed'].sum())



        ################### Esquema de seleção de conteúdo no banco - prox feed - 1 #########################
        #####################################################################################################

        ## Selecionando conteúdos que não foram disponibilizados
        conteudos_consumo = feed_disp_consumido['id_conteudos'].loc[feed_disp_consumido['consumo'] == 1]
        conteudos_consumo_list = conteudos_consumo.values.tolist()

        query_prox_conteudo = 'select * from shae_db.conteudo'
        query_prox_conteudo += ' where id_Conteudo not in ({0})'.format(str(conteudos_consumo_list)[1:-1])

        dfConteudos_prox = pd.read_sql(query_prox_conteudo, conn)
        dfConteudos_prox.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url"]

        qtdtexto_prox = (merge_feed['prox_feed'].loc[merge_feed['tipo'] == "texto"]).tolist()
        qtdtexto_prox = int(float(str(qtdtexto_prox)[1:-1]))

        qtdteste_prox = (merge_feed['prox_feed'].loc[merge_feed['tipo'] == "questionario"]).tolist()
        qtdteste_prox = int(float(str(qtdteste_prox)[1:-1]))

        qtdaudio_prox = (merge_feed['prox_feed'].loc[merge_feed['tipo'] == "imagem"]).tolist()
        qtdaudio_prox = int(float(str(qtdaudio_prox)[1:-1]))

        qtdvideo_prox = merge_feed['prox_feed'].loc[merge_feed['tipo'] == "video"].tolist()
        qtdvideo_prox = int(float(str(qtdvideo_prox)[1:-1]))

        conteudosTexto_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdtexto_prox)
        conteudosTeste_prox = dfConteudos_prox.where(dfConteudos.tipo == "questionario").dropna(subset=["idConteudo"]).head(qtdteste_prox)
        conteudosAudio_prox = dfConteudos_prox.where(dfConteudos.tipo == "imagem").dropna(subset=["idConteudo"]).head(qtdaudio_prox)
        conteudosVideo_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "video").dropna(subset=["idConteudo"]).head(qtdvideo_prox)

        #
        conteudosFiltrados_prox = pd.concat([conteudosTexto_prox, conteudosVideo_prox, conteudosTeste_prox, conteudosAudio_prox])
        conteudosFiltrados_prox = conteudosFiltrados_prox.sample(frac=1)


    return conteudosFiltrados_prox.to_json(orient="records", force_ascii=False)

if __name__ == '__main__':
    app.run(host='127.0.0.2', port=5000)
