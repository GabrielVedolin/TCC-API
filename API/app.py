import psycopg2
import pandas as pd
from flask import Flask, jsonify

app = Flask(__name__)

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

        query = 'select * from shae_db.conteudo'

        dfConteudos = pd.read_sql(query, conn)
        dfConteudos.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url"]
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

        qt_conteudo_feed = 8

        query_quest = 'select * from ( '
        query_quest += 'select qp.user_id_aprendiz,qp.tipo_alternativa,sum(qp.peso) as somaDosPesos '
        query_quest += 'from shae_db.questionario_pedagogico qp '
        query_quest += 'group by user_id_aprendiz, tipo_alternativa '
        query_quest += ') as soma_resp_questionario where user_id_aprendiz = {0}'.format(user_id)

        dfquest = pd.read_sql(query_quest, conn)

        dfquest.columns = ["user_id_aprendiz", "tipo_alternativa", "peso"]

        dfquest['percent_peso'] = dfquest.peso / (dfquest.peso.sum())

        dfquest['prox_feed'] = dfquest.percent_peso * qt_conteudo_feed

        dfquest['prox_feed'] = round(dfquest['prox_feed'])

        dfquest['prox_feed'] = dfquest['prox_feed'].astype(int)

        qtdtexto = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "texto"].iloc[0]
        qtdteste = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "teste"].iloc[0]
        qtdaudio = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "audio"].iloc[0]
        qtdvideo = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "video"].iloc[0]

        # print(qtdtexto)

        #####################################################################################################
        #####################################################################################################
        #####################################################################################################

        query_conteudo_quest = 'select * from shae_db.conteudo'

        dfConteudos = pd.read_sql(query_conteudo_quest, conn)
        dfConteudos.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url"]

        conteudosTexto = dfConteudos.where(dfConteudos.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdtexto)
        # conteudosTeste = dfConteudos.where(dfConteudos.tipo == "teste").dropna(subset=["idConteudo"]).head(qtdteste)
        # conteudosAudio = dfConteudos.where(dfConteudos.tipo == "audio").dropna(subset=["idConteudo"]).head(qtdaudio)
        conteudosVideo = dfConteudos.where(dfConteudos.tipo == "video").dropna(subset=["idConteudo"]).head(qtdvideo)

        conteudosFiltrados = pd.concat([conteudosTexto, conteudosVideo])#,conteudosTeste,conteudosAudio])

        feed_disp = pd.DataFrame({'id_conteudos': conteudosFiltrados.idConteudo,
                                  'tipo': conteudosFiltrados.tipo})

        feed_disp_arm = pd.DataFrame({'tipo': conteudosFiltrados['tipo'].unique(),
                                    'qt_disp': conteudosFiltrados.groupby('tipo')['idConteudo'].count()})

        # feed_disp.set_index = ["tipo", "qt_disp"]

        feed_disp_arm.reset_index(inplace=True, drop=True)

        feed_disp_consumido = pd.DataFrame({'id_conteudos': conteudosFiltrados.idConteudo,
                                  'tipo': conteudosFiltrados.tipo,
                                  'consumo' : (0,1,1,0)})

        consumo = pd.DataFrame({
                        'tipo': feed_disp_consumido['tipo'].unique(),
                        'qt_consumo': feed_disp_consumido.groupby('tipo')['consumo'].sum()})

        # consumo.set_index = ["tipo", "qt_consumo"]

        consumo.reset_index(inplace=True, drop=True)
        #
        merge_feed = pd.merge(feed_disp_arm, consumo, how="left", on='tipo')

        merge_feed['tipo','']

        #
        # concat_feed = pd.concat([feed_disp_arm, consumo]).sort_values(by='tipo')
        # concat_feed = concat_feed[concat_feed['qt_disp'].notna()]

    return merge_feed.to_json(orient="records", force_ascii=False)








app.run()
