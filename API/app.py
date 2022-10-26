from flask import Flask, jsonify
from flask_cors import CORS
import consultas as con
import logicaFeed as lF

app = Flask(__name__)
CORS(app)

@app.route('/')
def main():
    query ='select * from shae_db.conteudo'
    lista = con.consulta(query)
    return jsonify(lista)
    # return '<h1>API Iniciada</h1>'


@app.route('/obter_recomendacao/<int:user_id>/<int:user_tipo>')
def recomendacao(user_id, user_tipo):
    if user_tipo == 1:
        return jsonify(con.consultaFeedProf(user_id, user_tipo))
    else:
        conteudosFiltrados = lF.primeiraFase(user_id)
    return conteudosFiltrados.to_json(orient="records", force_ascii=False)

            # .to_json(orient="records", force_ascii=False)


@app.route('/obter_feed/<int:user_id>/<int:user_tipo>')
def feed(user_id, user_tipo):
    if user_tipo == 1:
       return jsonify(con.consultaFeedProf(user_id, user_tipo))
    else:
        conteudosFiltrados_prox = lF.SegundaFase(user_id)
    return conteudosFiltrados_prox.to_json(orient="records", force_ascii=False)




@app.route('/obter_feed_adaptado/<int:user_id>/<int:user_tipo>')
def feed_adaptado(user_id, user_tipo):
    if user_tipo == 1:
       return jsonify(con.consultaFeedProf(user_id, user_tipo))
    else:

       conteudosFiltrados_prox = lF.TerceiraFase(user_id)
    return conteudosFiltrados_prox.to_json(orient="records", force_ascii=False)


if __name__ == '__main__':
    app.run(host='127.0.0.2', port=5000)
