import psycopg2
import pandas as pd
from flask import Flask, jsonify

app = Flask(__name__)


#config db
host = 'ec2-50-19-255-190.compute-1.amazonaws.com'
dbname = 'de67a2stf4tsc2'
user = 'gadpffmonctxbl'
password = '8908b69b11b078c19ee8e791042bd7ee0a24fc9729062c77fea0791c91dc6c02'
ssl = 'require'

#string conexao
conn_string = 'host={0} user={1} dbname={2} password={3} sslmode={4}'.format(host,user,dbname,password,ssl)

# #config db
# host = 'localhost'
# dbname = 'postgres'
# user = 'postgres'
# password = '1234'

# #string conexao
# conn_string = 'host={0} user={1} dbname={2} password={3}'.format(host, user, dbname, password)

conn = psycopg2.connect(conn_string)

@app.route('/')
def ola():
    cursor = conn.cursor()
    cursor.execute('select * from shae_db.conteudo')
    lista = cursor.fetchall()
    return jsonify(lista)
    #return '<h1>API Iniciada</h1>'

@app.route('/obter_recomendacao/<int:user_id>')
def recomendacao(user_id):
    cursor = conn.cursor()
    cursor.execute('select * from v_obterConteudosPorAlunoFeed where user_id = {0}'.format(user_id))
    lista = cursor.fetchall()
    return jsonify(lista)

app.run()