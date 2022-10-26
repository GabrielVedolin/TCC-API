import psycopg2

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