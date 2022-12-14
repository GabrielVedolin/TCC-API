import pandas as pd
import random
import numpy as N
from scipy.spatial import distance
import consultas as con


def primeiraFase(user_id):
  ############################### Motor de Adaptação - 1º Acesso - Questionário #######################
  #####################################################################################################

    qt_conteudo_feed = 8

    query_quest = """select * from ( 
                            select qp.user_id_aprendiz,qp.tipo_alternativa,sum(qp.peso) as somaDosPesos 
                            from shae_db.questionario_pedagogico qp 
                            group by user_id_aprendiz, tipo_alternativa 
                      ) as soma_resp_questionario where user_id_aprendiz = {0}""".format(user_id)

    dfquest = con.montaDataFrame(query_quest)
    
    dfquest.columns = ["user_id_aprendiz", "tipo_alternativa", "peso"]

    dfquest['percent_peso'] = dfquest.peso / (dfquest.peso.sum())

    dfquest['prox_feed'] = dfquest.percent_peso * qt_conteudo_feed

    dfquest['prox_feed'] = round(dfquest['prox_feed'])

    dfquest['prox_feed'] = dfquest['prox_feed'].astype(int)
    print(dfquest)
    ## Qtd de conteúdos por tipo
    qtdTexto = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "texto"].iloc[0]
    qtdQuestionario = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "questionario"].iloc[0]
    qtdAudio = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "audio"].iloc[0]
    qtdVideo = dfquest['prox_feed'][dfquest['tipo_alternativa'] == "video"].iloc[0]

    # print(qtdTexto, qtdVideo, qtdAudio, qtdTeste)

    ########################### Esquema de seleção de conteúdo no banco #################################
    #####################################################################################################
    
    conteudosFiltrados = con.obterConteudoFiltradoFeed(qtdTexto,qtdQuestionario,qtdAudio,qtdVideo)
    # print(conteudosFiltrados.index)
    #[(1, user_id, idConteudo,false)]
    con.inserirUltimoFeed(1,user_id,conteudosFiltrados)

    conteudosFiltrados = con.obterConteudoFiltradoFeed(qtdTexto,qtdQuestionario,qtdAudio,qtdVideo)

    return conteudosFiltrados


def SegundaFase(user_id):
    qt_conteudo_feed = 8

    ################################ Base armazenada - com o último feed ################################
    #####################################################################################################

    query_feed_disp = """SELECT DISTINCT ult.id_feed, ult.id_aprendiz, ult.id_conteudo,cont.tipo, ult.consumido 
                            FROM shae_db.ultimo_feed as ult 
                            left join shae_db.conteudo as cont on cont.id_conteudo = ult.id_conteudo
                            where id_aprendiz = {0} """.format(user_id)

    df_feed_disp = con.montaDataFrame(query_feed_disp)

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
        conteudos_disp_ult_list = N.unique(conteudos_disp_ult_list)
        conteudos_disp_ult_list = conteudos_disp_ult_list.tolist()

        query_prox_conteudo = 'select distinct * from shae_db.v_obterConteudosComProfessores'
        query_prox_conteudo += ' where id_Conteudo not in ({0})'.format(str(conteudos_disp_ult_list)[1:-1])

        # print(conteudos_disp_ult_list)

    # print(query_prox_conteudo)

    ################################ Motor de Adaptação - do prox feed-inicial 1 ###############
    #####################################################################################################

    consumo = pd.DataFrame({'qt_consumo': feed_disp_consumido.groupby('tipo')['consumo'].sum()}).reset_index()

    feed_disp_arm = pd.DataFrame({'qt_disp': feed_disp_consumido.groupby(['tipo'])['tipo'].size()}).reset_index()

    merge_feed = pd.merge(feed_disp_arm, consumo, how="left", on='tipo')

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

    dfConteudos_prox = con.montaDataFrame(query_prox_conteudo)
    dfConteudos_prox.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url","id_especialista","nome_especialista","user_tipo","id_feed"]

    ## Setando a quantide de conteúdos por tipo
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
    ##

    ## Selecionando as quantidades de conteúdo
    conteudosTexto_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdtexto_prox)
    conteudosTeste_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "questionario").dropna(subset=["idConteudo"]).head(qtdteste_prox)
    conteudosAudio_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "imagem").dropna(subset=["idConteudo"]).head(qtdaudio_prox)
    conteudosVideo_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "video").dropna(subset=["idConteudo"]).head(qtdvideo_prox)

    #
    conteudosFiltrados_prox = pd.concat([conteudosTexto_prox, conteudosVideo_prox, conteudosTeste_prox, conteudosAudio_prox])
    conteudosFiltrados_prox = conteudosFiltrados_prox.sample(frac=1)

    ################################ Salvando bases ################################
    #####################################################################################################

    Id_feed = df_feed_disp['id_feed'].nunique()


    ## Armazenando na base de histórico - antigo
    con.inserirHistoricoFeed(Id_feed, user_id, feed_disp_consumido)

    ## Deletando feed antigo
    con.excluirUltimoFeed()

    ## Inserindo feed novo
    con.inserirUltimoFeed(2, user_id, conteudosFiltrados_prox)
    return conteudosFiltrados_prox

def TerceiraFase(user_id):
    qt_conteudo_feed = 8

    ################################ Base armazenada - com o último feed ################################
    #####################################################################################################

    query_feed_disp = """SELECT DISTINCT ult.id_feed, ult.id_aprendiz, ult.id_conteudo,cont.tipo, ult.consumido
                            FROM shae_db.ultimo_feed as ult 
                            left join shae_db.conteudo as cont on cont.id_conteudo = ult.id_conteudo
                            where id_aprendiz = {0} """.format(user_id)

    df_feed_disp = con.montaDataFrame(query_feed_disp) 
    df_feed_disp['consumido'] = df_feed_disp['consumido'].astype(int)

    feed_disp_consumido = pd.DataFrame({'id_conteudos': df_feed_disp.id_conteudo,
                                        'tipo': df_feed_disp.tipo,
                                        'consumo': df_feed_disp.consumido})
    Id_feed = df_feed_disp['id_feed'].nunique()

    ## Armazenando na base de histórico - novo
    con.inserirHistoricoFeed(Id_feed,user_id,feed_disp_consumido)

    # Deletando feed antigo
    con.excluirUltimoFeed()
    ##Selecionando feed armazenado
    query_feed_disp_arm = """Select hist.id_feed as id_feed, cont.tipo as tipo,
                                count(hist.id_conteudo) as qt_disp,
                                sum(cast(hist.consumido as integer)) as consumido
                                from shae_db.historico_feed as hist
                                left join shae_db.conteudo as cont on cont.id_conteudo = hist.id_conteudo
                                where hist.id_aprendiz = {0}
                                group by hist.id_feed,cont.tipo""".format(user_id)

    df_feed_disp_arm = con.montaDataFrame(query_feed_disp_arm)
    df_feed_disp_arm['consumido'] = df_feed_disp_arm['consumido'] .astype(int)

    #criando index
    index = []
    for x in df_feed_disp_arm.index:
        index.append(x)
    ## feed histórico
    feed_disp_consumido_arm = pd.DataFrame({'id_feed': df_feed_disp_arm.id_feed,
                                            'tipo':  df_feed_disp_arm.tipo,
                                            'qt_disp': df_feed_disp_arm.qt_disp,
                                            'consumo': df_feed_disp_arm.consumido})

    feed_disp_consumido_arm.columns = ['id_feed', 'tipo', 'qt_disp', 'consumo']

    feed_disp_consumido_arm_id_quest = pd.DataFrame({'id_feed': feed_disp_consumido_arm['id_feed'].nunique()}, index=[index])
    feed_disp_consumido_arm_id_quest = feed_disp_consumido_arm_id_quest.head(1)
    feed_disp_consumido_arm_id_quest = feed_disp_consumido_arm_id_quest['id_feed']
    feed_disp_consumido_arm_id_quest = feed_disp_consumido_arm_id_quest.values.tolist()

    feed_disp_consumido_arm_id = pd.DataFrame({'id_feed' :feed_disp_consumido_arm['id_feed'].nunique()}, index=[index])
    feed_disp_consumido_arm_id = feed_disp_consumido_arm_id.tail(1)
    feed_disp_consumido_arm_id = feed_disp_consumido_arm_id['id_feed']
    feed_disp_consumido_arm_id = feed_disp_consumido_arm_id.values.tolist()

    #questionario
    consumo1 = int(float(str(feed_disp_consumido_arm_id_quest)[1:-1]))
    # ult_feed
    consumo2 = int(float(str(feed_disp_consumido_arm_id)[1:-1]))

    feed_disp_consumido_arm1 = feed_disp_consumido_arm.loc[feed_disp_consumido_arm['id_feed'] == consumo1]
    fdc_arm1_texto = feed_disp_consumido_arm1['qt_disp'].loc[feed_disp_consumido_arm1['tipo'] == 'texto']
    fdc_arm1_teste = feed_disp_consumido_arm1['qt_disp'].loc[feed_disp_consumido_arm1['tipo'] == 'questionario']
    fdc_arm1_audio = feed_disp_consumido_arm1['qt_disp'].loc[feed_disp_consumido_arm1['tipo'] == 'imagem']
    fdc_arm1_video = feed_disp_consumido_arm1['qt_disp'].loc[feed_disp_consumido_arm1['tipo'] == 'video']

    feed_disp_consumido_arm2 = feed_disp_consumido_arm.loc[feed_disp_consumido_arm['id_feed'] == consumo2]
    fdc_arm2_texto = feed_disp_consumido_arm2['qt_disp'].loc[feed_disp_consumido_arm2['tipo'] == 'texto']
    fdc_arm2_teste = feed_disp_consumido_arm2['qt_disp'].loc[feed_disp_consumido_arm2['tipo'] == 'questionario']
    fdc_arm2_audio = feed_disp_consumido_arm2['qt_disp'].loc[feed_disp_consumido_arm2['tipo'] == 'imagem']
    fdc_arm2_video = feed_disp_consumido_arm2['qt_disp'].loc[feed_disp_consumido_arm2['tipo'] == 'video']

    ## Feed _ atual
    consumo = pd.DataFrame({'qt_consumo': feed_disp_consumido.groupby('tipo')['consumo'].sum()}).reset_index()
    feed_disp_consumido_atual = pd.DataFrame({'qt_disp': feed_disp_consumido.groupby(['tipo'])['tipo'].size()}).reset_index()
    feed_disp_consumido_atual = pd.merge(feed_disp_consumido_atual, consumo, how="left", on='tipo')

    fdc_atual_texto = feed_disp_consumido_atual['qt_disp'].loc[feed_disp_consumido_atual['tipo'] == 'texto']
    fdc_atual_teste = feed_disp_consumido_atual['qt_disp'].loc[feed_disp_consumido_atual['tipo'] == 'questionario']
    fdc_atual_audio = feed_disp_consumido_atual['qt_disp'].loc[feed_disp_consumido_atual['tipo'] == 'imagem']
    fdc_atual_video = feed_disp_consumido_atual['qt_disp'].loc[feed_disp_consumido_atual['tipo'] == 'video']

    ## Calculando a distancia entre o penúltimo feed e o 1º
    dist1_texto = distance.euclidean(fdc_arm1_texto, fdc_arm2_texto)
    dist1_teste = distance.euclidean(fdc_arm1_teste, fdc_arm2_teste)
    dist1_audio = distance.euclidean(fdc_arm1_audio, fdc_arm2_audio)
    dist1_video = distance.euclidean(fdc_arm1_video, fdc_arm2_video)

    ## Calculando a distancia entre o último feed e o 1º
    dist2_texto = distance.euclidean(fdc_arm1_texto, fdc_atual_texto)
    dist2_teste = distance.euclidean(fdc_arm1_teste, fdc_atual_teste)
    dist2_audio = distance.euclidean(fdc_arm1_audio, fdc_atual_audio)
    dist2_video = distance.euclidean(fdc_arm1_video, fdc_atual_video)


    ## Definindo qual a distância mais próxima do 1º feed atribuindo para o calculo do próximo

    if dist1_texto < dist2_texto:
        fdc_final_texto = feed_disp_consumido_arm1.loc[feed_disp_consumido_arm1['tipo'] == 'texto']
    else:
        fdc_final_texto = feed_disp_consumido_arm2.loc[feed_disp_consumido_arm2['tipo'] == 'texto']

    if dist1_teste < dist2_teste:
        fdc_final_teste = feed_disp_consumido_arm1.loc[feed_disp_consumido_arm1['tipo'] == 'questionario']
    else:
        fdc_final_teste = feed_disp_consumido_arm2.loc[feed_disp_consumido_arm2['tipo'] == 'questionario']

    if dist1_audio < dist2_audio:
        fdc_final_audio = feed_disp_consumido_arm1.loc[feed_disp_consumido_arm1['tipo'] == 'imagem']
    else:
        fdc_final_audio = feed_disp_consumido_arm2.loc[feed_disp_consumido_arm2['tipo'] == 'imagem']

    if dist1_video < dist2_video:
        fdc_final_video = feed_disp_consumido_arm1.loc[feed_disp_consumido_arm1['tipo'] == 'video']
    else:
        fdc_final_video = feed_disp_consumido_arm2.loc[feed_disp_consumido_arm2['tipo'] == 'video']

    ## feed_ADAPATADO_FINAL
    fdc_final = pd.concat([fdc_final_texto, fdc_final_teste, fdc_final_audio, fdc_final_video])



    ################################ Motor de Adaptação - do prox feed_adaptado ###############
    #####################################################################################################
    # % De consumo

    print(feed_disp_consumido_atual['qt_consumo'].sum())
    print(fdc_final['qt_disp'].sum())
    #
    percent_consumo = (feed_disp_consumido_atual['qt_consumo'].sum() / fdc_final['qt_disp'].sum())
    print(percent_consumo)
    #
    fdc_final['Disp_vs_consumo'] = (fdc_final['qt_disp']*percent_consumo)

    ## % De consumo real
    fdc_final['consumo_real'] = (feed_disp_consumido_atual['qt_consumo']/fdc_final['Disp_vs_consumo'])

    ## Caso o consumo real for 0, feed não consumido pegar a distribuição anterior
    fdc_final['consumo_real'] = (feed_disp_consumido_atual['qt_consumo']/ fdc_final['Disp_vs_consumo'])


    ## Aplicando o consumo real ao que foi disponibilizado, gerando a próxima disponibilização
    fdc_final['prox_feed'] = (fdc_final['qt_disp'] * fdc_final['consumo_real'])

    fdc_final['prox_feed'].loc[fdc_final['prox_feed'].isnull()] = fdc_final['qt_disp']
    #
    ## Caso o consumo de algum dos tipos tenha sido zero, é atribuido o que falta para totalizar o próx fedd
    fdc_final['prox_feed'].loc[fdc_final['prox_feed'] == 0] = (qt_conteudo_feed - fdc_final['prox_feed'].sum())


    #####################################################################################################
    ## Selecionando conteúdos que não foram disponibilizados

    query_cont_amr = """Select hist.*, cont.tipo
    from shae_db.historico_feed as hist
    left join shae_db.conteudo as cont ON cont.id_conteudo = hist.id_conteudo
    where hist.id_aprendiz = {0}""".format(user_id)

    df_cont_amr = con.montaDataFrame(query_cont_amr)
    df_cont_amr = pd.DataFrame({'id_feed': df_cont_amr.id_feed,
                                'tipo':  df_cont_amr.tipo,
                                'id_conteudo': df_cont_amr.id_conteudo,
                                'consumido': df_cont_amr.consumido})

    conteudos_disp_ult = df_cont_amr['id_conteudo']
    conteudos_disp_ult_list = conteudos_disp_ult.values.tolist()

    tam_list = int(len(conteudos_disp_ult_list)/2)
    conteudos_disp_ult_list = random.sample(conteudos_disp_ult_list, tam_list)

    conteudos_consumo = df_cont_amr['id_conteudo'].loc[df_cont_amr['consumido'] == 1]
    conteudos_consumo_list = conteudos_consumo.values.tolist()

    if len(conteudos_consumo_list) == 0:
        query_prox_conteudo = 'select distinct * from shae_db.v_obterConteudosComProfessores'
        query_prox_conteudo += ' where id_Conteudo not in ({0})'.format(str(conteudos_disp_ult_list)[1:-1])

    else:
        conteudos_disp_ult_list = conteudos_disp_ult_list+conteudos_consumo_list
        conteudos_disp_ult_list = N.unique(conteudos_disp_ult_list)
        conteudos_disp_ult_list = conteudos_disp_ult_list.tolist()

        query_prox_conteudo = 'select distinct * from shae_db.v_obterConteudosComProfessores'
        query_prox_conteudo += ' where id_Conteudo not in ({0})'.format(str(conteudos_disp_ult_list)[1:-1])



    ################### Esquema de seleção de conteúdo no banco - prox feed - 1 #########################
    #####################################################################################################

    dfConteudos_prox = con.montaDataFrame(query_prox_conteudo)
    dfConteudos_prox.columns = ["idConteudo", "descricao", "tipo", "ordem", "idTopico", "descricao_texto", "url","id_especialista","nome_especialista","user_tipo","id_feed"]

    ## Setando a quantide de conteúdos por tipo
    qtdtexto_prox = (fdc_final['prox_feed'].loc[fdc_final['tipo'] == "texto"]).tolist()
    if len(qtdtexto_prox) == 0:
        qtdtexto_prox = 1
    else:
        qtdtexto_prox = int(float(str(qtdtexto_prox)[1:-1]))

    qtdteste_prox = (fdc_final['prox_feed'].loc[fdc_final['tipo'] == "questionario"]).tolist()
    if len(qtdteste_prox) == 0:
        qtdteste_prox = 1
    else:
        qtdteste_prox = int(float(str(qtdteste_prox)[1:-1]))

    qtdaudio_prox = (fdc_final['prox_feed'].loc[fdc_final['tipo'] == "imagem"]).tolist()
    if len(qtdaudio_prox) == 0:
        qtdaudio_prox = 1
    else:
        qtdaudio_prox = int(float(str(qtdaudio_prox)[1:-1]))

    qtdvideo_prox = fdc_final['prox_feed'].loc[fdc_final['tipo'] == "video"].tolist()
    if len(qtdvideo_prox) == 0:
        qtdvideo_prox = 1
    else:
        qtdvideo_prox = int(float(str(qtdvideo_prox)[1:-1]))
    ##

    ## Selecionando as quantidades de conteúdo
    conteudosTexto_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "texto").dropna(subset=["idConteudo"]).head(qtdtexto_prox)
    conteudosTeste_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "questionario").dropna(subset=["idConteudo"]).head(qtdteste_prox)
    conteudosAudio_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "imagem").dropna(subset=["idConteudo"]).head(qtdaudio_prox)
    conteudosVideo_prox = dfConteudos_prox.where(dfConteudos_prox.tipo == "video").dropna(subset=["idConteudo"]).head(qtdvideo_prox)

    #
    conteudosFiltrados_prox = pd.concat([conteudosTexto_prox, conteudosVideo_prox, conteudosTeste_prox, conteudosAudio_prox])
    conteudosFiltrados_prox = conteudosFiltrados_prox.sample(frac=1)
    consumo2 = consumo2 + 1
    # Inserindo feed novo
    con.inserirUltimoFeed(consumo2, user_id, conteudosFiltrados_prox)

    return conteudosFiltrados_prox