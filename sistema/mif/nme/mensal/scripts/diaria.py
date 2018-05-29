# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import unicode_literals
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import when, expr, concat, lit
from decimal import Decimal
import deserialize
import sys

def run(data, process):
    print("Start nme {0}".format(process))

    table_name_generic = 'MS0_DT_DWH_GENERIC_RESULT_DIA'
    table_name_result = 'MS0_FT_DWH_BLCE_RESULT_DIA'
    table_name_client = 'MS0_DT_DWH_CLIENTE_RESULT_DIA'

    path_target_hier = '/sistemas/mif/input/hierarquia'
    path_target_para = '/sistemas/mif/input/parametros'
    path_target_fx2 = '/sistemas/ods/tgtfiles/estrutural'
    path_target_fx = '/sistemas/mif/input/parametros'
    path_sqoop = '/sistemas/mif/sqoop_imported'

    ###########################################################################

    """Funcoes"""

    """Le arquivos no formato que voce escolher"""
    def read(path_target, data, file, format):
        return sqlContext.read.format(format).load(
            path_target + '/' + file + '/' + data + '/*.parquet')
       
    """Le arquivos com o textFile"""
    def read_textFile(path_target, file):
        return sc.textFile(path_target + '/' + file, num_partitions, True)
        
    """Funcao para retirar espaco em branco nas colunas"""
    def retiraEspacos(row):
        dyct = row.asDict()
        novo_dyct = {}
        for campo, valor in dyct.iteritems():
            campo = campo.lower()
            try:
                valor = str(valor).strip()
                novo_dyct[campo] = valor
            except Exception as e:
                novo_dyct[campo] = ''
        return Row(**novo_dyct)

    """Save the file in .txt"""
    def copyMerge(src_dir, dst_file, overwrite, deleteSource, debug):
        # this function has been migrated to https://github.com/Tagar/abalon Python package

        hadoop = sc._jvm.org.apache.hadoop
        conf = hadoop.conf.Configuration()
        fs = hadoop.fs.FileSystem.get(conf)

        # check files that will be merged
        files = []
        for f in fs.listStatus(hadoop.fs.Path(src_dir)):
            if f.isFile():
                files.append(f.getPath())
        if not files:
            raise ValueError("Source directory {} is empty".format(src_dir))
        files.sort(key=lambda f: str(f))

        # dst_permission = hadoop.fs.permission.FsPermission.valueOf(permission)      # , permission='-rw-r-----'
        out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)

        try:
            # loop over files in alphabetical order and append them one by one to the target file
            for file in files:
                if debug:
                    print("Appending file {} into {}".format(file, dst_file))

                in_stream = fs.open(file)  # InputStream object
                try:
                    hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf,
                                                False)  # False means don't close out_stream
                finally:
                    in_stream.close()
        finally:
            out_stream.close()

        if deleteSource:
            fs.delete(hadoop.fs.Path(src_dir), True)  # True=recursive
            if debug:
                print("Source directory {} removed.".format(src_dir))

    ###########################################################################

    """Posicao do Codigo e Descricao das Hierarquias de Negocio e Carteira"""
    pos_desc_arn = [(1533, 1632), (1433, 1532), (1333, 1432), (1233, 1332), (1133, 1232), (1033, 1132),
                    (933, 1032), (833, 932), (733, 832), (633, 732), (533, 632), (433, 532), (333, 432),
                    (233, 332)]

    pos_cod_arn = [(218, 232), (203, 217), (188, 202), (173, 187), (158, 172),
                   (143, 157), (128, 142), (113, 127), (98, 112), (83, 97),
                   (68, 82), (53, 67), (38, 52), (23, 37)]

    pos_cod_cart = [(33, 57), (58, 82), (83, 107), (108, 132),
                    (133, 157), (158, 182), (183, 207),
                    (208, 232), (233, 257), (258, 282),
                    (283, 307), (308, 332), (333, 357),
                    (358, 382)]

    pos_desc_cart = [(383, 482), (483, 582), (583, 682), (683, 782), (783, 882),
                     (883, 982), (983, 1082), (1083, 1182), (1183, 1282),
                     (1283, 1382), (1383, 1482), (1483, 1582), (1583, 1682),
                     (1683, 1782)]

    """Posicao da Hierarquia de Produtos na Vertical"""
    POS_COD_PADRE = [3, 22]
    POS_COD_HIJO = [23, 42]
    POS_DES_HIJO = [43, 142]

    ###########################################################################

    """Hierarquias"""

    """Hierarquia de Area de Negocio"""
    def filtro_neg(rdd):
        codigo = rdd[2:7].strip()

        if codigo == "JAN02":
            return True
        else:
            return False

    def position_neg(rdd):
        codigo = ""
        nivel_codigo = []
        nivel_descricao = []

        codigo = rdd[8:23].strip()

        for pos_cod in pos_cod_arn:
            linha = rdd[pos_cod[0]:pos_cod[1]].strip()
            nivel_codigo.append(linha)

        for pos_des in pos_desc_arn:
            linha = rdd[pos_des[0]:pos_des[1]].strip()
            nivel_descricao.append(linha)

        hierarquia = (codigo, nivel_codigo, nivel_descricao)

        return hierarquia

    def make_hierarquia_neg(rdd):
        codigo = rdd[0]
        nivel_codigo = rdd[1]
        nivel_descricao = rdd[2]

        for idx, cod in enumerate(nivel_codigo):
            if codigo == cod:
                try:
                    for desc in nivel_descricao[idx:]:
                        if desc.find('TBP') != -1:
                            pass
                        elif desc.find('BP -') != -1:
                            desc_BP = desc.find('BP -')
                            desc = desc[desc_BP:]
                            return tuple((codigo, desc))
                        elif desc.find('BP –') != -1:
                            desc_BP_ = desc.find('BP –')
                            desc = desc[desc_BP_:]
                            return tuple((codigo, desc))

                    return tuple((codigo, ""))

                except IndexError as e:
                    return tuple((codigo, ""))

        return tuple((codigo, ""))

    rdd_hier_negocio = read_textFile(path_target_hier, 'MS0DMDWHAREANEGOCIOCTR_55_{0}.txt'.format(data))
    rdd_hier_negocio = rdd_hier_negocio.filter(filtro_neg)
    rdd_hier_negocio = rdd_hier_negocio.map(position_neg)
    rdd_hier_negocio = rdd_hier_negocio.map(make_hierarquia_neg)
    df_hier_negocio = sqlContext.createDataFrame(rdd_hier_negocio, ['codigo', 'Area_de_Negocio_Reporting'])

    df_hier_negocio.count()

    """Hierarquia de Carteira"""
    def filtro_carteira(rdd):
        codigo = rdd[2:8].strip()

        if codigo == "JCRGE":
            return True
        else:
            return False

    def position_cart(rdd):
        codigo_nivel = []
        codigo = rdd[8:33].strip()

        for pos_cod in pos_cod_cart:
            linha = rdd[pos_cod[0]:pos_cod[1]].strip()
            codigo_nivel.append(linha)

        hierarquia = (codigo_nivel, codigo)

        return hierarquia

    def make_hier_cart(rdd):
        codigo_nivel = rdd[0]
        codigo = rdd[1]
        descricao = ""

        if codigo == codigo_nivel[3]:
            descricao = codigo_nivel[2]

        elif codigo in (codigo_nivel[2], codigo_nivel[1], codigo_nivel[0]):
            descricao = "GER19999"

        return (codigo, descricao)

    rdd_hier_carteira = read_textFile(path_target_hier, 'MS0DMDWHCARTERAGESTCTR_55_{0}.txt'.format(data))
    rdd_hier_carteira_pv = rdd_hier_carteira
    rdd_hier_carteira = rdd_hier_carteira.filter(filtro_carteira)
    rdd_hier_carteira = rdd_hier_carteira.map(position_cart)
    rdd_hier_carteira = rdd_hier_carteira.map(make_hier_cart)
    df_hier_carteira = sqlContext.createDataFrame(rdd_hier_carteira, ['codigo_cart', 'Ger_Matr'])
    
    df_hier_carteira.count()

    """Hierarquia de Carteira PV"""
    def filtro_carteira_pv(rdd):
        codigo = rdd[2:8].strip()

        if codigo == "JCRPV":
            return True
        else:
            return False

    def position_cart_pv(rdd):
        codigo_nivel = []
        descricao_nivel = []

        codigo = rdd[8:33].strip()

        for pos_cod in pos_cod_cart:
            linha = rdd[pos_cod[0]:pos_cod[1]].strip()
            codigo_nivel.append(linha)

        for pos_des in pos_desc_cart:
            linha = rdd[pos_des[0]:pos_des[1]].strip()
            descricao_nivel.append(linha)

        hierarquia = (codigo, codigo_nivel, descricao_nivel)

        return hierarquia

    def make_hierarquia_cart_pv(rdd):
        codigo = rdd[0]
        codigo_nivel = rdd[1]
        descricao_nivel = rdd[2]
        descricao = ""
        nivel = 1

        while nivel < 14:

            if 'CARTEIRA' in descricao_nivel[nivel]:
                descricao = codigo_nivel[nivel - 1]
                return tuple((codigo, descricao))

            nivel += 1

        if not descricao:
            descricao = codigo

        return tuple((codigo, descricao))

    rdd_hier_carteira_pv = rdd_hier_carteira_pv.filter(filtro_carteira_pv)
    rdd_hier_carteira_pv = rdd_hier_carteira_pv.map(position_cart_pv)
    rdd_hier_carteira_pv = rdd_hier_carteira_pv.map(make_hierarquia_cart_pv)
    df_hier_carteira_pv = sqlContext.createDataFrame(rdd_hier_carteira_pv, ['codigo_cart_pv', 'Cod_Carteira_PV'])

    df_hier_carteira_pv.count()

    """Hierarquia de Produtos"""
    def filtro_hir_vert_prod(rdd):
        codigo = rdd[143:162].strip()
        if codigo == "JPBMG":
            return True
        else:
            return False

    def position_hier_vert_prod(rdd):
        cod_padre = rdd[POS_COD_PADRE[0]:POS_COD_PADRE[1]].strip()
        cod_hijo = rdd[POS_COD_HIJO[0]:POS_COD_HIJO[1]].strip()
        des_hijo = rdd[POS_DES_HIJO[0]:POS_DES_HIJO[1]].strip()

        if ('BP' in des_hijo) and ('TBP' not in des_hijo):
            desc_BP = des_hijo.find('BP')
            des_hijo = des_hijo[desc_BP:]

        return (cod_hijo, cod_padre, des_hijo)

    def monta_hier_vert_prod(rdd):
        def filtro_BP(x):
            desc = x[2]
            if ('BP -' in desc) or ('BP –' in desc) and ('TBP' not in desc):
                return True
            else:
                return False

        def not_filtro(x):
            desc = x[2]
            if ('BP -' in desc) or ('BP –' in desc) and ('TBP' not in desc):
                return False
            else:
                return True

        def not_BP_final_filtro(x):
            x = x[1][1]
            if x:
                return False
            else:
                return True

        def not_BP_final_filtro_inverse(x):
            x = x[1][1]
            if x:
                return True
            else:
                return False

        def filtro_BP_final(x):
            x = x[1][1]
            if x:
                return filtro_BP(x)
            else:
                False

        def filtro_sobra(x):

            if not (not_BP_final_filtro(x)) and not (filtro_BP_final(x)):
                return True
            else:
                return False

        rdd_hijo_BP = rdd.filter(filtro_BP)
        rdd_hijo_BP_final = rdd_hijo_BP.map(lambda x: (x[0], x[0], x[2]))

        rdd_hijo_not_BP = rdd.filter(not_filtro)
        rdd_hijo_not_BP_final = sc.parallelize([(None, None, None)])

        rdd = rdd.map(lambda x: (x[0], (x[0], x[1], x[2])))

        for i in xrange(15):

            rdd_superior = rdd_hijo_not_BP.map(lambda x: (x[1], (x[0], x[1], x[2])))

            rdd_hijo_BP_aux = rdd_superior.leftOuterJoin(rdd)

            rdd_hijo_BP_aux.count()

            rdd_hijo_not_BP_ant_final = rdd_hijo_BP_aux.filter(not_BP_final_filtro)
            rdd_hijo_not_BP_ant_final = rdd_hijo_not_BP_ant_final.map(lambda x: (x[1][0][0], "FORABP", "FORABP"))

            rdd_hijo_not_BP_final = rdd_hijo_not_BP_final.union(rdd_hijo_not_BP_ant_final)

            rdd_hijo_BP = rdd_hijo_BP_aux.filter(filtro_BP_final)
            rdd_hijo_BP = rdd_hijo_BP.map(lambda x: (x[1][0][0], x[1][1][0], x[1][1][2]))

            rdd_hijo_BP_final = rdd_hijo_BP_final.union(rdd_hijo_BP)

            rdd_sobra = rdd_hijo_BP_aux.filter(filtro_sobra)

            if len(rdd_sobra.take(1)) < 1:
                break

            rdd_hijo_not_BP = rdd_sobra.map(lambda x: (x[1][0][0], x[1][1][1], x[1][1][2]))

        if len(rdd_sobra.take(1)) >= 1:
            rdd_sobra = rdd_sobra.map(lambda x: (x[0], "FORABP", "FORABP"))

            rdd_hijo_not_BP_final = rdd_hijo_not_BP_final.join(rdd_sobra)

        rdd_hijo_not_BP_final = rdd_hijo_not_BP_final.filter(lambda x: x[0])
        rdd_hijo_BP_final = rdd_hijo_BP_final.union(rdd_hijo_not_BP_final)

        return rdd_hijo_BP_final

    rdd_hier_vert_prod = read_textFile(path_target_hier, 'MS0DMBODWHPRODGESTIONCTR_55_{0}.txt'.format(data))
    rdd_hier_vert_prod = rdd_hier_vert_prod.filter(filtro_hir_vert_prod)
    rdd_hier_vert_prod = rdd_hier_vert_prod.map(position_hier_vert_prod)
    rdd_hier_vert_prod = rdd_hier_vert_prod.distinct()
    rdd_hier_vert_prod = monta_hier_vert_prod(rdd_hier_vert_prod)
    df_hier_vert_prod = sqlContext.createDataFrame(rdd_hier_vert_prod,
                                                    ['codigo_prod', 'Codigo_Linha_BP', 'Linha_BP'])

    df_hier_vert_prod.count()

    """Hierarquia de Produtos na horizontal"""
    def filtro_prod(rdd):
        codigo = rdd[2:7].strip()

        if codigo == "JBLDN":
            return True
        else:
            return False

    def position_prod(rdd):
        codigo = ""
        descricao_nivel3 = []

        codigo = rdd[8:22].strip()

        descricao_nivel3 = rdd[419:518].strip()

        hierarquia = tuple((codigo, descricao_nivel3))

        return hierarquia
    
    rdd_hier_prod = read_textFile(path_target_hier, 'MS0DMDWHPRODUCTOSCTR_55_{0}.txt'.format(data))
    rdd_hier_prod = rdd_hier_prod.filter(filtro_prod)
    rdd_hier_prod = rdd_hier_prod.map(position_prod)
    df_hier_prod = sqlContext.createDataFrame(rdd_hier_prod, ['cod_prod', 'Balanco_LDN_Nivel_3'])

    df_hier_prod.count()

    ###########################################################################

    df_result = read(path_sqoop, data, table_name_result, 'parquet')
    # df_result = df_result.repartition(num_partitions)

    df_result = df_result.rdd.map(retiraEspacos)
    df_result = sqlContext.createDataFrame(df_result)

    df_result.registerTempTable("result")
    df_result = sqlContext.sql("select * from result "
                               "where cod_contenido = 'COM' or "
                               "cod_contenido = 'PLZ' or "
                               "cod_contenido = 'ARF' or "
                               "cod_contenido = 'CCL' or "
                               "cod_contenido = 'CCO' or "
                               "cod_contenido = 'CRE' or "
                               "cod_contenido = 'CTA' or "
                               "cod_contenido = 'CTB' or "
                               "cod_contenido = 'CTG' or "
                               "cod_contenido = 'FON' or "
                               "cod_contenido = 'PRE' ").persist()

    df_result.count()

    ###########################################################################

    """Cria os Criterios de Sinais"""
    rdd_criterios_sinais = read_textFile(path_target_para, 'Criterio_de_Sinais_NME.txt')
    rdd_criterios_sinais = rdd_criterios_sinais.map(lambda x: x.split(";"))
    header_criterios_sinais = rdd_criterios_sinais.first()
    rdd_criterios_sinais = rdd_criterios_sinais.filter(lambda line: line != header_criterios_sinais)
    df_criterio_sinais = sqlContext.createDataFrame(rdd_criterios_sinais, header_criterios_sinais)

    df_result = df_result.join(df_hier_prod,
                               df_result.cod_producto_gest == df_hier_prod.cod_prod,
                               'left_outer'). \
        drop(df_hier_prod.cod_prod)

    cond_crit_sinais = [df_result.tip_concepto_ctb == df_criterio_sinais.Conceito_Contabil,
                        df_result.Balanco_LDN_Nivel_3 == df_criterio_sinais.Balanco_LDN_Nivel_3]


    df_result = df_result.join(df_criterio_sinais, cond_crit_sinais, 'left_outer'). \
        drop(df_criterio_sinais["Balanco_LDN_Nivel_3"]). \
        drop(df_criterio_sinais["Conceito_Contabil"])

    ###########################################################################

    """Campos calculados da Result"""
    def cal_volume(df):
        return df.withColumn('Volume', (df["imp_sdo_cap_med_ml"] +
                                        df["imp_sdo_med_int_ml"] +
                                        df["imp_sdo_med_insolv_ml"]) * df["Criterio_de_Sinais"])

    def cal_ponta(df):
        return df.withColumn('Saldo_ponta', (df["imp_sdo_cap_ml"] +
                                             df["imp_sdo_int_ml"] +
                                             df["imp_sdo_insolv_ml"]) * df["Criterio_de_Sinais"])

    def cal_res_total(df):
        return df.withColumn('Resultado_Total_ml', (df["imp_ing_per_ml"] +
                                                    df["imp_ing_cap_ml"] +
                                                    df["imp_egr_per_ml"] +
                                                    df["imp_egr_cap_ml"] +
                                                    df["imp_ajtti_egr_tb_cap_ml"] +
                                                    df["imp_ajtti_egr_sl_cap_ml"] +
                                                    df["imp_ajtti_egr_per_ml"] +
                                                    df["imp_ajtti_ing_tb_cap_ml"] +
                                                    df["imp_ajtti_ing_sl_cap_ml"] +
                                                    df["imp_ajtti_ing_per_ml"] +
                                                    df["imp_efec_enc_ml"]) * df["Criterio_de_Sinais"])

    def cal_receita(df):
        return df.withColumn('Receita', (df["imp_ing_per_ml"] +
                                         df["imp_ing_cap_ml"] +
                                         df["imp_ajtti_ing_tb_cap_ml"] +
                                         df["imp_ajtti_ing_sl_cap_ml"] +
                                         df["imp_ajtti_ing_per_ml"] +
                                         df["imp_efec_enc_ml"]) * df["Criterio_de_Sinais"])

    def cal_custo(df):
        return df.withColumn('Custo', (df["imp_egr_per_ml"] +
                                       df["imp_egr_cap_ml"] +
                                       df["imp_ajtti_egr_tb_cap_ml"] +
                                       df["imp_ajtti_egr_sl_cap_ml"] +
                                       df["imp_ajtti_egr_per_ml"]) * df["Criterio_de_Sinais"])

    def cal_res_real(df):
        return df.withColumn('Resultado_Real', (df["imp_ing_per_ml"] +
                                                df["imp_ing_cap_ml"] +
                                                df["imp_egr_per_ml"] +
                                                df["imp_egr_cap_ml"]) * df["Criterio_de_Sinais"])

    def cal_res_ficticio(df):
        return df.withColumn('Res_ficticio', (df["imp_ajtti_ing_tb_cap_ml"] +
                                              df["imp_ajtti_ing_sl_cap_ml"] +
                                              df["imp_ajtti_ing_per_ml"] +
                                              df["imp_ajtti_egr_tb_cap_ml"] +
                                              df["imp_ajtti_egr_sl_cap_ml"] +
                                              df["imp_ajtti_egr_per_ml"] +
                                              df["imp_efec_enc_ml"]) * df["Criterio_de_Sinais"])

    def cal_res_ficticio_acum(df):
        return df.withColumn('Resultado_Ficticio_Acum', (df["imp_ajtti_ing_tb_cap_ml_acum"] +
                                                         df["imp_ajtti_ing_sl_cap_ml_acum"] +
                                                         df["imp_ajtti_ing_per_ml_acum"] +
                                                         df["imp_efec_enc_ml_acum"] +
                                                         df["imp_ajtti_egr_tb_cap_ml_acum"] +
                                                         df["imp_ajtti_egr_sl_cap_ml_acum"] +
                                                         df["imp_ajtti_egr_per_ml_acum"]) * df["Criterio_de_Sinais"])

    df_result = df_result.withColumn('Criterio_de_Sinais',
                                     when(df_result['Criterio_de_Sinais'].isNull(), 1).
                                     otherwise(df_result['Criterio_de_Sinais']))

    df_result = cal_volume(df_result)
    df_result = cal_ponta(df_result)
    df_result = cal_res_total(df_result)
    df_result = cal_receita(df_result)
    df_result = cal_custo(df_result)
    df_result = cal_res_real(df_result)
    df_result = cal_res_ficticio(df_result)
    df_result = cal_res_ficticio_acum(df_result)

    df_result = df_result.drop('Criterio_de_Sinais')

    ###########################################################################

    """Cria data frame da generic"""
    df_generic = read(path_sqoop, data, table_name_generic, 'parquet')
    # df_generic = df_generic.repartition(num_partitions)

    df_generic = df_generic.rdd.map(retiraEspacos)
    df_generic = sqlContext.createDataFrame(df_generic)

    df_generic.registerTempTable("generic")
    df_generic = sqlContext.sql("select * from generic "
                                "where cod_contenido = 'ARF' or "
                                "cod_contenido = 'CCO' or "
                                "cod_contenido = 'COM' or "
                                "cod_contenido = 'CRE' or "
                                "cod_contenido = 'CTA' or "
                                "cod_contenido = 'CTG' or "
                                "cod_contenido = 'FON' or "
                                "cod_contenido = 'PLZ' or "
                                "cod_contenido = 'PRE' ").persist()

    df_generic.count()

    ############################################################################

    """CONDICAO DO JOIN DA RESULT COM A GENERIC"""
    cond_result_generic = [df_result["idf_cto_ods"] == df_generic["idf_cto_ods"],
                           df_result["fec_data"] == df_generic["fec_data"],
                           df_result["cod_contenido"] == df_generic["cod_contenido"]]


    df_join_bl_ge = df_result.join(df_generic, cond_result_generic, 'inner'). \
        select(df_result["idf_cto_ods"], df_result["fec_data"],
                df_result["cod_entidad_espana"], df_result["cod_producto_gest"],
                df_result["cod_cta_cont_gestion"], df_result["cod_segmento_gest"],
                df_result["cod_area_negocio"], df_result["cod_tip_ajuste"],
                df_result["cod_centro_cont"], df_result["cod_ofi_comercial"],
                df_result["ind_conciliacion"], df_result["cod_origen_inf"],
                df_result["cod_contenido"], df_result["out_tti"],
                df_result["cod_est_sdo"], df_result["tasa_base"],
                df_result["origen_tasa"], df_result["cod_sis_origen"],
                df_result["ind_pool"], df_result["idf_pers_ods"],
                df_result["tipo_tasa_adis"], df_result["spread_liq"],
                df_result["Volume"], df_result["Saldo_ponta"],
                df_result["Resultado_Total_ml"], df_result["Receita"],
                df_result["Custo"], df_result["Resultado_Real"],
                df_result["Res_ficticio"], df_result["Resultado_Ficticio_Acum"],
                df_generic["cod_producto"], df_generic["tas_predef"],
                df_generic["cod_subprodu"], df_generic["fec_alta_cto"],
                df_generic["fec_ven"], df_generic["fec_can_ant"],
                df_generic["imp_ini_mo"], df_generic["tas_int"],
                df_generic["cod_complemento"], df_generic["imp_cuo_ini_mo"],
                df_generic["imp_cuo_mo"], df_generic["num_cuo_pac"],
                df_generic["plz_contractual"], df_generic["cod_cur_ref"],
                df_generic["cod_met_amrt"], df_generic["num_dia_demora"],
                df_generic["plz_med_rec"], df_generic["cod_bca_int"],
                df_generic["fec_prx_liq"], df_generic["fec_ult_liq"],
                df_generic["por_cupon"], df_generic["por_util_ind"],
                df_generic["cod_cartera_gestion"], df_generic["cod_tip_tas"]).persist().repartition(10)

    ###########################################################################

    def cal_cupom_tc(df):
        return df.withColumn('Cupom_tc', expr("case when tipo_tasa_adis = 6 or "
                                              "tipo_tasa_adis = 'A' then por_cupon "
                                              "when tipo_tasa_adis = 7 or "
                                              "tipo_tasa_adis = 'B' then por_util_ind "
                                              "else '' end"))

    def cal_cupom_tti(df):
        return df.withColumn('Cupom_tti', expr("case when tipo_tasa_adis = 6 or "
                                               "tipo_tasa_adis = 7 then spread_liq "
                                               "when tipo_tasa_adis = 'A' or "
                                               "tipo_tasa_adis = 'B' then tas_predef "
                                               "else '' end"))

    def cal_cod_sistema(df):
        return df.withColumn('Cd_Sist', expr("case when cod_sis_origen <> '' "
                                             "then cod_sis_origen "
                                             "when ind_conciliacion = 'M' "
                                             "then 'MN' else cod_contenido end"))

    def cal_spread(df):
        return df.withColumn('Spread', expr("case when cod_contenido = 'CRE' or "
                                            "cod_contenido = 'PRE' or "
                                            "cod_contenido = 'CCO' or "
                                            "cod_contenido = 'ARF' or "
                                            "cod_contenido = 'CAP' or "
                                            "cod_contenido = 'COM' and "
                                            "cod_tip_tas = 'V' then Cupom_tc - Cupom_tti "
                                            "when cod_contenido = 'CTA' or "
                                            "cod_contenido = 'PLZ' and "
                                            "cod_tip_tas = 'V' then Cupom_tc - Cupom_tti "
                                            "when cod_contenido = 'CTA' or "
                                            "cod_contenido = 'PLZ' and "
                                            "cod_tip_tas = 'F' then out_tti - tas_int "
                                            "when cod_contenido = 'CRE' or "
                                            "cod_contenido = 'PRE' or "
                                            "cod_contenido = 'CCO' or "
                                            "cod_contenido = 'ARF' or "
                                            "cod_contenido = 'CAP' or "
                                            "cod_contenido = 'COM' and "
                                            "cod_tip_tas = 'F' then tas_int - out_tti "
                                            "else '' end"))

    df_join_bl_ge = cal_cupom_tc(df_join_bl_ge)
    df_join_bl_ge = cal_cupom_tti(df_join_bl_ge)
    df_join_bl_ge = cal_spread(df_join_bl_ge)
    df_join_bl_ge = cal_cod_sistema(df_join_bl_ge)
    df_join_bl_ge = cal_spread(df_join_bl_ge)
    df_join_bl_ge = cal_spread(df_join_bl_ge)

    ###########################################################################

    """Cria data frame da client"""
    df_client = read(path_sqoop, data, table_name_client, 'parquet')
    # df_client = df_client.repartition(num_partitions)

    df_client = df_client.rdd.map(retiraEspacos)
    df_client = sqlContext.createDataFrame(df_client)

    ###########################################################################

    """CONDICAO DA BL_GE COM A CLIENT"""
    cond_bl_ge_cl = [df_join_bl_ge.idf_pers_ods == df_client.idf_pers_ods]

    df_bl_ge_cl = df_join_bl_ge.join(df_client, cond_bl_ge_cl, 'inner'). \
        select(df_join_bl_ge["fec_data"], df_join_bl_ge["cod_entidad_espana"],
            df_join_bl_ge["idf_cto_ods"], df_client["idf_cliente"],
            df_join_bl_ge["idf_pers_ods"], df_client["nom_nombre"],
            df_client["cod_tip_persona"], df_join_bl_ge["cod_producto"],
            df_join_bl_ge["cod_subprodu"], df_join_bl_ge["cod_producto_gest"],
            df_join_bl_ge["cod_cta_cont_gestion"], df_join_bl_ge["cod_segmento_gest"],
            df_join_bl_ge["cod_area_negocio"], df_join_bl_ge["cod_tip_ajuste"],
            df_join_bl_ge["cod_centro_cont"], df_join_bl_ge["cod_cartera_gestion"],
            df_join_bl_ge["cod_ofi_comercial"], df_join_bl_ge["ind_conciliacion"],
            df_join_bl_ge["cod_origen_inf"], df_join_bl_ge["Cd_Sist"],
            df_join_bl_ge["cod_contenido"], df_join_bl_ge["fec_alta_cto"],
            df_join_bl_ge["fec_ven"], df_join_bl_ge["fec_can_ant"],
            df_join_bl_ge["imp_ini_mo"], df_join_bl_ge["Volume"],
            df_join_bl_ge["Saldo_ponta"], df_join_bl_ge["Resultado_Total_ml"],
            df_join_bl_ge["Receita"], df_join_bl_ge["Custo"],
            df_join_bl_ge["tas_int"], df_join_bl_ge["out_tti"],
            df_join_bl_ge["Spread"], df_join_bl_ge["Resultado_Real"],
            df_join_bl_ge["Res_ficticio"], df_join_bl_ge["cod_complemento"],
            df_join_bl_ge["Cupom_tc"], df_join_bl_ge["Cupom_tti"],
            df_join_bl_ge["origen_tasa"], df_join_bl_ge["imp_cuo_ini_mo"],
            df_join_bl_ge["imp_cuo_mo"], df_join_bl_ge["num_cuo_pac"],
            df_join_bl_ge["plz_contractual"], df_join_bl_ge["cod_cur_ref"],
            df_join_bl_ge["cod_met_amrt"], df_join_bl_ge["num_dia_demora"],
            df_join_bl_ge["cod_tip_tas"], df_join_bl_ge["tipo_tasa_adis"],
            df_join_bl_ge["plz_med_rec"], df_join_bl_ge["cod_bca_int"],
            df_join_bl_ge["Resultado_Ficticio_Acum"], df_join_bl_ge["ind_pool"],
            df_join_bl_ge["cod_est_sdo"], df_join_bl_ge["tasa_base"],
            df_join_bl_ge["fec_prx_liq"], df_join_bl_ge["fec_ult_liq"]).persist().repartition(10)

    ###########################################################################

    """Cria Data Frame de Tratamento BP"""
    rdd_tratamento_bp = read_textFile(path_target_para, 'Layout_Excecao_Linha_BP.txt')
    rdd_tratamento_bp = rdd_tratamento_bp.map(lambda x: x.split(";"))
    header_tratamento_bp = rdd_tratamento_bp.first()
    rdd_tratamento_bp = rdd_tratamento_bp.filter(lambda line: line != header_tratamento_bp)
    df_tratamento_bp = sqlContext.createDataFrame(rdd_tratamento_bp, header_tratamento_bp)

    cond_hier_vert_prod = [df_bl_ge_cl.cod_producto_gest == df_hier_vert_prod.codigo_prod]

    df_bl_ge_cl = df_bl_ge_cl.join(df_hier_vert_prod, cond_hier_vert_prod, 'left_outer'). \
        drop(df_hier_vert_prod.codigo_prod)

    df_cod_linhaBP_not_null = df_bl_ge_cl.where('Codigo_Linha_BP is not null')
    df_cod_linhaBP_null = df_bl_ge_cl.where('Codigo_Linha_BP is null')

    cond1 = ((df_cod_linhaBP_not_null['Codigo_Linha_BP'] == df_tratamento_bp['Linha_BP_ORIGEM']) &
             (df_cod_linhaBP_not_null['cod_producto_gest'] == df_tratamento_bp['Produto_Gestao_ORIGEM']) &
             (df_cod_linhaBP_not_null['cod_cta_cont_gestion'] == df_tratamento_bp['CONTA_DE_GESTAO']))

    cond2 = ((df_cod_linhaBP_not_null['cod_producto_gest'] == df_tratamento_bp['Produto_Gestao_ORIGEM']) &
             (df_cod_linhaBP_not_null['cod_cta_cont_gestion'] == df_tratamento_bp['CONTA_DE_GESTAO']))

    cond3 = ((df_cod_linhaBP_not_null['Codigo_Linha_BP'] == df_tratamento_bp['Linha_BP_ORIGEM']) &
             (df_cod_linhaBP_not_null['cod_cta_cont_gestion'] == df_tratamento_bp['CONTA_DE_GESTAO']))

    cond4 = ((df_cod_linhaBP_not_null['Codigo_Linha_BP'] == df_tratamento_bp['Linha_BP_ORIGEM']) &
             (df_cod_linhaBP_not_null['cod_producto_gest'] == df_tratamento_bp['Produto_Gestao_ORIGEM']))

    cond5 = ((df_cod_linhaBP_not_null['cod_cta_cont_gestion'] == df_tratamento_bp['CONTA_DE_GESTAO']))

    cond6 = ((df_cod_linhaBP_not_null['cod_producto_gest'] == df_tratamento_bp['Produto_Gestao_ORIGEM']))

    cond7 = ((df_cod_linhaBP_not_null['Codigo_Linha_BP'] == df_tratamento_bp['Linha_BP_ORIGEM']))

    df_cod_linhaBP_not_null = df_cod_linhaBP_not_null.join(df_tratamento_bp,
                                                            cond1 |
                                                            cond2 |
                                                            cond3 |
                                                            cond4 |
                                                            cond5 |
                                                            cond6 |
                                                            cond7, "left_outer"). \
        drop('Linha_BP_ORIGEM'). \
        drop('Produto_Gestao_ORIGEM'). \
        drop('CONTA_DE_GESTAO')

    df_cod_linhaBP_not_null = df_cod_linhaBP_not_null.withColumnRenamed('Codigo_Linha_BP', 'Codigo_Linha_BP_Antigo'). \
        withColumnRenamed('Linha_BP', 'Linha_BP_Antigo')

    df_cod_linhaBP_not_null = df_cod_linhaBP_not_null.join(df_hier_vert_prod,
                                                           df_cod_linhaBP_not_null.Linha_BP_PARA == df_hier_vert_prod.Codigo_Linha_BP,
                                                           'left_outer'). \
        drop(df_hier_vert_prod.codigo_prod)

    df_cod_linhaBP_not_null = df_cod_linhaBP_not_null.withColumn('Codigo_Linha_BP',
                                                                 when(df_cod_linhaBP_not_null['Linha_BP_PARA'].isNull(),
                                                                      df_cod_linhaBP_not_null[
                                                                          'Codigo_Linha_BP_Antigo']).
                                                                 otherwise(df_cod_linhaBP_not_null['Linha_BP_PARA'])). \
        withColumn('Linha_BP',
                   when(df_cod_linhaBP_not_null['Linha_BP_PARA'].isNull(),
                        df_cod_linhaBP_not_null['Linha_BP_Antigo']).
                   otherwise(df_cod_linhaBP_not_null['Linha_BP'])). \
        drop(df_cod_linhaBP_not_null['Codigo_Linha_BP_Antigo']). \
        drop(df_cod_linhaBP_not_null['Linha_BP_Antigo']). \
        drop(df_cod_linhaBP_not_null['Linha_BP_PARA'])

    df_bl_ge_cl = df_cod_linhaBP_not_null.unionAll(df_cod_linhaBP_null)

    ###########################################################################

    """CONDICAO DAS HIERARQUIAS"""
    cond_hier_neg = [df_bl_ge_cl.cod_area_negocio == df_hier_negocio.codigo]
    cond_hier_cart_pv = [df_bl_ge_cl.cod_cartera_gestion == df_hier_carteira_pv.codigo_cart_pv]
    cond_hier_cart = [df_bl_ge_cl.cod_cartera_gestion == df_hier_carteira.codigo_cart]

    df_bl_ge_cl = df_bl_ge_cl.join(df_hier_negocio, cond_hier_neg, 'left_outer'). \
        join(df_hier_carteira_pv, cond_hier_cart_pv, 'left_outer'). \
        join(df_hier_carteira, cond_hier_cart, 'left_outer'). \
        drop(df_hier_negocio.codigo). \
        drop(df_hier_carteira_pv.codigo_cart_pv). \
        drop(df_hier_carteira.codigo_cart)
    
    ###########################################################################

    """Cria Data Frame do FX"""
    my_fields = ['TIP_REG', 'COD-ORG', 'CTR-ALT', 'ENTIDAD', 'TP-CRAL', 'TP-ORG']

    length_fields = [1, 4, 4, 4, 3, 3]
    
    rdd_fx = read_textFile(path_target_fx, 'TCC_JC0_0007_001_D_FULL_{0}.out'.format(data))
    rdd_fx = rdd_fx.filter(lambda line: line.startswith("1"))
    deserializar = deserialize.DeserializeLinesToRows(my_fields, length_fields, sqlContext)
    df_fx = deserializar(rdd_fx)

    df_bl_ge_cl = df_bl_ge_cl.withColumn('Cod_Uniorg', expr("case when length(Cod_Carteira_PV) = 9 "
                                                            "then substring(Cod_Carteira_PV, 4, length(Cod_Carteira_PV)) "
                                                            "when length(Cod_Carteira_PV) = 8 "
                                                            "then substring(Cod_Carteira_PV, 3, length(Cod_Carteira_PV)) "
                                                            "when length(Cod_Carteira_PV) = 5 "
                                                            "then concat('0', Cod_Carteira_PV) "
                                                            "else Cod_Carteira_PV end"))

    df_bl_ge_cl = df_bl_ge_cl.withColumn('TP_CRAL', expr("substring(Cod_Uniorg, 1, 2)")). \
        withColumn('CD_CRAL', expr("substring(Cod_Uniorg, 3, length(Cod_Uniorg))")). \
        drop('Cod_Uniorg')
    
    df_fx = df_fx.select('ENTIDAD', 'TP-CRAL', 'CTR-ALT', 'TP-ORG', 'COD-ORG')

    cond_fx = [df_bl_ge_cl['TP_CRAL'] == df_fx['TP-CRAL'],
               df_bl_ge_cl['CD_CRAL'] == df_fx['CTR-ALT'],
               ((df_fx['ENTIDAD'] == '033') | (df_fx['ENTIDAD'] == '0033'))]
    

    df_bl_ge_cl = df_bl_ge_cl.join(df_fx, cond_fx, 'left_outer'). \
        drop(df_bl_ge_cl['TP_CRAL']). \
        drop(df_bl_ge_cl['CD_CRAL']). \
        drop(df_fx['TP-CRAL']). \
        drop(df_fx['CTR-ALT']). \
        drop(df_fx['ENTIDAD'])

    df_bl_ge_cl = df_bl_ge_cl.withColumn('Superior_Carteira_Uniorg', concat('TP-ORG', 'COD-ORG'))

    ###########################################################################

    df_bl_ge_cl = df_bl_ge_cl.withColumn('Data_Extracao', lit(data))

    df_bl_ge_cl = df_bl_ge_cl.select(df_bl_ge_cl["Data_Extracao"],
                                    df_bl_ge_cl["fec_data"],
                                    df_bl_ge_cl["cod_entidad_espana"],
                                    df_bl_ge_cl["idf_cto_ods"],
                                    df_bl_ge_cl["idf_cliente"],
                                    df_bl_ge_cl["idf_pers_ods"],
                                    df_bl_ge_cl["nom_nombre"],
                                    df_bl_ge_cl["cod_tip_persona"],
                                    df_bl_ge_cl["cod_producto"],
                                    df_bl_ge_cl["cod_subprodu"],
                                    df_bl_ge_cl["cod_producto_gest"],
                                    df_bl_ge_cl["cod_cta_cont_gestion"],
                                    df_bl_ge_cl["cod_segmento_gest"],
                                    df_bl_ge_cl["cod_area_negocio"],
                                    df_bl_ge_cl["Area_de_Negocio_Reporting"],
                                    df_bl_ge_cl["Codigo_Linha_BP"],
                                    df_bl_ge_cl["Linha_BP"],
                                    df_bl_ge_cl["cod_tip_ajuste"],
                                    df_bl_ge_cl["cod_centro_cont"],
                                    df_bl_ge_cl["cod_cartera_gestion"],
                                    df_bl_ge_cl["Ger_Matr"],
                                    df_bl_ge_cl["cod_ofi_comercial"],
                                    df_bl_ge_cl["Cod_Carteira_PV"],
                                    df_bl_ge_cl["ind_conciliacion"],
                                    df_bl_ge_cl["cod_origen_inf"],
                                    df_bl_ge_cl["Cd_Sist"],
                                    df_bl_ge_cl["cod_contenido"],
                                    df_bl_ge_cl["fec_alta_cto"],
                                    df_bl_ge_cl["fec_ven"],
                                    df_bl_ge_cl["fec_can_ant"],
                                    df_bl_ge_cl["imp_ini_mo"],
                                    df_bl_ge_cl["Volume"],
                                    df_bl_ge_cl["Saldo_ponta"],
                                    df_bl_ge_cl["Resultado_Total_ml"],
                                    df_bl_ge_cl["Receita"],
                                    df_bl_ge_cl["Custo"],
                                    df_bl_ge_cl["tas_int"],
                                    df_bl_ge_cl["out_tti"],
                                    df_bl_ge_cl["Spread"],
                                    df_bl_ge_cl["Resultado_Real"],
                                    df_bl_ge_cl["Res_ficticio"],
                                    df_bl_ge_cl["cod_complemento"],
                                    df_bl_ge_cl["Cupom_tc"],
                                    df_bl_ge_cl["Cupom_tti"],
                                    df_bl_ge_cl["origen_tasa"],
                                    df_bl_ge_cl["imp_cuo_ini_mo"],
                                    df_bl_ge_cl["imp_cuo_mo"],
                                    df_bl_ge_cl["num_cuo_pac"],
                                    df_bl_ge_cl["plz_contractual"],
                                    df_bl_ge_cl["cod_cur_ref"],
                                    df_bl_ge_cl["cod_met_amrt"],
                                    df_bl_ge_cl["num_dia_demora"],
                                    df_bl_ge_cl["cod_tip_tas"],
                                    df_bl_ge_cl["tipo_tasa_adis"],
                                    df_bl_ge_cl["plz_med_rec"],
                                    df_bl_ge_cl["cod_bca_int"],
                                    df_bl_ge_cl['Superior_Carteira_Uniorg'],
                                    df_bl_ge_cl["Resultado_Ficticio_Acum"],
                                    df_bl_ge_cl["ind_pool"],
                                    df_bl_ge_cl["cod_est_sdo"],
                                    df_bl_ge_cl["tasa_base"],
                                    df_bl_ge_cl["fec_prx_liq"],
                                    df_bl_ge_cl["fec_ult_liq"])

    oldColumns = df_bl_ge_cl.schema.names
    newColumns = ["Data_Extracao", "Ref", "Banco", "Contrato", "Cpf_cnpj", "Penumper",
                  "Nome_Cliente", "Tp_Pessoa", "Produto", "Subpro",
                  "Produto_Gestao", "Conta_Gestao", "Segto", "Dimensao_AdN",
                  "Area_de_Negocio_Reporting", "Codigo_Linha_BP", "Linha_BP",
                  "Criterio_de_Ajuste", "Centro_Contable", "Cli_Carte",
                  "Ger_Matr", "Agencia", "Cod_Carteira_PV", "Indicador_Conciliacao",
                  "Origem_Informacao", "Cd_Sist", "Conteudo", "Data_Formalizacao",
                  "Data_Vencimento", "Data_Liq_Antecip", "Valor_do_Contrato",
                  "Volume", "Saldo_ponta", "Resultado_Total_ml", "Receita",
                  "Custo", "Taxa_Contrato", "Taxa_Captacao", "Spread",
                  "Resultado_Real", "Res_ficticio",
                  "Cod_Complemento", "Cupom_tc", "Cupom_tti", "Ind_Remarcagem",
                  "Valor_Parcela_Inicial", "Valor_Parcela", "Qtde_Total_Parcelas",
                  "Prazo_Contratual", "Curva_Referencia", "Metodo_Amortizacao",
                  "Qtde_Dias_Atraso", "Ind_Fixo_Variavel", "Tipo_Taxa_Adis",
                  "Prazo_Medio_Receb", "Base_Periodificacao_TC", "Superior_Carteira_Uniorg",
                  "Resultado_Ficticio_Acum", "Indicador_Pool", "Estado_Saldo",
                  "Taxa_Base", "Data_Prx_Liq", "Data_Ult_Liq"]

    def rename_column(old_columns, new_columns, dataframe):
        return reduce(lambda dataframe, idx:
                      dataframe.withColumnRenamed(old_columns[idx], new_columns[idx]),
                      xrange(len(old_columns)), dataframe)

    df_bl_ge_cl = rename_column(oldColumns, newColumns, df_bl_ge_cl)

    ###########################################################################

    df_bl_ge_cl = df_bl_ge_cl.withColumn('Penumper', expr("case when substring(Penumper, 1, 5) = 10033 "
                                                          "then substring(Penumper, 6, length(Penumper)) "
                                                          "else Penumper end"))

    df_bl_ge_cl = df_bl_ge_cl.withColumn('Agencia', expr("case when substring(Agencia, 1, 2) = 01 "
                                                         "then substring(Agencia, 3, length(Agencia)) "
                                                         "else Agencia end"))

    df_bl_ge_cl = df_bl_ge_cl.withColumn('Fonte', lit(""))

    ###########################################################################

    print("Gravando no HDFS Tabela Final")
    df_bl_ge_cl.registerTempTable("df_final_agregado")
    df_bl_ge_cl_agregado = sqlContext.sql("select Data_Extracao, Ref, Banco, Contrato, Cpf_cnpj, Penumper, "
                                        "Nome_Cliente, Tp_Pessoa, Produto, Subpro, Produto_Gestao, Conta_Gestao, "
                                        "Segto, Dimensao_AdN, Area_de_Negocio_Reporting, Codigo_Linha_BP, Linha_BP, "
                                        "Criterio_de_Ajuste, Centro_Contable, Cli_Carte, "
                                        "Ger_Matr, Agencia, Cod_Carteira_PV, Indicador_Conciliacao, "
                                        "Origem_Informacao, Cd_Sist, Conteudo, Data_Formalizacao, "
                                        "Data_Vencimento, Data_Liq_Antecip, Valor_do_Contrato, "
                                        "sum(Volume) as Volume, sum(Saldo_ponta) as Saldo_ponta, "
                                        "sum(Resultado_Total_ml) as Resultado_Total_ml, sum(Receita) as Receita, "
                                        "sum(Custo) as Custo, Taxa_Contrato, Taxa_Captacao, Spread, "
                                        "sum(Resultado_Real) as Resultado_Real, sum(Res_ficticio) as Res_ficticio, "
                                        "Cod_Complemento, Cupom_tc, Cupom_tti, Ind_Remarcagem, "
                                        "Valor_Parcela_Inicial, Valor_Parcela, Qtde_Total_Parcelas, "
                                        "Prazo_Contratual, Curva_Referencia, Metodo_Amortizacao, "
                                        "Qtde_Dias_Atraso, Ind_Fixo_Variavel, Tipo_Taxa_Adis, "
                                        "Prazo_Medio_Receb, Base_Periodificacao_TC, Superior_Carteira_Uniorg, "
                                        "Resultado_Ficticio_Acum, Indicador_Pool, Estado_Saldo, "
                                        "Taxa_Base, Data_Prx_Liq, Data_Ult_Liq, Fonte from df_final_agregado "
                                        "group by Data_Extracao, Ref, "
                                        "Banco, Contrato, Cpf_cnpj, Penumper, "
                                        "Nome_Cliente, Tp_Pessoa, Produto, Subpro, Produto_Gestao, Conta_Gestao, "
                                        "Segto, Dimensao_AdN, Area_de_Negocio_Reporting, Codigo_Linha_BP, Linha_BP, "
                                        "Criterio_de_Ajuste, Centro_Contable, Cli_Carte, "
                                        "Ger_Matr, Agencia, Cod_Carteira_PV, Indicador_Conciliacao, "
                                        "Origem_Informacao, Cd_Sist, Conteudo, Data_Formalizacao, "
                                        "Data_Vencimento, Data_Liq_Antecip, Valor_do_Contrato, "
                                        "Taxa_Contrato, Taxa_Captacao, Spread, "
                                        "Cod_Complemento, Cupom_tc, Cupom_tti, Ind_Remarcagem, "
                                        "Valor_Parcela_Inicial, Valor_Parcela, Qtde_Total_Parcelas, "
                                        "Prazo_Contratual, Curva_Referencia, Metodo_Amortizacao, "
                                        "Qtde_Dias_Atraso, Ind_Fixo_Variavel, Tipo_Taxa_Adis, "
                                        "Prazo_Medio_Receb, Base_Periodificacao_TC, Superior_Carteira_Uniorg, "
                                        "Resultado_Ficticio_Acum, Indicador_Pool, Estado_Saldo, "
                                        "Taxa_Base, Data_Prx_Liq, Data_Ult_Liq, Fonte")

    infile = "/sistemas/mif/output/nme_{0}_{1}.temp".format(process, data)
    outfile = "/sistemas/mif/output/nme_{0}_{1}.txt".format(process, data)

    df_bl_ge_cl_agregado.coalesce(240).write.csv(infile, mode="append", compression=None, sep=";", quote=None, escape=None, header="true", nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None, timestampFormat=None)
    
    copyMerge(infile,
              outfile,
              True, True, False)

    # df_bl_ge_cl_agregado.createOrReplaceTempView("df_bl_ge_cl_agregado")
    # sqlContext.sql("drop table if exists nme_{0}_{1}".format(process, data))
    # sqlContext.sql("CREATE EXTERNAL TABLE nme_{0}_{1} BY LOCATION /sistemas/mif/output/nme_{0}_{1}.txt".format(process, data))

    ###########################################################################

    print("Gravando no HDFS Tabela Consolidado")
    df_bl_ge_cl.registerTempTable("df_final")
    df_agregado = sqlContext.sql("select Data_Extracao, Ref, Tp_Pessoa, "
                                 "Produto_Gestao, Conta_Gestao, Segto, "
                                 "Dimensao_AdN, Area_de_Negocio_Reporting, "
                                 "Codigo_Linha_BP, Linha_BP, Superior_Carteira_Uniorg, "
                                 "Criterio_de_Ajuste, Centro_Contable, "
                                 "Cli_Carte, Cod_Carteira_PV, "
                                 "Indicador_Conciliacao, Origem_Informacao, "
                                 "Indicador_Pool, sum(Volume) as Volume, "
                                 "sum(Saldo_ponta) as Saldo_ponta, "
                                 "sum(Resultado_Total_ml) as Resultado_Total_ml, "
                                 "sum(Receita) as Receita, "
                                 "sum(Custo) as Custo from df_final " 
                                 "group by Data_Extracao, Ref, Tp_Pessoa, "
                                 "Produto_Gestao, Conta_Gestao, Segto, "
                                 "Dimensao_AdN, Area_de_Negocio_Reporting, "
                                 "Codigo_Linha_BP, Linha_BP, Superior_Carteira_Uniorg, "
                                 "Criterio_de_Ajuste, Centro_Contable, "
                                 "Cli_Carte, Cod_Carteira_PV, "
                                 "Indicador_Conciliacao, Origem_Informacao, Indicador_Pool")

    infile = "/sistemas/mif/output/nme_{0}_consolidado_{1}.temp".format(process, data)
    outfile = "/sistemas/mif/output/nme_{0}_consolidado_{1}.txt".format(process, data)

    df_agregado.coalesce(40).write.csv(infile, mode="append", compression=None, sep=";", quote=None, escape=None, header="true", nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None, timestampFormat=None)

    copyMerge(infile,
              outfile,
              True, True, False)

    # hiveContext.registerDataFrameAsTable(df_agregado, "df_agregado")
    # sqlContext.sql("drop table if exists nme_{0}_consolidado_{1}".format(process, data))
    # sqlContext.sql("CREATE EXTERNAL TABLE nme_{0}_consolidado_{1} BY LOCATION /sistemas/mif/output/nme_{0}_consolidado_{1}.txt".format(process, data))


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    sparkConf = SparkConf()
    sparkConf.setAppName('NME')
    sparkConf.set('spark.sql.parquet.compression.codec', 'snappy')
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)

    num_partitions = int(sqlContext._sc._conf.get("spark.default.parallelism"))

    if len(sys.argv) != 3:
        print("Numero de Argumentos invalidos!")
        exit(-1)

    run(sys.argv[1], sys.argv[2])
    print(sys.argv[2] + " com sucesso")