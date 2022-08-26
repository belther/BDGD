
  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_SAP" ("EQUIPAMENTO", "OBJ_TECNICO", "IDUC", "GRUPO_DE_PLANEJAMENTO", "BDGD", "DATA_CONEXAO", "FABRICANTE", "N_SERIE_FAB", "ANO_CONSTRUCAO", "EQUI_SUP", "SIT_OPER", "DENOMINACAO", "LOCAL_INSTALACAO", "ODI", "SUBESTACAO", "STATUS_SE", "STATUS_UE", "DATA_IMOB", "TUC", "UAR", "MODELO", "TI", "CENTRO_MODULAR", "A1_TEC", "A1", "A2_TEC", "A2", "A3_TEC", "A3", "A4_TEC", "A4", "A5_TEC", "A5", "A6_TEC", "A6", "UC_UAR", "CLAS_TEN", "TEN_PRI", "TEN_SEC", "TEN_TER", "TEN_REG", "COR_NOM", "POT_NOM", "POT_F01", "POT_F02", "FAS_CON", "LIG", "LIG_FAS_P", "LIG_FAS_S", "LIG_FAS_T", "SITCONT", "TIP_UNID", "TIP_REGU", "PER_FER", "PER_TOT", "R", "XHL", "TEL_TP", "REL_TC", "ELO_FSV", "MEI_ISO", "ABER_CRG", "COMP", "FLX_INV", "TABLE_ORIG", "REGISTRO_ATIVO")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT * FROM BDGD_STAGING.SAP
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_SSDBT" ("COD_ID", "PN_CON_1", "PN_CON_2", "UNI_TR_D", "CTMT", "UNI_TR_S", "SUB", "CONJ", "ARE_LOC", "FAS_CON", "DIST", "PAC_1", "PAC_2", "TIP_CND", "POS", "ODI_FAS", "TI_FAS", "SITCONTFAS", "ODI_NEU", "TI_NEU", "SITCONTNEU", "COMP", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT SEGMENTO.COD_ID,
         SEGMENTO.PN_CON_1,
         SEGMENTO.PN_CON_2,
         SEGMENTO.UNI_TR_D,
         SEGMENTO.CTMT,
         SEGMENTO.UNI_TR_S,
         SEGMENTO.SUB,
         SEGMENTO.CONJ,
         SEGMENTO.ARE_LOC,
         SEGMENTO.FAS_CON,
         SEGMENTO.DIST,
         DECODE(SEGMENTO.PAC_1,
         'BT-0', 'D1-'||SEGMENTO.COD_ID,'0', 'D2-'||SEGMENTO.COD_ID, SEGMENTO.PAC_1) PAC_1, SEGMENTO.PAC_2,
         SEGMENTO.TIP_CND,
         SEGMENTO.POS,
         SEGMENTO.ODI_FAS,
         SEGMENTO.TI_FAS,
         SEGMENTO.SITCONTFAS,
         SEGMENTO.ODI_NEU,
         SEGMENTO.TI_NEU,
         SEGMENTO.SITCONTNEU,
         SEGMENTO.COMP,
         SEGMENTO.DESCR,
         SEGMENTO.GEOMETRY
FROM
    (SELECT GEN.COD_ID,
         GEN.PN_CON_1,
         GEN.PN_CON_2,
         GEN.UNI_TR_D,
         COALESCE(ALI.NOVO_ALIMENTADOR,GEN.CTMT, '0') CTMT,
         COALESCE(ALI.UN_TR_S,DECODE(TRA.LOC_INST_SAP_TT, NULL,'0', '0', '0', TRA.LOC_INST_SAP_TT)) UNI_TR_S,
         GEN.SUB,
         GEN.CONJ,
         GEN.ARE_LOC,
         --GEN.FAS_CON,
         DECODE(COALESCE(SUGERE.FASE_SUGERIDA, GEN.FAS_CON, '-1'),'ACN','CAN',COALESCE(SUGERE.FASE_SUGERIDA, GEN.FAS_CON, '-1')) as FAS_CON,
         --COALESCE(SUGERE.FASE_SUGERIDA, GEN.FAS_CON, '-1') FAS_CON,
         GEN.DIST, DECODE(QDP.PAC_1, NULL, DECODE(CON.NR_PTO_ELET_FIM, NULL,'BT-' || COALESCE(FLY.PAC_1, GEN.PAC_1), ''||CON.NR_PTO_ELET_FIM ), QDP.PAC_1) PAC_1,
          'BT-' || GEN.PAC_2 PAC_2,
          GEN.TIP_CND,
          GEN.POS,
          GEN.ODI_FAS,
          GEN.TI_FAS,
          GEN.SITCONTFAS,
          GEN.ODI_NEU,
          GEN.TI_NEU,
          GEN.SITCONTNEU,
          GEN.COMP,
          GEN.DESCR,
          GEN.GEOMETRY
    FROM BDGD_STAGING.MV_GENESIS_SSDBT GEN
    LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TRA
        ON TRA.CD_ALIMENTADOR = GEN.CTMT
    LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONEC_MT_BT CON
        ON CON.NR_PTO_ELET_FIM = GEN.PAC_1
    LEFT JOIN BDGD_STAGING.MV_GENESIS_FLY_TAP_BT FLY
        ON GEN.PAC_1 = FLY.PAC_2
    LEFT JOIN BDGD_STAGING.MV_GENESIS_QDP QDP
        ON QDP.PAC_2 = GEN.PAC_1
    LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ALI
        ON ALI.ALIMENTADOR = GEN.CTMT 
    LEFT JOIN BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT  SUGERE
        ON SUGERE.FEATURE_ID = GEN.COD_ID) SEGMENTO
        
        --select * from BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNSEMT_BKP" ("COD_ID", "DIST", "PAC_1", "PAC_2", "FAS_CON", "SIT_ATIV", "TIP_UNID", "P_N_OPE", "CAP_ELO", "COR_NOM", "TLCD", "DAT_CON", "POS", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "ARE_LOC", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT   TO_CHAR(GEN.COD_ID) COD_ID,
            GEN.DIST,
            COALESCE(TO_CHAR(GEN.PAC_1),
            'DESC_' || TO_CHAR(GEN.COD_ID)) PAC_1, 
            COALESCE(TO_CHAR(GEN.PAC_2),
            'DESC_' || TO_CHAR(GEN.COD_ID)) PAC_2, 
            GEN.FAS_CON, 'AT' SIT_ATIV, 
            GEN.TIP_UNID, GEN.P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            NVL(TCOR.COD_ID, '0') COR_NOM,
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(SAP.DATA_CONEXAO,'01/01/1900', '24/10/2010', SAP.DATA_CONEXAO) DAT_CON, 
            GEN.POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,GEN.CTMT,'0') AS "CTMT", 
            DECODE(NV.UN_TR_S, NULL, DECODE(TAB.ID_TT, NULL,'0','0','0', 'SE-' ||TAB.ID_TT), NV.UN_TR_S) AS UNI_TR_S,
            COALESCE(GEN.SUB,'0') as SUB, 
            GEN.CONJ, 
            GEN.MUN, 
            GEN.ARE_LOC, 
            SAP.LOCAL_INSTALACAO AS "DESCR", 
            GEN.GEOMETRY
    FROM BDGD_STAGING.MV_GENESIS_UNSEMT_1 GEN
    LEFT JOIN MV_BDGD_SAP SAP
        ON SAP.EQUIPAMENTO = GEN.NR_EQUIPAMENTO
    LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
        ON TAB.CD_ALIMENTADOR = GEN.CTMT
    LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
        ON NV.ALIMENTADOR = TAB.CD_ALIMENTADOR
    LEFT JOIN TCAPELFU
        ON TCAPELFU.CAP = REPLACE(SAP.ELO_FSV, ',0', '')
    LEFT JOIN TCOR
        ON TCOR.CORR = SAP.COR_NOM
    WHERE SAP.A2 < 60
            OR SAP.A2 IS NULL
    UNION
    ALL
    SELECT TO_CHAR(GEN.COD_ID),
            GEN.DIST,
            COALESCE(TO_CHAR(GEN.PAC_1),
            'DESC_'||TO_CHAR(GEN.COD_ID)) PAC_1, 
            COALESCE(TO_CHAR(GEN.PAC_2),'DESC_'||TO_CHAR(GEN.COD_ID)) PAC_2, 
            GEN.FAS_CON, 'AT' SIT_ATIV, 
            GEN.TIP_UNID, GEN.P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            NVL(TCOR.COD_ID, '0') COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(SAP.DATA_CONEXAO,'01/01/1900', '24/10/2010', SAP.DATA_CONEXAO) DAT_CON, 
            GEN.POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,GEN.CTMT,'0') AS "CTMT", 
            DECODE(NV.UN_TR_S, NULL, DECODE(TAB.ID_TT, NULL,'0','0','0', 'SE-' ||TAB.ID_TT), NV.UN_TR_S) AS UNI_TR_S, 
            COALESCE(GEN.SUB,'0') as SUB, 
            GEN.CONJ, 
            GEN.MUN, GEN.ARE_LOC, 
            SAP.LOCAL_INSTALACAO AS "DESCR", 
            GEN.GEOMETRY
    FROM BDGD_STAGING.MV_GENESIS_UNSEMT_2 GEN
    LEFT JOIN 
        (SELECT *
        FROM MV_BDGD_SAP S
        WHERE S.BDGD = 'EQSE'
                AND S.OBJ_TECNICO IN ('BF','CD', 'CDA', 'CDP', 'CDT', 'CO', 'FA', 'FF', 'FP', 'FR', 'FT', 'FU')) SAP
        ON SAP.LOCAL_INSTALACAO = GEN.DESCR
    LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
        ON TAB.CD_ALIMENTADOR = GEN.CTMT
    LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
        ON NV.ALIMENTADOR = TAB.CD_ALIMENTADOR
    LEFT JOIN TCAPELFU
        ON TCAPELFU.CAP = REPLACE(SAP.ELO_FSV, ',0', '')
    LEFT JOIN TCOR
        ON TCOR.CORR = SAP.COR_NOM
    WHERE SAP.A2 < 60
            OR SAP.A2 IS NULL
    UNION
    ALL
    SELECT A.*,
            G.geometry GEOMETRY
        FROM (SELECT'SE-' || CON.PAC2_FID COD_ID, '5697' DIST,
            CASE
            WHEN MAX(CON.PAC1_FID) IS NOT NULL THEN
            'SE-' || MAX(CON.PAC1_FID)
            ELSE 'D1-'||CON.PAC2_FID
            END AS PAC_1, 
            'SE-' || CON.PAC2_FID PAC_2, 
            'ABC' FAS_CON, 
            'AT' SIT_ATIV, 
            DECODE(S.TUC, 160, DECODE(S.A1, 3, 22, 34), 210, 29, 345, 32) TIP_UNID, 
            'F' P_N_OPE, NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            TCOR.COD_ID COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(S.DATA_CONEXAO,'01/01/1900', '24/10/2010', S.DATA_CONEXAO) DAT_CON, 
            'PD' POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,TAB.CD_ALIMENTADOR,'0') AS "CTMT", 
            DECODE(NODE2.OBJECTID,NULL, '0', 'SE-' || NODE2.OBJECTID) AS UNI_TR_S, 
            COALESCE(SUB.SG_SUBESTACAO,'0') SUB, 
            CONJ.NR_CONJUNTO_ANEEL CONJ, 
            MUN.NR_MUN_ANEEL MUN, 
            DECODE(SUB.ID_AREA_OBJETO, 'U', 'UB', 'R', 'NU', NULL) ARE_LOC, 
            S.LOCAL_INSTALACAO DESCR--, --NODE.SHAPE GEOMETRY
        FROM 
            (SELECT A2.EQUIPAMENTO,
            A2.STATUS_SE,
            A2.COR_NOM,
            A2.A1,
            A2.TUC,
            A2.A3,
            A2.A2,
            A2.BDGD,
            A2.ELO_FSV,
            A2.DATA_CONEXAO,
            A2.LOCAL_INSTALACAO
            FROM 
                (SELECT S1.*,
            ROW_NUMBER()
                    OVER (PARTITION BY LOCAL_INSTALACAO
                ORDER BY  DATA_CONEXAO DESC) ROW_NUMBER
                FROM MV_BDGD_SAP S1
                WHERE LOCAL_INSTALACAO LIKE 'SE%'
                        AND BDGD ='EQSE' ) A2
                WHERE ROW_NUMBER = 1
                GROUP BY  A2.LOCAL_INSTALACAO,A2.COR_NOM, A2.STATUS_SE, A2.A1, A2.TUC, A2.A3, A2.A2, A2.BDGD, A2.ELO_FSV, A2.DATA_CONEXAO, A2.EQUIPAMENTO ) S
                INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN CON
                    ON CON.PAC2 = S.LOCAL_INSTALACAO
                LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
                    ON NODE.LOC_INST_SAP = CON.PAC2
                LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
                    ON TAB.LOC_INST_SAP_ALI = NODE.ALIMENTADOR
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_ALIMENTADOR ALI
                    ON ALI.CD_ALIMENTADOR = TAB.CD_ALIMENTADOR
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
                    ON SUB.NR_SUBESTACAO = TO_NUMBER(SUBSTR(NODE.LOC_INST_SAP, 4, 3))
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONJUNTO_ANEEL CONJ
                    ON CONJ.PRI_CONJUNTO_CNS_ID = SUB.PRI_CONJUNTO_CNS_ID
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_MUNICIPIO_ANEEL MUN
                    ON MUN.PRI_MUNICIPIO_ID = SUB.PRI_MUNICIPIO_ID
                LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
                    ON NV.ALIMENTADOR = TAB.CD_ALIMENTADOR
                LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2
                    ON NODE2.LOC_INST_SAP = NODE.UN_TR_S
                LEFT JOIN TCAPELFU
                    ON TCAPELFU.CAP = REPLACE(S.ELO_FSV, ',0', '')
                LEFT JOIN TCOR
                    ON TCOR.CORR = S.COR_NOM
                WHERE S.BDGD = 'EQSE'
                        AND S.A2 < 60
                GROUP BY  
                CON.PAC2_FID, 
                S.STATUS_SE, 
                S.A1, 
                S.TUC, 
                TCOR.COD_ID, 
                S.A3, 
                TCAPELFU.COD_ID, 
                S.DATA_CONEXAO, 
                TAB.CD_ALIMENTADOR, 
                TAB.ID_TT, 
                SUB.SG_SUBESTACAO, 
                CONJ.NR_CONJUNTO_ANEEL, 
                MUN.NR_MUN_ANEEL, 
                SUB.ID_AREA_OBJETO, 
                S.LOCAL_INSTALACAO, 
                NV.NOVO_ALIMENTADOR,
                NODE.UN_TR_S,
                NODE2.OBJECTID, 
                NV.UN_TR_S ) A
            LEFT JOIN GEN_BDGD2019.V_EQPTOS_SUBESTACAO G
            ON G.local_inst = A.DESCR
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQSE" ("COD_ID", "DIST", "PAC_1", "PAC_2", "CLAS_TEN", "ELO_FSV", "MEI_ISO", "FAS_CON", "COR_NOM", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "ABER_CRG", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT CAST(S.EQUIPAMENTO AS VARCHAR2(20)) AS COD_ID,
         '5697' AS DIST,
          COALESCE(to_char(gen.PAC_1), 'D1-'||gen.cod_id) PAC_1,
           COALESCE(to_char(gen.PAC_2), 'D2-'||gen.cod_id) PAC_2, 
           nvl(TCLATEN.COD_ID,'0') AS CLAS_TEN, 
           nvl(TCAPELFU.COD_ID,'0') AS ELO_FSV, 
         nvl(TMEIISO.COD_ID,'0') AS MEI_ISO, 
         nvl(GEN.fas_con, '0') AS FAS_CON,
         nvl(tcor.cod_id,'0') AS COR_NOM,
         s.odi AS ODI,
         TARE.BDGD AS TI,
         nvl(s.centro_modular, '999') AS CM,
         s.tuc AS TUC,
         nvl(s.a1, '00')  A1,
         nvl(s.a2, '00')  A2,
         nvl(s.a3, '00')  A3,
         nvl(s.a4, '00')  A4,
         nvl(s.a5, '00')  A5,
         nvl(s.a6, '00')  A6,
         s.iduc AS IDUC,
         CASE
            WHEN S.TUC='000' THEN
            'COM'
            ELSE 'AT1'
            END AS SITCONT, 
         nvl(SUBSTR(s.data_imob, 0,10), '20/10/2010') AS DAT_IMO,
         decode(s.aber_crg, 'VERDADEIRO', 1, 'FALSO', 0, 1) AS ABER_CRG,
         S.LOCAL_INSTALACAO AS DESCR
FROM 
    (SELECT *
    FROM MV_BDGD_SAP
    WHERE LOCAL_INSTALACAO LIKE 'RD%'
            AND obj_tecnico = 'RL' )s
LEFT JOIN BDGD_STAGING.MV_GENESIS_UNSEMT_RELIG gen
    ON gen.DESCR = s.local_instalacao
LEFT JOIN tcor
    ON tcor.corr = s.cor_nom
LEFT JOIN tcapelfu
    ON tcapelfu.cap = s.elo_fsv
LEFT JOIN TCLATEN 
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TMEIISO
    ON TMEIISO.DESCR = s.mei_iso 
LEFT JOIN TARE
    ON TARE.COD_ID = GEN.ARE_LOC
WHERE gen.PAC_1 is NOT null
        AND s.bdgd = 'EQSE'
        AND s.local_instalacao is NOT null
UNION
ALL
SELECT f.cod_id_novo AS COD_ID,
         '5697' AS DIST,
         COALESCE(to_char(gen.PAC_1), 'D1-'||f.cod_id_novo) PAC_1,
         COALESCE(to_char(gen.PAC_2), 'D2-'||f.cod_id_novo) PAC_2,
         nvl(TCLATEN.COD_ID,'0') AS CLAS_TEN,
         nvl(TCAPELFU.COD_ID, '0') AS ELO_FSV, --s.mei_iso, 
         nvl(TMEIISO.COD_ID,'0') AS MEI_ISO, 
         nvl(coalesce(f.fas_con, s.fas_con), '0') AS FAS_CON,
         nvl(tcor.cod_id,'0') AS COR_NOM,
         s.odi AS ODI,
         TARE.BDGD AS TI,
         nvl(s.centro_modular, '999') AS CM,
         s.tuc AS TUC,
         nvl(s.a1, '00')  A1,
         nvl(s.a2, '00')  A2,
         nvl(s.a3, '00')  A3,
         nvl(s.a4, '00')  A4,
         nvl(s.a5, '00')  A5,
         nvl(s.a6, '00')  A6,
         s.iduc AS IDUC,
         CASE
        WHEN s.TUC='000' THEN
        'COM'
        ELSE 'AT1'
        END AS SITCONT, 
         nvl(SUBSTR(s.data_imob, 0,10), '20/10/2010') AS DAT_IMO,
         decode(s.aber_crg, 'VERDADEIRO', 1, 'FALSO', 0, 1) AS ABER_CRG, S.LOCAL_INSTALACAO /*|| ',' || s.obj_tecnico*/ AS DESCR
FROM 
    (SELECT *
    FROM 
        (SELECT S1.*,
         RANK()
            OVER (PARTITION BY LOCAL_INSTALACAO
        ORDER BY  DATA_CONEXAO) RANK
        FROM MV_BDGD_SAP S1
        WHERE (LOCAL_INSTALACAO LIKE 'SE%'
                OR LOCAL_INSTALACAO LIKE 'RD%')
                AND obj_tecnico IN ('CD', 'CDP', 'CDA', 'CT', 'CDT', 'FA', 'BF') ) A2
        WHERE RANK = 1 ) s
    LEFT JOIN BDGD_STAGING.MV_GENESIS_UNSEMT_SECND gen
    ON gen.DESCR = s.local_instalacao
LEFT JOIN tab_bdgd_unsemt_secnd f
    ON f.cod_id_antigo = gen.COD_ID
LEFT JOIN tcor
    ON tcor.corr = s.cor_nom
LEFT JOIN tcapelfu
    ON tcapelfu.cap = s.elo_fsv
LEFT JOIN TCLATEN 
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TMEIISO
    ON TMEIISO.DESCR =s.mei_iso 
LEFT JOIN TARE
    ON TARE.COD_ID = GEN.ARE_LOC
WHERE gen.PAC_1 is NOT null
        AND s.bdgd = 'EQSE'
        --AND s.equipamento is null
        AND s.grupo_de_planejamento = 'GRD'
        AND s.local_instalacao NOT LIKE 'AX%'
        AND s.local_instalacao is NOT null
        AND s.obj_tecnico IN ('CD', 'CDP', 'CDA', 'CT', 'CDT', 'FA', 'BF') and f.cod_id_novo is NOT null
UNION
ALL
SELECT f.COD_ID_NOVO AS COD_ID,
         '5697' AS DIST, 
         COALESCE(to_char(gen.PAC_1), 'D1-'||f.cod_id_novo) PAC_1, 
         COALESCE(to_char(gen.PAC_2), 'D2-'||f.cod_id_novo) PAC_2, 
         TCLATEN.COD_ID AS CLAS_TEN, 
         nvl(TCAPELFU.COD_ID, '0') AS ELO_FSV, 
         TMEIISO.COD_ID AS MEI_ISO, 
         nvl(coalesce(f.fas_con, s.fas_con), '0') AS FAS_CON, 
         nvl(tcor.cod_id, '0') AS COR_NOM, 
         s.odi AS ODI, 
         TARE.BDGD AS TI, 
         nvl(s.centro_modular, '999') AS CM,
    CASE
    WHEN s.a2 < 34 THEN
    '000'
    ELSE s.tuc
    END AS TUC,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a1, '00')
    END AS A1,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a2, '00')
    END AS A2,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a3, '00')
    END AS A3,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a4, '00')
    END AS A4,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a5, '00')
    END AS A5,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a6, '00')
    END AS A6, 
    s.iduc AS IDUC, 
    CASE
    WHEN s.a2 < 34 THEN
    'COM'
    ELSE 'AT1'
    END AS SITCONT,  
    nvl(SUBSTR(s.data_imob, 0,10), '20/10/2010') AS DAT_IMO, 
    decode(s.aber_crg, 'VERDADEIRO', 1, 'FALSO', 0, 1) AS ABER_CRG, 
    S.LOCAL_INSTALACAO AS DESCR
FROM 
    (SELECT *
    FROM 
        (SELECT S1.*,
         RANK()
            OVER (PARTITION BY LOCAL_INSTALACAO
        ORDER BY  DATA_CONEXAO) RANK
        FROM MV_BDGD_SAP S1
        WHERE (LOCAL_INSTALACAO LIKE 'RD%')
                AND obj_tecnico IN ('FF', 'FP', 'FR', 'FT', 'FU', 'FA') ) A2
        WHERE RANK = 1 ) s
    LEFT JOIN BDGD_STAGING.MV_GENESIS_UNSEMT_FUSVL gen 
    ON gen.DESCR = s.local_instalacao
LEFT JOIN tab_bdgd_unsemt_fusvl f
    ON f.cod_id_antigo = gen.COD_ID
LEFT JOIN tcapelfu
    ON tcapelfu.cap = s.elo_fsv
LEFT JOIN tcor
    ON tcor.corr = s.cor_nom
LEFT JOIN TCLATEN 
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TMEIISO
    ON TMEIISO.DESCR = s.mei_iso 
LEFT JOIN TARE
    ON TARE.COD_ID = GEN.ARE_LOC
WHERE gen.PAC_1 is NOT null
        AND s.bdgd = 'EQSE'
        AND s.local_instalacao NOT LIKE 'AX%'
        AND s.local_instalacao is NOT null
        AND s.obj_tecnico IN ('FF', 'FP', 'FR', 'FT', 'FU', 'FA')

/*UNION
ALL

SELECT s.equipamento AS COD_ID,
    '5697' AS DIST, 
    COALESCE('SE-' || con.pac1_fid,'D1-'||s.equipamento) PAC_1,
    COALESCE('SE-' || con.pac2_fid,'D2-'||s.equipamento) PAC_2,
    TCLATEN.COD_ID AS CLAS_TEN, 
    nvl(TCAPELFU.COD_ID, '0') AS ELO_FSV, 
    TMEIISO.COD_ID AS MEI_ISO, 
    'ABC' AS FAS_CON, 
    nvl(tcor.cod_id, '0') AS COR_NOM, 
    s.odi AS ODI, 
    s.ti AS TI, 
    nvl(s.centro_modular, '999') AS CM,
    CASE
    WHEN s.a2 < 34 THEN
    '000'
    ELSE s.tuc
    END AS TUC,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a1, '00')
    END AS A1,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a2, '00')
    END AS A2,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a3, '00')
    END AS A3,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a4, '00')
    END AS A4,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a5, '00')
    END AS A5,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE nvl(s.a6, '00')
    END AS A6, 
    s.iduc AS IDUC,
    CASE
    WHEN s.a2 < 34 THEN
    'COM'
    ELSE 'AT1'
    END AS SITCONT,  
    nvl(SUBSTR(s.data_imob, 0,10), '20/10/2010') AS DAT_IMO, 
    decode(s.aber_crg, 'VERDADEIRO', 1, 'FALSO', 0, 1) AS ABER_CRG, 
    S.LOCAL_INSTALACAO AS DESCR
FROM 
    (SELECT *
    FROM 
        (SELECT S1.*,
         RANK()
            OVER (PARTITION BY LOCAL_INSTALACAO
        ORDER BY  DATA_CONEXAO) RANK
        FROM MV_BDGD_SAP S1
        WHERE (LOCAL_INSTALACAO LIKE 'SE%')
                AND obj_tecnico IN ('FF', 'FP', 'FR', 'FT', 'FU', 'FA') ) A2
        WHERE RANK = 1 ) s
    INNER JOIN 
    (SELECT MAX (pac1_fid)as pac1_fid,
        pac2_fid,
        pac2
    FROM BDGD_STAGING.tab_bdgd_sub_conn
    GROUP BY  pac2_fid,pac2) con
    ON con.pac2 = s.local_instalacao
LEFT JOIN tcapelfu
    ON tcapelfu.cap = s.elo_fsv
LEFT JOIN tcor
    ON tcor.corr = s.cor_nom
LEFT JOIN TCLATEN 
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TMEIISO
    ON TMEIISO.DESCR = s.mei_iso 
WHERE --gen.PAC_1 is NOT null
        --AND 
        s.bdgd = 'EQSE'
        --AND s.equipamento is null
        AND s.local_instalacao LIKE 'SE%'
        AND s.local_instalacao is NOT null
        AND s.obj_tecnico IN ('FF', 'FP', 'FR', 'FT', 'FU', 'FA')
*/

UNION 
ALL

SELECT TO_CHAR(gen.cod_id) AS COD_ID,
         '5697' AS DIST, 
         to_char(gen.PAC_1) PAC_1, 
         to_char(gen.PAC_2) PAC_2, 
         nvl(TCLATEN.COD_ID,'0') AS CLAS_TEN,  
         nvl(TCAPELFU.COD_ID, '0') AS ELO_FSV, 
         nvl(TMEIISO.COD_ID,'0') AS MEI_ISO, 
         nvl(gen.fas_con, '0') AS FAS_CON, 
         nvl(tcor.cod_id, '0') AS COR_NOM, 
         s.odi AS ODI, 
         TARE.BDGD AS TI, 
         nvl(s.centro_modular, '999') AS CM,
    CASE
    WHEN s.a2 < 34 THEN
    '000'
    ELSE s.tuc
    END AS TUC,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE NVL(s.a1, '00')
    END AS A1,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE NVL(s.a2, '00')
    END AS A2,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE NVL(s.a3, '00')
    END AS A3,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE NVL(s.a4,'00')
    END AS A4,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE NVL(s.a5, '00')
    END AS A5,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
    ELSE NVL(s.a6, '00')
    END AS A6, 
    s.iduc AS IDUC, 
    CASE
    WHEN s.a2 < 34 THEN
    'COM'
    ELSE 'AT1'
    END AS SITCONT,  
    nvl(SUBSTR(s.data_imob, 0,10), '20/10/2010') AS DAT_IMO, 
    decode(s.aber_crg, 'VERDADEIRO', 1, 'FALSO', 0, 1) AS ABER_CRG, 
    S.LOCAL_INSTALACAO AS DESCR
FROM 
    (SELECT *
    FROM 
        (SELECT S1.*,
         RANK()
            OVER (PARTITION BY LOCAL_INSTALACAO
        ORDER BY  DATA_CONEXAO) RANK
        FROM MV_BDGD_SAP S1
        WHERE (LOCAL_INSTALACAO LIKE 'SE%'
                OR LOCAL_INSTALACAO LIKE 'RD%')
                AND obj_tecnico IN ('SL') ) A2
        WHERE RANK = 1 ) s
    LEFT JOIN BDGD_STAGING.MV_GENESIS_UNSEMT_SECLZ gen
    ON gen.DESCR = s.local_instalacao
LEFT JOIN tcapelfu
    ON tcapelfu.cap = s.elo_fsv
LEFT JOIN tcor
    ON tcor.corr = s.cor_nom
LEFT JOIN TCLATEN 
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TMEIISO
    ON TMEIISO.DESCR = s.mei_iso 
LEFT JOIN TARE
    ON TARE.COD_ID = GEN.ARE_LOC
WHERE gen.PAC_1 is NOT null
        AND s.bdgd = 'EQSE'
        AND s.grupo_de_planejamento = 'GRD'
        AND s.local_instalacao NOT LIKE 'AX%'
        AND s.local_instalacao is NOT null
        AND s.obj_tecnico IN ('SL')        

/* UNION
 ALL

 SELECT  s.equipamento COD_ID,
       '5697' DIST,
       COALESCE('SE-' || con.pac1_fid,'D1-'||s.equipamento) PAC_1,
       COALESCE('SE-' || con.pac2_fid,'D2-'||s.equipamento) PAC_2,
       TCLATEN.COD_ID as CLAS_TEN,
       nvl(TCAPELFU.COD_ID, '0') as ELO_FSV,
       TMEIISO.COD_ID as MEI_ISO,
      'ABC' AS FAS_CON,
       nvl(tcor.cod_id, '0')  COR_NOM,
       s.odi  ODI,
       s.ti  TI,
       s.centro_modular  CM,
       s.tuc  TUC,
       nvl(s.a1, '00')  A1,
       nvl(s.a2, '00')  A2,
       nvl(s.a3, '00')  A3,
       nvl(s.a4, '00')  A4,
       nvl(s.a5, '00')  A5,
       nvl(s.a6, '00')  A6,
       s.iduc  IDUC,
       CASE
        WHEN s.TUC = '000' THEN
        'COM'
        ELSE 'AT1'
        END AS SITCONT, 
       nvl(SUBSTR(s.data_imob, 0,10), '20/10/2010') as DAT_IMO,
       decode(s.aber_crg, 'VERDADEIRO', 1,
                          'FALSO',      0,
                          1) as "ABER_CRG",
       S.LOCAL_INSTALACAO DESCR
FROM 
    (SELECT COR_NOM,
         ELO_FSV,
        LOCAL_INSTALACAO,
         A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        IDUC,
        DATA_IMOB,
        ABER_CRG,
        TUC,
        CENTRO_MODULAR,
         TI,
        ODI,
        MEI_ISO,
        EQUIPAMENTO,
         OBJ_TECNICO,
        CLAS_TEN
    FROM 
        (SELECT S1.*,
         RANK()
            OVER (PARTITION BY LOCAL_INSTALACAO
        ORDER BY  DATA_CONEXAO) RANK
        FROM MV_BDGD_SAP S1
        WHERE LOCAL_INSTALACAO LIKE 'SE%'
                AND TUC='160'
                AND OBJ_TECNICO IN ('CN', 'CO', 'CQ', 'CU', 'CUA', 'CT', 'CDT', 'CD') ) A2
        WHERE RANK = 1) s
    INNER JOIN 
    (SELECT MAX (pac1_fid)as pac1_fid,
        pac2_fid,
        pac2
    FROM BDGD_STAGING.tab_bdgd_sub_conn
    GROUP BY  pac2_fid,pac2) con
    ON con.pac2 = s.local_instalacao --Tirar duplicadas
LEFT JOIN tcapelfu
    ON tcapelfu.cap = s.elo_fsv
LEFT JOIN tcor
    ON tcor.corr = s.cor_nom
LEFT JOIN TCLATEN 
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TMEIISO
    ON TMEIISO.DESCR = s.mei_iso;*/
    --ON TMEIISO.DESCR = CONVERT(s.mei_iso ,'WE8ISO8859P15', 'UTF8')

-- UNION ALL SELECT * FROM TAB_BDGD_AUX_EQSE"
--Mudanca comeca aqui
union all
select DECODE(SUB_GEN.R, 3, s.equipamento||'-3', s.equipamento)  COD_ID,
       '5697' DIST,
      coalesce(p.pac_1, 'SE-'||SUB_GEN.NR_PTO_ELET_INICIO) PAC_1,
      coalesce(p.pac_2, 'SE-'||SUB_GEN.NR_PTO_ELET_FIM) PAC_2,
       nvl(TCLATEN.COD_ID,'0') AS CLAS_TEN, 
       nvl(TCAPELFU.COD_ID, '0') as ELO_FSV,
       nvl(TMEIISO.COD_ID,'0') AS MEI_ISO, 
      'ABC' AS FAS_CON,
       nvl(tcor.cod_id, '0')  COR_NOM,
       s.odi  ODI,
       s.ti  TI,
       s.centro_modular  CM,
       s.tuc  TUC,
       nvl(s.a1, '00')  A1,
       nvl(s.a2, '00')  A2,
       nvl(s.a3, '00')  A3,
       nvl(s.a4, '00')  A4,
       nvl(s.a5, '00')  A5,
       nvl(s.a6, '00')  A6,
       s.iduc  IDUC,
        CASE
        WHEN S.TUC = '000' THEN
        'COM'
        ELSE 'AT1'
        END AS SITCONT, 
       nvl(SUBSTR(s.data_imob, 0,10), '20/10/2010') as DAT_IMO,
       decode(s.aber_crg, 'VERDADEIRO', 1,
                          'FALSO',      0,
                          1) as "ABER_CRG",
       S.LOCAL_INSTALACAO DESCR
from MV_BDGD_SAP s
INNER JOIN (
  SELECT * FROM (SELECT A.*, RANK() OVER (PARTITION BY LOCAL_INST ORDER BY ID) R FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO A WHERE TIPO_EQPTO = 'CBY') WHERE R != 2
  UNION
  ALL
  SELECT S.*,4 AS R FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO S WHERE S.ID_SITUACAO_OPER_EQPTO = 2 AND S.TIPO_EQPTO='CN'
  UNION
  ALL
  SELECT S.*,4 AS R FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO S WHERE S.tipo_eqpto in ('CD','CF','CO','CQ','CT','CU','CUA','DJ','FU','RL')
  ) SUB_GEN ON SUB_GEN.LOCAL_INST = LOCAL_INSTALACAO
LEFT JOIN tcapelfu
    ON tcapelfu.cap = s.elo_fsv
LEFT JOIN tcor
    ON tcor.corr = s.cor_nom
LEFT JOIN TCLATEN 
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TMEIISO
    ON TMEIISO.DESCR = s.mei_iso
left join tab_aux_pac_se_re p
    on p.loc_inst = SUB_GEN.LOCAL_INST
WHERE s.obj_tecnico in ('CBY','CN','CO','CQ','CU','CUA','CD','CDT','CF','FU','DJ','RL')
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQTRD" ("COD_ID", "DIST", "PAC_1", "PAC_2", "PAC_3", "CLAS_TEN", "POT_NOM", "LIG", "FAS_CON", "TEN_PRI", "TEN_SEC", "TEN_TER", "LIG_FAS_P", "LIG_FAS_S", "LIG_FAS_T", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "SITCONT", "DAT_IMO", "PER_FER", "PER_TOT", "R", "XHL", "XHT", "XLT", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT S.EQUIPAMENTO AS "COD_ID",
'5697' AS "DIST",
CASE
WHEN GENESIS.PAC1 is null or GENESIS.PAC1=0 THEN 'D1-'||S.EQUIPAMENTO
ELSE CAST(GENESIS.PAC1 AS VARCHAR2(20)) END AS PAC_1,
CASE
WHEN GENESIS.PAC2 is null or GENESIS.PAC2=0 THEN 'D2-'||S.EQUIPAMENTO
ELSE CAST(GENESIS.PAC2 AS VARCHAR2(20)) END AS PAC_2,
'D3-'||S.EQUIPAMENTO AS "PAC_3",
    TCLATEN.COD_ID AS "CLAS_TEN",
TPOTAPRT.COD_ID AS "POT_NOM",
--Caso LIG Nulo no SAP pegar no Genisis FAS_CON
COALESCE(TLIG.COD_ID, DECODE(GENESIS.FAS_CON_P, '7', '2','6')) AS "LIG",
--TLIG.COD_ID AS "LIG",
SUGERE.FAS_CON_P AS "FAS_CON",
NVL(T1.COD_ID,0) AS "TEN_PRI",
--NVL(T2.COD_ID,0) AS "TEN_SEC",
DECODE(LENGTH(SUGERE.FAS_CON_S), 2, '17', 4, DECODE(T2.COD_ID, '10', '10', '15'), '15') TEN_SEC,
'0' AS "TEN_TER",
SUGERE.FAS_CON_P AS "LIG_FAS_P",
SUGERE.FAS_CON_S AS "LIG_FAS_S",
SUGERE.FAS_CON_T  AS "LIG_FAS_T",
DECODE(GENESIS.TIPO, '1', '190400', '2', '190400', '190401') AS "ODI",
DECODE((GENESIS.TIPO||GENESIS.ARE_LOC), '1U', '40',
                                '1R', '41',
                                '2U', '40',
                                '2R', '41',
                                '3R', '43',
                                '4R', '43',
                                '5R', '43',
                                '6R', '43',
                                '3U', '42',
                                '4U', '42',
                                '5U', '42',
                                '6U', '42') AS "TI",
NVL(S.CENTRO_MODULAR, '999') AS "CM",
NVL(S.TUC, '000') AS "TUC",
NVL(S.A1, '00') AS "A1",
NVL(S.A2, '00') AS "A2",
NVL(S.A3, '00') AS "A3",
NVL(S.A4, '00') AS "A4",
NVL(S.A5, '00') AS "A5",
NVL(S.A6, '00') AS "A6",
DECODE(S.DATA_IMOB, NULL, 'NIM', 'AT1') AS "SITCONT",
COALESCE(S.DATA_IMOB,'20/12/2010') AS "DAT_IMO",
CAST (TRUNC(REPLACE(S.PER_FER, ',','.'),3)as NUMBER) AS "PER_FER",
CAST (TRUNC(REPLACE(S.PER_TOT,',','.'),3)as NUMBER) AS "PER_TOT",
CAST (TRUNC(REPLACE(S.R,',','.'),3) as NUMBER) AS "R",
CAST (TRUNC(REPLACE(S.XHL,',','.'),3)as NUMBER) AS "XHL",
0 AS "XHT",
0 AS "XLT",
S.LOCAL_INSTALACAO AS "DESCR"
FROM MV_BDGD_SAP S
LEFT JOIN BDGD_STAGING.MV_GENESIS_EQTRD GENESIS ON GENESIS.EQUIPAMENTO = S.LOCAL_INSTALACAO
LEFT JOIN TLIG ON TLIG.DESCR = S.LIG
LEFT JOIN TCLATEN ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TPOTAPRT ON TPOTAPRT.POT = S.POT_NOM
LEFT JOIN TTEN T1 ON T1.TEN = S.TEN_PRI
LEFT JOIN TTEN T2 ON T2.TEN = S.TEN_SEC
LEFT JOIN (SELECT
 DISTINCT FEATURE_ID,
          FASE_TRAFO_PRIMARIO AS "FAS_CON_P",
          FASE_TRAFO_SECUNDARIO AS "FAS_CON_S",
          FASE_TRAFO_TERCIARIO AS "FAS_CON_T"
 FROM BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT )SUGERE ON SUGERE.FEATURE_ID=GENESIS.ID
where s.bdgd = 'EQTRD'
AND (S.LOCAL_INSTALACAO LIKE 'RD%' OR S.LOCAL_INSTALACAO LIKE 'O-%') AND SUGERE.FAS_CON_P IS NOT NULL 
UNION ALL
SELECT S.EQUIPAMENTO AS "COD_ID",
       '5697' AS "DIST",
         DECODE(SUB.NR_PTO_ELET_INICIO, NULL,'D1-'||S.EQUIPAMENTO,'SE-'||SUB.NR_PTO_ELET_INICIO) AS "PAC_1", 
         DECODE(SUB.NR_PTO_ELET_FIM, NULL,'D2-'||S.EQUIPAMENTO,'SE-'||SUB.NR_PTO_ELET_FIM) AS "PAC_2", 
         'D3-'||S.EQUIPAMENTO AS "PAC_3",
       TCLATEN.COD_ID AS "CLAS_TEN",
CASE
WHEN A4 = 61 THEN '40'
WHEN A4 = 63 THEN '43'
WHEN A4 = 65 THEN '45'
WHEN A4 = 70 THEN '50'
WHEN A4 = 72 THEN '54'
WHEN A4 = 73 THEN '58'
WHEN A4 = 75 THEN '61'
WHEN A4 = 75 THEN '62'
WHEN A4 = 77 THEN '65'
WHEN A4 = 78 THEN '65'
WHEN A4 = 80 THEN '68'
WHEN A4 = 82 THEN '74'
WHEN A4 = 83 THEN '76'
WHEN A4 = 85 THEN '78'
WHEN A4 = 86 THEN '80'
WHEN A4 = 87 THEN '82'
WHEN A4 = 88 THEN '83'
WHEN A4 = 89 THEN '85'
WHEN A4 = 90 THEN '85'
WHEN A4 = 91 THEN '88'
WHEN A4 = 92 THEN '91'
WHEN A4 = 95 THEN '95'
ELSE '0' END AS "POT_NOM",
       TLIG.COD_ID AS "LIG",
       'ABC' AS "FAS_CON",
       T1.COD_ID AS "TEN_PRI",
       T2.COD_ID AS "TEN_SEC",
       NVL(T3.COD_ID, '0') AS "TEN_TER",
       'ABC' AS "LIG_FAS_P",
       'ABC' AS "LIG_FAS_S",
       '0' AS "LIG_FAS_T",
        S.ODI AS "ODI",
        S.TI AS "TI",
        S.CENTRO_MODULAR AS "CM",
        S.TUC AS "TUC",
        NVL(S.A1,'00') AS "A1",
        NVL(S.A2, '00') AS "A2",
        NVL(S.A3, '00') AS "A3",
        NVL(S.A4, '00') AS "A4",
        NVL(S.A5, '00') AS "A5",
        NVL(S.A6, '00') AS "A6",
        DECODE(S.DATA_IMOB, NULL, 'NIM', 'AT1') AS "SITCONT",
        S.DATA_IMOB AS "DAT_IMO",
        TRUNC(REPLACE(S.PER_FER, ',','.'),3) AS "PER_FER",
        TRUNC(REPLACE(S.PER_TOT,',','.'),3) AS "PER_TOT",
        TRUNC(REPLACE(S.R,',','.'),3) AS "R",
        TRUNC(REPLACE(S.XHL,',','.'),3) AS "XHL",
        0 AS "XHT",
        0 AS "XLT",
       S.local_instalacao AS "DESCR"
FROM MV_BDGD_SAP S
     LEFT JOIN TCLATEN
          ON TCLATEN.TEN = S.CLAS_TEN
     LEFT JOIN TPOTAPRT
          ON TPOTAPRT.POT = S.POT_NOM
     LEFT JOIN TLIG
          ON TLIG.DESCR = S.LIG
     LEFT JOIN TTEN T1
          ON T1.TEN = S.TEN_PRI
     LEFT JOIN TTEN T2
          ON T2.TEN = S.TEN_SEC
     LEFT JOIN TTEN T3
          ON T3.TEN = S.TEN_TER
    LEFT JOIN (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE TIPO_EQPTO = 'TT') SUB
ON S.LOCAL_INSTALACAO = SUB.LOCAL_INST
where s.bdgd = 'EQTRS'
AND S.LOCAL_INSTALACAO LIKE 'SE-0%'
AND TCLATEN.COD_ID < 11
UNION ALL
SELECT 
EQP3.COD_ID,
CAST (EQP3.DIST as VARCHAR(4)),
CAST (EQP3.PAC_1 as VARCHAR(50)),
CAST (EQP3.PAC_2 as VARCHAR(50)),
CAST (EQP3.PAC_3 as VARCHAR2(50)),
CAST (EQP3.CLAS_TEN as VARCHAR2(2)),
EQP3.POT_NOM,
CAST(EQP3.LIG as VARCHAR2(2)),
EQP3.FAS_CON,
CAST(EQP3.TEN_PRI as VARCHAR2(4)),
CAST(EQP3.TEN_SEC as VARCHAR2(4)),
CAST(EQP3.TEN_TER as VARCHAR2(4)),
EQP3.LIG_FAS_P,
EQP3.LIG_FAS_S,
CAST(EQP3.LIG_FAS_T as VARCHAR2(4)),
CAST(EQP3.ODI as VARCHAR2(255)),
EQP3.TI,
CAST(EQP3.CM as VARCHAR2(255)),
EQP3.TUC,EQP3.A1,EQP3.A2,EQP3.A3,EQP3.A4,EQP3.A5,EQP3.A6,
CAST(EQP3.SITCONT as VARCHAR2(255)),
EQP3.DAT_IMO,
CASE
WHEN EQP3.PER_FER=0 THEN 85
ELSE EQP3.PER_FER END as PER_FER,
CASE
WHEN EQP3.PER_TOT=0 THEN 300
ELSE EQP3.PER_TOT END as PER_TOT,
CASE
WHEN EQP3.R=0 THEN 2.15
ELSE EQP3.R END as R,
CASE
WHEN EQP3.XHL=0 THEN 1.28
ELSE EQP3.XHL END as XHL,
EQP3.XHT,EQP3.XLT,EQP3.DESCR
FROM gen_bdgd2019.V_BDGD_EQTRD_PARTICULAR3 EQP3
INNER JOIN MV_BDGD_UNTRD UNTRD ON UNTRD.PAC_2=CAST(EQP3.PAC_2 AS VARCHAR(100))
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNREMT_TEMP" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "PAC_1", "PAC_2", "UNI_TR_S", "CTMT", "SUB", "CONJ", "MUN", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
unremt.cod_id,
unremt.dist,
unremt.fas_con,
unremt.sit_ativ,
unremt.tip_unid,
coalesce(ajust.pac_1,unremt.pac_1) as pac_1,
coalesce(ajust.pac_2,unremt.pac_2) as pac_2,
coalesce(ajust.uni_tr_s,unremt.uni_tr_s) as uni_tr_s,
coalesce(ajust.ctmt,unremt.ctmt) as ctmt,
coalesce(ajust.sub,unremt.sub) as sub,
unremt.conj,
unremt.mun,
unremt.dat_con,
unremt.banc,
unremt.pos,
unremt.descr,
unremt.geometry
from mv_bdgd_unremt unremt left join 
(
select distinct local_inst,eqpto.sub,ctmt,ten_nom,uni_tr_s,'D-'||LOCAL_INST as pac_1,pac as pac_2 from
(select * from 
GEN_BDGD2019.mv_eqptos_subestacao 
where id_tipo_modulo != 1 and 
id_tn_nominal_eqpto < 6 and 
tipo_eqpto in ('RG')) EQPTO 
inner join (select sub,
cod_id as ctmt,
pac,
uni_tr_s,
decode(ten_nom,72,5,62,4,49,3,NULL) as ten_nom
from mv_bdgd_ctmt where cod_id in (
select min(COD_ID) from mv_bdgd_ctmt group by sub,ten_nom)) ctmt on ctmt.sub = eqpto.sub and ctmt.ten_nom = eqpto.id_tn_nominal_eqpto ) ajust 
on ajust.local_inst = unremt.descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNSEMT" ("COD_ID", "DIST", "PAC_1", "PAC_2", "FAS_CON", "SIT_ATIV", "TIP_UNID", "P_N_OPE", "CAP_ELO", "COR_NOM", "TLCD", "DAT_CON", "POS", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "ARE_LOC", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT   TO_CHAR(GEN.COD_ID) COD_ID,
            GEN.DIST,
            COALESCE(TO_CHAR(GEN.PAC_1),
            'DESC_' || TO_CHAR(GEN.COD_ID)) PAC_1, 
            COALESCE(TO_CHAR(GEN.PAC_2),
            'DESC_' || TO_CHAR(GEN.COD_ID)) PAC_2, 
            GEN.FAS_CON, 'AT' SIT_ATIV, 
            GEN.TIP_UNID, GEN.P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            NVL(TCOR.COD_ID, '0') COR_NOM,
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(SAP.DATA_CONEXAO,'01/01/1900', '24/10/2010', SAP.DATA_CONEXAO) DAT_CON, 
            GEN.POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,GEN.CTMT,'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,TAB.LOC_INST_SAP_TT,'0') AS "UNI_TR_S",
            COALESCE(GEN.SUB,'0') as SUB, 
            GEN.CONJ, 
            GEN.MUN, 
            GEN.ARE_LOC, 
            SAP.LOCAL_INSTALACAO AS "DESCR", 
            GEN.GEOMETRY
    FROM BDGD_STAGING.MV_GENESIS_UNSEMT_1 GEN
    LEFT JOIN MV_BDGD_SAP SAP
        ON SAP.EQUIPAMENTO = GEN.NR_EQUIPAMENTO
    LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
        ON TAB.CD_ALIMENTADOR = GEN.CTMT
    LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
        ON NV.ALIMENTADOR = TAB.CD_ALIMENTADOR
    LEFT JOIN TCAPELFU
        ON TCAPELFU.CAP = REPLACE(SAP.ELO_FSV, ',0', '')
    LEFT JOIN TCOR
        ON TCOR.CORR = SAP.COR_NOM
    WHERE SAP.A2 < 60
            OR SAP.A2 IS NULL
    UNION
    ALL
    SELECT TO_CHAR(GEN.COD_ID),
            GEN.DIST,
            COALESCE(TO_CHAR(GEN.PAC_1),
            'DESC_'||TO_CHAR(GEN.COD_ID)) PAC_1, 
            COALESCE(TO_CHAR(GEN.PAC_2),'DESC_'||TO_CHAR(GEN.COD_ID)) PAC_2, 
            GEN.FAS_CON, 'AT' SIT_ATIV, 
            GEN.TIP_UNID, GEN.P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            NVL(TCOR.COD_ID, '0') COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(SAP.DATA_CONEXAO,'01/01/1900', '24/10/2010', SAP.DATA_CONEXAO) DAT_CON, 
            GEN.POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,GEN.CTMT,'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,TAB.LOC_INST_SAP_TT,'0') AS "UNI_TR_S",
            COALESCE(GEN.SUB,'0') as SUB, 
            GEN.CONJ, 
            GEN.MUN, GEN.ARE_LOC, 
            SAP.LOCAL_INSTALACAO AS "DESCR", 
            GEN.GEOMETRY
    FROM BDGD_STAGING.MV_GENESIS_UNSEMT_2 GEN
    LEFT JOIN 
        (SELECT *
        FROM MV_BDGD_SAP S
        WHERE S.BDGD = 'EQSE'
                AND S.OBJ_TECNICO IN ('BF','CD', 'CDA', 'CDP', 'CDT', 'CO', 'FA', 'FF', 'FP', 'FR', 'FT', 'FU')) SAP
        ON SAP.LOCAL_INSTALACAO = GEN.DESCR
    LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
        ON TAB.CD_ALIMENTADOR = GEN.CTMT
    LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
        ON NV.ALIMENTADOR = TAB.CD_ALIMENTADOR
    LEFT JOIN TCAPELFU
        ON TCAPELFU.CAP = REPLACE(SAP.ELO_FSV, ',0', '')
    LEFT JOIN TCOR
        ON TCOR.CORR = SAP.COR_NOM
    WHERE SAP.A2 < 60
            OR SAP.A2 IS NULL
    UNION
    ALL
    -----Subestacao
    select k.cod_id,
           k.dist,
           coalesce(p.pac_1, k.pac_1) pac_1,
           coalesce(p.pac_2, k.pac_2) pac_2,
           k.fas_con,
           k.SIT_ATIV, 
           k.TIP_UNID, 
           k.P_N_OPE, 
           k.CAP_ELO, 
           k.COR_NOM,
           k.TLCD, 
           k.DAT_CON, 
           k.POS, 
           k.CTMT, 
           k.UNI_TR_S,
           k.SUB, 
           k.CONJ, 
           k.MUN, 
           k.ARE_LOC, 
           k.DESCR, 
           k.GEOMETRY
    from
    (
    SELECT A.*,
            G.sub_geometry GEOMETRY
        FROM (SELECT'' || sub.ID COD_ID, '5697' DIST,
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_INICIO) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_INICIO)
            ELSE 'D1-'||sub.NR_PTO_ELET_INICIO
            END AS PAC_1, 
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_FIM) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_FIM)
            ELSE 'D2-'||sub.NR_PTO_ELET_FIM
            END AS PAC_2, 
            'ABC' FAS_CON, 
            'AT' SIT_ATIV, 
            DECODE(S.TUC, 160, DECODE(S.A1, 3, 22, 34), 210, 29, 345, 32) TIP_UNID, 
            decode(sub.situacao_operacional, 1, 'A', 2, 'F', 3, 'F', '-1') P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            TCOR.COD_ID COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(S.DATA_CONEXAO,'01/01/1900', '24/10/2010', S.DATA_CONEXAO) DAT_CON, 
            'PD' POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,DECODE(SUB.ID_TIPO_MODULO,1,SUB.CD_ALIMENTADOR,'0'),'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
            COALESCE(SUB.sub,'0') SUB, 
            sub.conj CONJ, 
            sub.municipio MUN, 
            DECODE(SUB.ARE_LOC, 'U', 'UB', 'R', 'NU', NULL) ARE_LOC,
            Sub.LOCAL_INST DESCR--, --NODE.SHAPE GEOMETRY
        FROM 
            (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto in ('CBY','CD','CF','CN','CO','CQ','CT','CU','CUA','DJ','FU','RL') AND id_tn_nominal_eqpto < '6' AND (FEATURE_TYPE != 'LocalInstalacaoEqptoOperTandem' AND FEATURE_TYPE !='LocalInstalacaoEqptoOperByPass')) SUB
            LEFT JOIN 
            (SELECT A2.EQUIPAMENTO,
            A2.STATUS_SE,
            A2.COR_NOM,
            A2.A1,
            A2.TUC,
            A2.A3,
            A2.A2,
            A2.BDGD,
            A2.ELO_FSV,
            A2.DATA_CONEXAO,
            A2.LOCAL_INSTALACAO
            FROM 
                (SELECT S1.*,
            ROW_NUMBER()
                    OVER (PARTITION BY LOCAL_INSTALACAO
                ORDER BY  DATA_CONEXAO DESC) ROW_NUMBER
                FROM MV_BDGD_SAP S1
                WHERE LOCAL_INSTALACAO LIKE 'SE%'
                        AND BDGD ='EQSE' ) A2
                WHERE ROW_NUMBER = 1
                GROUP BY  A2.LOCAL_INSTALACAO,A2.COR_NOM, A2.STATUS_SE, A2.A1, A2.TUC, A2.A3, A2.A2, A2.BDGD, A2.ELO_FSV, A2.DATA_CONEXAO, A2.EQUIPAMENTO ) S
                    ON S.LOCAL_INSTALACAO = SUB.LOCAL_INST
                /*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN CON
                    ON CON.PAC2 = S.LOCAL_INSTALACAO
                LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
                    ON NODE.LOC_INST_SAP = CON.PAC2*/                LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
                    ON TAB.CD_ALIMENTADOR = SUB.CD_ALIMENTADOR
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_ALIMENTADOR ALI
                    ON ALI.CD_ALIMENTADOR = TAB.CD_ALIMENTADOR
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
                    --ON SUB.NR_SUBESTACAO = TO_NUMBER(SUBSTR(NODE.LOC_INST_SAP, 4, 3))
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONJUNTO_ANEEL CONJ
                    --ON CONJ.PRI_CONJUNTO_CNS_ID = SUB.PRI_CONJUNTO_CNS_ID
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_MUNICIPIO_ANEEL MUN
                    --ON MUN.PRI_MUNICIPIO_ID = SUB.PRI_MUNICIPIO_ID
                LEFT JOIN (SELECT distinct substr(alimentador,1,3) as SUB, novo_alimentador,un_tr_s FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) NV ON NV.SUB = SUB.SUB
                --LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2
                    --ON NODE2.LOC_INST_SAP = NODE.UN_TR_S
                LEFT JOIN TCAPELFU
                    ON TCAPELFU.CAP = REPLACE(S.ELO_FSV, ',0', '')
                LEFT JOIN TCOR
                    ON TCOR.CORR = S.COR_NOM
                --WHERE S.BDGD = 'EQSE'
                        --AND S.A2 < 60
                GROUP BY  
                SUB.ID,
                SUB.LOCAL_INST,
                sub.nr_pto_elet_fim, 
                S.STATUS_SE, 
                S.A1, 
                S.TUC, 
                TCOR.COD_ID, 
                S.A3, 
                TCAPELFU.COD_ID, 
                S.DATA_CONEXAO, 
                sub.CD_ALIMENTADOR, 
                sub.local_inst_tt, 
                SUB.sub, 
                sub.conj, 
                sub.municipio, 
                SUB.ARE_LOC, 
                S.LOCAL_INSTALACAO, 
                NV.NOVO_ALIMENTADOR,
                NR_PTO_ELET_INICIO,
                sub.situacao_operacional,
                --NODE.UN_TR_S,
                --NODE2.OBJECTID, 
                NV.UN_TR_S,
                SUB.ID_TIPO_MODULO ) A
            LEFT JOIN GEN_BDGD2019.MV_EQPTOS_SUBESTACAO G
            ON G.local_inst = A.DESCR
    UNION ALL --CHAVE TANDEM   
       SELECT A.*,
            G.sub_geometry GEOMETRY
        FROM (SELECT'' || sub.ID COD_ID, '5697' DIST,
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_INICIO) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_INICIO)
            ELSE 'D1-'||sub.NR_PTO_ELET_INICIO
            END AS PAC_1, 
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_FIM) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_FIM)
            ELSE 'D2-'||sub.NR_PTO_ELET_FIM
            END AS PAC_2, 
            'ABC' FAS_CON, 
            'AT' SIT_ATIV, 
            DECODE(S.TUC, 160, DECODE(S.A1, 3, 22, 34), 210, 29, 345, 32) TIP_UNID, 
            decode(sub.situacao_operacional, 1, 'A', 2, 'F', 3, 'F', '-1') P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            TCOR.COD_ID COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(S.DATA_CONEXAO,'01/01/1900', '24/10/2010', S.DATA_CONEXAO) DAT_CON, 
            'PD' POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,DECODE(SUB.ID_TIPO_MODULO,1,SUB.CD_ALIMENTADOR,'0'),'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
            COALESCE(SUB.sub,'0') SUB, 
            sub.conj CONJ, 
            sub.municipio MUN, 
            DECODE(SUB.ARE_LOC, 'U', 'UB', 'R', 'NU', NULL) ARE_LOC,
            Sub.LOCAL_INST DESCR--, --NODE.SHAPE GEOMETRY
        FROM 
            (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto in ('CBY','CD','CF','CN','CO','CQ','CT','CU','CUA','DJ','FU','RL') AND id_tn_nominal_eqpto < '6' AND FEATURE_TYPE = 'LocalInstalacaoEqptoOperTandem' AND ID_SITUACAO_OPER_EQPTO = 2) SUB
            LEFT JOIN 
            (SELECT A2.EQUIPAMENTO,
            A2.STATUS_SE,
            A2.COR_NOM,
            A2.A1,
            A2.TUC,
            A2.A3,
            A2.A2,
            A2.BDGD,
            A2.ELO_FSV,
            A2.DATA_CONEXAO,
            A2.LOCAL_INSTALACAO
            FROM 
                (SELECT S1.*,
            ROW_NUMBER()
                    OVER (PARTITION BY LOCAL_INSTALACAO
                ORDER BY  DATA_CONEXAO DESC) ROW_NUMBER
                FROM MV_BDGD_SAP S1
                WHERE LOCAL_INSTALACAO LIKE 'SE%'
                        AND BDGD ='EQSE' ) A2
                WHERE ROW_NUMBER = 1
                GROUP BY  A2.LOCAL_INSTALACAO,A2.COR_NOM, A2.STATUS_SE, A2.A1, A2.TUC, A2.A3, A2.A2, A2.BDGD, A2.ELO_FSV, A2.DATA_CONEXAO, A2.EQUIPAMENTO ) S
                    ON S.LOCAL_INSTALACAO = SUB.LOCAL_INST
                /*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN CON
                    ON CON.PAC2 = S.LOCAL_INSTALACAO
                LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
                    ON NODE.LOC_INST_SAP = CON.PAC2*/                LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
                    ON TAB.CD_ALIMENTADOR = SUB.CD_ALIMENTADOR
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_ALIMENTADOR ALI
                    ON ALI.CD_ALIMENTADOR = TAB.CD_ALIMENTADOR
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
                    --ON SUB.NR_SUBESTACAO = TO_NUMBER(SUBSTR(NODE.LOC_INST_SAP, 4, 3))
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONJUNTO_ANEEL CONJ
                    --ON CONJ.PRI_CONJUNTO_CNS_ID = SUB.PRI_CONJUNTO_CNS_ID
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_MUNICIPIO_ANEEL MUN
                    --ON MUN.PRI_MUNICIPIO_ID = SUB.PRI_MUNICIPIO_ID
               LEFT JOIN (SELECT distinct substr(alimentador,1,3) as SUB, novo_alimentador,un_tr_s FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) NV ON NV.SUB = SUB.SUB
                --LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2
                    --ON NODE2.LOC_INST_SAP = NODE.UN_TR_S
                LEFT JOIN TCAPELFU
                    ON TCAPELFU.CAP = REPLACE(S.ELO_FSV, ',0', '')
                LEFT JOIN TCOR
                    ON TCOR.CORR = S.COR_NOM
                --WHERE S.BDGD = 'EQSE'
                        --AND S.A2 < 60
                GROUP BY  
                SUB.ID,
                SUB.LOCAL_INST,
                sub.nr_pto_elet_fim, 
                S.STATUS_SE, 
                S.A1, 
                S.TUC, 
                TCOR.COD_ID, 
                S.A3, 
                TCAPELFU.COD_ID, 
                S.DATA_CONEXAO, 
                sub.CD_ALIMENTADOR, 
                sub.local_inst_tt, 
                SUB.sub, 
                sub.conj, 
                sub.municipio, 
                SUB.ARE_LOC, 
                S.LOCAL_INSTALACAO, 
                NV.NOVO_ALIMENTADOR,
                NR_PTO_ELET_INICIO,
                sub.situacao_operacional,
                --NODE.UN_TR_S,
                --NODE2.OBJECTID, 
                NV.UN_TR_S,
                SUB.ID_TIPO_MODULO ) A
            LEFT JOIN (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE ID_SITUACAO_OPER_EQPTO = 2) G
            ON G.local_inst = A.DESCR
    UNION ALL --CHAVE BY PASS    
    SELECT A.*,
            G.sub_geometry GEOMETRY
        FROM (SELECT'' || sub.ID COD_ID, '5697' DIST,
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_INICIO) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_INICIO)
            ELSE 'D1-'||sub.NR_PTO_ELET_INICIO
            END AS PAC_1, 
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_FIM) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_FIM)
            ELSE 'D2-'||sub.NR_PTO_ELET_FIM
            END AS PAC_2, 
            'ABC' FAS_CON, 
            'AT' SIT_ATIV, 
            DECODE(S.TUC, 160, DECODE(S.A1, 3, 22, 34), 210, 29, 345, 32) TIP_UNID, 
            decode(sub.situacao_operacional, 1, 'A', 2, 'F', 3, 'F', '-1') P_N_OPE,
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            TCOR.COD_ID COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(S.DATA_CONEXAO,'01/01/1900', '24/10/2010', S.DATA_CONEXAO) DAT_CON, 
            'PD' POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,DECODE(SUB.ID_TIPO_MODULO,1,SUB.CD_ALIMENTADOR,'0'),'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
            COALESCE(SUB.sub,'0') SUB, 
            sub.conj CONJ, 
            sub.municipio MUN, 
            DECODE(SUB.ARE_LOC, 'U', 'UB', 'R', 'NU', NULL) ARE_LOC,
            Sub.LOCAL_INST DESCR--, --NODE.SHAPE GEOMETRY
        FROM 
            (SELECT * FROM (SELECT A.*, RANK() OVER (PARTITION BY LOCAL_INST ORDER BY ID) R FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO A WHERE FEATURE_TYPE = 'LocalInstalacaoEqptoOperByPass') WHERE R != 1) SUB
            LEFT JOIN 
            (SELECT A2.EQUIPAMENTO,
            A2.STATUS_SE,
            A2.COR_NOM,
            A2.A1,
            A2.TUC,
            A2.A3,
            A2.A2,
            A2.BDGD,
            A2.ELO_FSV,
            A2.DATA_CONEXAO,
            A2.LOCAL_INSTALACAO
            FROM 
                (SELECT S1.*,
            ROW_NUMBER()
                    OVER (PARTITION BY LOCAL_INSTALACAO
                ORDER BY  DATA_CONEXAO DESC) ROW_NUMBER
                FROM MV_BDGD_SAP S1
                WHERE LOCAL_INSTALACAO LIKE 'SE%'
                        AND BDGD ='EQSE' ) A2
                WHERE ROW_NUMBER = 1
                GROUP BY  A2.LOCAL_INSTALACAO,A2.COR_NOM, A2.STATUS_SE, A2.A1, A2.TUC, A2.A3, A2.A2, A2.BDGD, A2.ELO_FSV, A2.DATA_CONEXAO, A2.EQUIPAMENTO ) S
                    ON S.LOCAL_INSTALACAO = SUB.LOCAL_INST
                /*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN CON
                    ON CON.PAC2 = S.LOCAL_INSTALACAO
                LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
                    ON NODE.LOC_INST_SAP = CON.PAC2*/                LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
                    ON TAB.CD_ALIMENTADOR = SUB.CD_ALIMENTADOR
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_ALIMENTADOR ALI
                    ON ALI.CD_ALIMENTADOR = TAB.CD_ALIMENTADOR
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
                    --ON SUB.NR_SUBESTACAO = TO_NUMBER(SUBSTR(NODE.LOC_INST_SAP, 4, 3))
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONJUNTO_ANEEL CONJ
                    --ON CONJ.PRI_CONJUNTO_CNS_ID = SUB.PRI_CONJUNTO_CNS_ID
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_MUNICIPIO_ANEEL MUN
                    --ON MUN.PRI_MUNICIPIO_ID = SUB.PRI_MUNICIPIO_ID
                LEFT JOIN (SELECT distinct substr(alimentador,1,3) as SUB, novo_alimentador,un_tr_s FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) NV ON NV.SUB = SUB.SUB
                --LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2
                    --ON NODE2.LOC_INST_SAP = NODE.UN_TR_S
                LEFT JOIN TCAPELFU
                    ON TCAPELFU.CAP = REPLACE(S.ELO_FSV, ',0', '')
                LEFT JOIN TCOR
                    ON TCOR.CORR = S.COR_NOM
                --WHERE S.BDGD = 'EQSE'
                        --AND S.A2 < 60
                GROUP BY  
                SUB.ID,
                SUB.LOCAL_INST,
                sub.nr_pto_elet_fim, 
                S.STATUS_SE, 
                S.A1, 
                S.TUC, 
                TCOR.COD_ID, 
                S.A3, 
                TCAPELFU.COD_ID, 
                S.DATA_CONEXAO, 
                sub.CD_ALIMENTADOR, 
                sub.local_inst_tt, 
                SUB.sub, 
                sub.conj, 
                sub.municipio, 
                SUB.ARE_LOC, 
                S.LOCAL_INSTALACAO, 
                NV.NOVO_ALIMENTADOR,
                NR_PTO_ELET_INICIO,
                sub.situacao_operacional,
                --NODE.UN_TR_S,
                --NODE2.OBJECTID, 
                NV.UN_TR_S,
                SUB.ID_TIPO_MODULO ) A
            LEFT JOIN (SELECT * FROM (SELECT A.*, RANK() OVER (PARTITION BY LOCAL_INST ORDER BY ID) R FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO A WHERE FEATURE_TYPE = 'LocalInstalacaoEqptoOperByPass') WHERE R = 1) G
            ON G.local_inst = A.DESCR) k
            left join tab_aux_pac_se_re p
                    on p.loc_inst = descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EP" ("COD_ID", "DIST", "SUB_GRP_PR", "SUB_GRP_SE", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND NEXT null
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT EP.COD_ID,
         EP.DIST,
         EP.SUB_GRP_PRI AS "SUB_GRP_PR",
         EP.SUB_GRP_SEC AS "SUB_GRP_SE",
         NVL(EP.ENE_01,
         0) AS "ENE_01",
         NVL(EP.ENE_02,
         0) AS "ENE_02",
         NVL(EP.ENE_03,
         0) AS "ENE_03",
         NVL(EP.ENE_04,
         0) AS "ENE_04",
         NVL(EP.ENE_05,
         0) AS "ENE_05",
         NVL(EP.ENE_06,
         0) AS "ENE_06",
         NVL(EP.ENE_07,
         0) AS "ENE_07",
         NVL(EP.ENE_08,
         0) AS "ENE_08",
         NVL(EP.ENE_09,
         0) AS "ENE_09",
         NVL(EP.ENE_10,
         0) AS "ENE_10",
         NVL(EP.ENE_11,
         0) AS "ENE_11",
         NVL(EP.ENE_12,
         0) AS "ENE_12",
         EP.DESCR
FROM BDGD_STAGING.TAB_PERTEC_EP EP
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UGBT" ("COD_ID", "PN_CON", "DIST", "PAC", "CEG", "UNI_TR_D", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CNAE", "FAS_CON", "GRU_TEN", "TEN_FORN", "SIT_ATIV", "DAT_CON", "POT_INST", "POT_CONT", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT SIGA.COD_ID,
       GEN.PN_CON,
       SIGA.DIST,
       'BT-'||GEN.PAC PAC,
       SIGA.CEG,
       GEN.UNI_TR_D UNI_TR_D,
       COALESCE(ALI.NOVO_ALIMENTADOR, GEN.CTMT) CTMT,
       --COALESCE(NV.UN_TR_S,  NVL2(TAB.ID_TT, 'SE-' || TAB.ID_TT, '0')) UNI_TR_S,
       COALESCE(ALI.UN_TR_S, NVL2(TAB.ID_TT, 'SE-' || TAB.ID_TT, '0')) UNI_TR_S,
       GEN.SUB,
       GEN.CONJ,
       SIGA.MUN,
       SIGA.LGRD,
       SIGA.BRR,
       SIGA.CEP,
       NVL(SIGA.CNAE, 0) CNAE,
       SIGA.FAS_CON,
       SIGA.GRU_TEN,
       SIGA.TEN_FORN,
       SIGA.SIT_ATIV,
       SIGA.DAT_CON,
       TO_NUMBER(SIGA.POT_INST) POT_INST,
       TO_NUMBER(SIGA.POT_CONT) POT_CONT,
       SIGA.ENE_01,
       SIGA.ENE_02,
       SIGA.ENE_03,
       SIGA.ENE_04,
       SIGA.ENE_05,
       SIGA.ENE_06,
       SIGA.ENE_07,
       SIGA.ENE_08,
       SIGA.ENE_09,
       SIGA.ENE_10,
       SIGA.ENE_11,
       SIGA.ENE_12,
       TO_NUMBER(REPLACE(DIC_FIC.DIC, ',', '.')) DIC,
       TO_NUMBER(DIC_FIC.FIC) FIC,
       GEN.DESCR,
       GEN.GEOMETRY
FROM MV_SIGA_UG_BT SIGA
     INNER JOIN BDGD_STAGING.MV_GENESIS_UCBT GEN
          ON SIGA.COD_ID = GEN.COD_ID
     LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
          ON GEN.CTMT = TAB.CD_ALIMENTADOR
     LEFT JOIN MV_TAB_UC_DIC_FIC DIC_FIC
          ON DIC_FIC.NR_CONTA_CNS = GEN.COD_ID
     LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ALI
        ON ALI.ALIMENTADOR = GEN.CTMT
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_RAMLIG" ("COD_ID", "PN_CON_1", "PN_CON_2", "DIST", "PAC_1", "PAC_2", "UNI_TR_D", "CTMT", "FAS_CON", "UNI_TR_S", "SUB", "CONJ", "ARE_LOC", "TIP_CND", "POS", "ODI_FAS", "TI_FAS", "SITCONTFAS", "ODI_NEU", "TI_NEU", "SITCONTNEU", "COMP", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT SIGA.COD_ID,
         GENESIS.PN_CON AS "PN_CON_1",
         DECODE(GENESIS.PN_CON, 'FORCENULL', GENESIS.PN_CON, NULL) AS "PN_CON_2",
         SIGA.DIST AS "DIST",
         COALESCE( DECODE(GENESIS.PAC1,'912892','912893','680280','680281','2675107','2675111','703810','703811',GENESIS.PAC1),'D1-' ||SIGA.COD_ID) AS "PAC_1",
         COALESCE(GENESIS.PAC2, 'D2-' ||SIGA.COD_ID) AS "PAC_2",
         COALESCE(GENESIS.TRANSFORMADOR,0) AS "UNI_TR_D",
         --DECODE(NV.NOVO_ALIMENTADOR, NULL, GENESIS.ALIMENTADOR, NV.NOVO_ALIMENTADOR) AS "CTMT",
         COALESCE(ALI.NOVO_ALIMENTADOR, GENESIS.ALIMENTADOR,'0')  CTMT,
         DECODE(SUGERE.FAS_CON,'ACN','CAN',SUGERE.FAS_CON) as FAS_CON,
       COALESCE(ALI.UN_TR_S, SUB_CONEC.LOC_INST_SAP_TT,'0') AS "UNI_TR_S",
     COALESCE(GENESIS.SUBESTACAO,'0') AS "SUB", 
    GENESIS.CONJUNTO AS "CONJ",
    CASE
    WHEN GENESIS.ARE_LOC = 'U' THEN
    'UB'
    WHEN GENESIS.ARE_LOC = 'R' THEN
    'NU'
    ELSE GENESIS.ARE_LOC
    END AS "ARE_LOC",
    --DECODE(SEGCON.COD_ID, NULL, DECODE(GENESIS.FASE, 1, '34172-1F', 2, '34172-1F', 3, '34172-1F', 4, '34175-2F', 5, '34175-2F', 6, '34175-2F', 7, '34178-3F', '34178-3F'), SEGCON.COD_ID) AS "TIP_CND",
    --SEGCON.COD_ID AS "TIP_CND",
    coalesce(TIP_CNS_AUX.TIP_CNS_NEW, DECODE(GENESIS.FASE, 'AN', '34172-1F', 'BN', '34172-1F', 'CN', '34172-1F', 'ABN', '34175-2F', 'ACN', '34175-2F', 'CAN', '34175-2F', 'BCN', '34175-2F', 'ABCN', '34178-3F', '34178-3F'), '-1') AS TIP_CND,
    'PD' AS POS,
    SIGA.ODI_FAS AS "ODI_FAS",
    SIGA.TI_FAS AS "TI_FAS",
    'AT1' AS "SITCONTFAS",
    SIGA.ODI_NEU AS "ODI_NEU",
    SIGA.TI_NEU AS "TI_NEU",
    'AT1' AS "SITCONTNEU",
    SIGA.COMP AS "COMP",
    --SIGA.COD_ID AS "DESCR"
    GENESIS.ID AS "DESCR"
FROM BDGD_STAGING.MV_SIGA_RAM_LIG SIGA
INNER JOIN (
SELECT 
DISTINCT ID,CONTA,FASE,PN_CON,PAC1,PAC2,RAMAL,TRANSFORMADOR,ALIMENTADOR,ARE_LOC,SUBESTACAO,CONJUNTO,MUNICIPIO
FROM BDGD_STAGING.MV_GENESIS_RAM_LIG 
 ) GENESIS ON GENESIS.CONTA = SIGA.UC
LEFT JOIN TAB_BDGD_SEGCON_RAMAL SEGCON ON SEGCON.COD_ID = SIGA.TIP_CND
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT SUB_CONEC  ON SUB_CONEC.CD_ALIMENTADOR = GENESIS.ALIMENTADOR
LEFT JOIN BDGD_STAGING.tab_ramlig_aux TIP_CNS_AUX     ON SUBSTR(TIP_CNS_AUX.TIP_CND_SIGA, 1,INSTR(TIP_CNS_AUX.TIP_CND_SIGA, '-')-1) = SUBSTR(SIGA.TIP_CND, 1,INSTR(SIGA.TIP_CND, '-')-1) AND DECODE(GENESIS.FASE,'AN',1,'BN',1,'CN',1,'ABN',2,'ACN',2,'CAN',2,'BCN',2,'ABCN',3) = TIP_CNS_AUX.QTD_FASE
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ALI ON ALI.ALIMENTADOR = GENESIS.ALIMENTADOR
INNER JOIN (SELECT 
 DISTINCT FEATURE_ID,
          FASE_SUGERIDA AS "FAS_CON"
 FROM BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT )SUGERE ON SUGERE.FEATURE_ID=GENESIS.ID
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_SSDMT" ("COD_ID", "PN_CON_1", "PN_CON_2", "CTMT", "UNI_TR_S", "SUB", "CONJ", "ARE_LOC", "DIST", "PAC_1", "PAC_2", "FAS_CON", "TIP_CND", "POS", "ODI_FAS", "TI_FAS", "SITCONTFAS", "ODI_NEU", "TI_NEU", "SITCONTNEU", "COMP", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
SELECT 
GEN.COD_ID,
GEN.PN_CON_1,
GEN.PN_CON_2,
COALESCE(ALI.NOVO_ALIMENTADOR, GEN.CTMT,'0') CTMT,
COALESCE(ALI.UN_TR_S, TRA.LOC_INST_SAP_TT,'0') UNI_TR_S,
GEN.SUB,
GEN.CONJ,
GEN.ARE_LOC,
GEN.DIST,
CASE
    WHEN COALESCE(PTO.PAC_1_NOVO, TO_CHAR(FLY.PAC_1), TO_CHAR(GEN.PAC_1))='0' THEN 'D1-'||GEN.COD_ID
    WHEN TO_CHAR(GEN.PAC_1) = '225912' THEN 'SE-SE-6057518'
    WHEN TO_CHAR(GEN.PAC_1) = '299262' THEN 'SE-SE-6057725'
    WHEN TO_CHAR(GEN.PAC_1) = '1547154' THEN 'SE-SE-6029071'
ELSE 
    COALESCE(PTO.PAC_1_NOVO, TO_CHAR(FLY.PAC_1), TO_CHAR(GEN.PAC_1))
END AS PAC_1,
CASE
    WHEN COALESCE(C_MT_2.PAC_NOVO, ''||GEN.PAC_2)='0' THEN 'D2-'||GEN.COD_ID
ELSE 
    COALESCE(C_MT_2.PAC_NOVO, ''||GEN.PAC_2)
END AS PAC_2,
GEN.FAS_CON,
GEN.TIP_CND,
GEN.POS,
GEN.ODI_FAS,
GEN.TI_FAS,
GEN.SITCONTFAS,
GEN.ODI_NEU,
GEN.TI_NEU,
GEN.SITCONTNEU,
GEN.COMP,
GEN.DESCR,
GEN.GEOMETRY
FROM BDGD_STAGING.MV_GENESIS_SSDMT GEN
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TRA                     ON TRA.CD_ALIMENTADOR = GEN.CTMT
LEFT JOIN gen_bdgd2019.V_GENESIS_CONECTIVIDADE_SE_MT_S PTO    ON PTO.PAC_JOIN = GEN.PAC_1
LEFT JOIN gen_bdgd2019.V_GENESIS_CONECTIVIDADE_SE_MT_E C_MT_2 ON C_MT_2.PAC_2_SSDMT = GEN.PAC_2
LEFT JOIN BDGD_STAGING.MV_GENESIS_FLY_TAP_MT FLY              ON GEN.PAC_1 = FLY.PAC_2
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ALI               ON ALI.ALIMENTADOR = GEN.CTMT)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQRE_TEMP" ("COD_ID", "DIST", "PAC_1", "PAC_2", "POT_NOM", "TIP_REGU", "TEN_REG", "LIG_FAS_P", "LIG_FAS_S", "COR_NOM", "REL_TP", "REL_TC", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "PER_FER", "PER_TOT", "R", "XHL", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
eqre.cod_id,
eqre.dist,
coalesce(ajust.pac_1,eqre.pac_1) as pac_1,
coalesce(ajust.pac_2,eqre.pac_2) as pac_2,
eqre.pot_nom,
eqre.tip_regu,
eqre.ten_reg,
eqre.lig_fas_p,
eqre.lig_fas_s,
eqre.cor_nom,
eqre.rel_tp,
eqre.rel_tc,
eqre.odi,
eqre.ti,
eqre.cm,
eqre.tuc,
eqre.a1,
eqre.a2,
eqre.a3,
eqre.a4,
eqre.a5,
eqre.a6,
eqre.iduc,
eqre.sitcont,
eqre.dat_imo,
eqre.per_fer,
eqre.per_tot,
eqre.r,
eqre.xhl,
eqre.ene_01,
eqre.ene_02,
eqre.ene_03,
eqre.ene_04,
eqre.ene_05,
eqre.ene_06,
eqre.ene_07,
eqre.ene_08,
eqre.ene_09,
eqre.ene_10,
eqre.ene_11,
eqre.ene_12,
eqre.descr
from mv_bdgd_eqre eqre left join
(
select distinct local_inst,eqpto.sub,ctmt,ten_nom,uni_tr_s,'D-'||LOCAL_INST as pac_1,pac as pac_2 from
(select * from 
GEN_BDGD2019.mv_eqptos_subestacao 
where id_tipo_modulo != 1 and 
id_tn_nominal_eqpto < 6 and 
tipo_eqpto in ('RG')) EQPTO 
inner join (select sub,
cod_id as ctmt,
pac,
uni_tr_s,
decode(ten_nom,72,5,62,4,49,3,NULL) as ten_nom
from mv_bdgd_ctmt where cod_id in (
select min(COD_ID) from mv_bdgd_ctmt group by sub,ten_nom)) ctmt on ctmt.sub = eqpto.sub and ctmt.ten_nom = eqpto.id_tn_nominal_eqpto ) ajust 
on ajust.local_inst = eqre.descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQTRS" ("COD_ID", "DIST", "PAC_1", "PAC_2", "PAC_3", "CLAS_TEN", "POT_NOM", "LIG", "POS", "FLX_INV", "FAS_CON", "TEN_PRI", "TEN_SEC", "TEN_TER", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "PER_FER", "PER_TOT", "POT_F01", "POT_F02", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT S.EQUIPAMENTO AS "COD_ID",
         '5697' AS "DIST", 
         DECODE(SUB.NR_PTO_ELET_INICIO, NULL, 'D1-' || S.EQUIPAMENTO, 'SE-'||SUB.NR_PTO_ELET_INICIO) AS PAC_1,
         DECODE(SUB.NR_PTO_ELET_FIM, NULL, 'D2-' || S.EQUIPAMENTO, 'SE-'||SUB.NR_PTO_ELET_FIM) AS PAC_2,
         'D3-' || S.EQUIPAMENTO PAC_3, 
         TCLATEN.COD_ID AS "CLAS_TEN", 
CASE
WHEN A4 = 61 THEN '40'
WHEN A4 = 63 THEN '43'
WHEN A4 = 65 THEN '45'
WHEN A4 = 70 THEN '50'
WHEN A4 = 72 THEN '54'
WHEN A4 = 73 THEN '58'
WHEN A4 = 75 THEN '61'
WHEN A4 = 75 THEN '62'
WHEN A4 = 77 THEN '65'
WHEN A4 = 78 THEN '65'
WHEN A4 = 80 THEN '68'
WHEN A4 = 82 THEN '74'
WHEN A4 = 83 THEN '76'
WHEN A4 = 85 THEN '78'
WHEN A4 = 86 THEN '80'
WHEN A4 = 87 THEN '82'
WHEN A4 = 88 THEN '83'
WHEN A4 = 89 THEN '85'
WHEN A4 = 90 THEN '85'
WHEN A4 = 91 THEN '88'
WHEN A4 = 92 THEN '91'
WHEN A4 = 95 THEN '95'
ELSE '0' END AS "POT_NOM",         
         TLIG.COD_ID AS "LIG", 
         'PD' AS "POS", 
         0 AS "FLX_INV", 
         'ABC' AS "FAS_CON", 
         T1.COD_ID AS "TEN_PRI", 
         T2.COD_ID AS "TEN_SEC", 
         NVL(T3.COD_ID, '0') AS "TEN_TER", 
         S.ODI AS "ODI", 
         S.TI AS "TI", 
         S.CENTRO_MODULAR AS "CM", 
         S.TUC AS "TUC", 
         nvl(S.A1,'00') AS "A1", 
         nvl(S.A2, '00') AS "A2", 
         nvl(S.A3, '00') AS "A3", 
         nvl(S.A4, '00') AS "A4", 
         nvl(S.A5, '00') AS "A5", 
         nvl(S.A6, '00') AS "A6", 
         S.IDUC AS "IDUC", 
         DECODE(S.DATA_IMOB, NULL, 'NIM', 'AT1') AS "SITCONT", 
         S.DATA_IMOB AS "DAT_IMO", 
         TRUNC((replace(S.PER_FER, ',', '.')/replace(S.POT_NOM, ',', '.'))/10,3) AS "PER_FER", 
         TRUNC((replace(S.PER_TOT, ',', '.')/replace(S.POT_NOM, ',', '.'))/10,3) AS "PER_TOT", 
         REPLACE(S.POT_F01, ',', '.')/1000 AS "POT_F01", REPLACE(S.POT_F02, ',', '.')/1000 AS "POT_F02", 
         s.local_instalacao AS "DESCR"
FROM MV_BDGD_SAP S
LEFT JOIN TCLATEN
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TPOTAPRT
    ON TPOTAPRT.POT = S.POT_NOM
LEFT JOIN TLIG
    ON TLIG.DESCR = S.LIG
LEFT JOIN TTEN T1
    ON T1.TEN = S.TEN_PRI
LEFT JOIN TTEN T2
    ON T2.TEN = S.TEN_SEC
LEFT JOIN TTEN T3
    ON T3.TEN = S.TEN_TER
LEFT JOIN GEN_BDGD2019.MV_EQPTOS_SUBESTACAO SUB
ON S.LOCAL_INSTALACAO = SUB.LOCAL_INST
    WHERE s.bdgd = 'EQTRS'
        AND S.LOCAL_INSTALACAO LIKE 'SE-0%'
        AND TCLATEN.COD_ID >= 11
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_CTMT" ("COD_ID", "NOM", "BARR", "SUB", "PAC", "TEN_NOM", "TEN_OPE", "ATIP", "RECONFIG", "DIST", "UNI_TR_S", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "PERD_A3A", "PERD_A4", "PERD_B", "PERD_MED", "PERD_A3A_A4", "PERD_A3A_B", "PERD_A4_A3A", "PERD_A4_B", "PERD_B_A3A", "PERD_B_A4", "PNTMT_01", "PNTMT_02", "PNTMT_03", "PNTMT_04", "PNTMT_05", "PNTMT_06", "PNTMT_07", "PNTMT_08", "PNTMT_09", "PNTMT_10", "PNTMT_11", "PNTMT_12", "PNTBT_01", "PNTBT_02", "PNTBT_03", "PNTBT_04", "PNTBT_05", "PNTBT_06", "PNTBT_07", "PNTBT_08", "PNTBT_09", "PNTBT_10", "PNTBT_11", "PNTBT_12", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT CTMT.COD_ID COD_ID,
         CTMT.NOM,
         DECODE(TAB.LOC_INST_SAP_BAR_1,  NULL,'0', '0', '0', TAB.LOC_INST_SAP_BAR_1) BARR,
         CTMT.SUB,
         DECODE(PAC.FILHO_ALIMENTADOR, NULL, CTMT.COD_ID || '-12345',PAC.FILHO_ALIMENTADOR) PAC,
         CTMT.TEN_NOM, '1' TEN_OPE, 
         STGPRD.ATIP AS ATIP,
         --DECODE(0, NULL, 1,'0',1, 0) ATIP,
         STGPRD.RECONFIG AS RECONFIG,
         CTMT.DIST,
         DECODE(TAB.LOC_INST_SAP_TT, NULL, '0', '0', '0', TAB.LOC_INST_SAP_TT) UNI_TR_S,
         COALESCE(STGPRD.ENE_01,0)as ENE_01,
         COALESCE(STGPRD.ENE_02,0)as ENE_02, 
         COALESCE(STGPRD.ENE_03,0)as ENE_03,
         COALESCE(STGPRD.ENE_04,0)as ENE_04,
         COALESCE(STGPRD.ENE_05,0)as ENE_05,
         COALESCE(STGPRD.ENE_06,0)as ENE_06,
         COALESCE(STGPRD.ENE_07,0)as ENE_07,
         COALESCE(STGPRD.ENE_08,0)as ENE_08,
         COALESCE(STGPRD.ENE_09,0)as ENE_09,
         COALESCE(STGPRD.ENE_10,0)as ENE_10,
         COALESCE(STGPRD.ENE_11,0)as ENE_11,
         COALESCE(STGPRD.ENE_12,0)as ENE_12,   
          COALESCE(PERD_A3A,0)as PERD_A3A,
          COALESCE(PERD_A4,0)as PERD_A4,
          COALESCE(PERD_B,0)as PERD_B,
          COALESCE(PERD_MED,0)as PERD_MED,
          COALESCE(PERD_A3A_A4,0)as PERD_A3A_A4,
          COALESCE(PERD_A3A_B,0)as PERD_A3A_B,
          COALESCE(PERD_A4_A3A,0)as PERD_A4_A3A,
          COALESCE(PERD_A4_B,0) as PERD_A4_B,
          COALESCE(PERD_B_A3A,0)as PERD_B_A3A,
          COALESCE(PERD_B_A4,0)as PERD_B_A4,
          COALESCE(PNTMT_01,0)as PNTMT_01,
          COALESCE(PNTMT_02,0)as PNTMT_02,
          COALESCE(PNTMT_03,0)as PNTMT_03,
          COALESCE(PNTMT_04,0)as PNTMT_04,
          COALESCE(PNTMT_05,0)as PNTMT_05,
          COALESCE(PNTMT_06,0)as PNTMT_06,
          COALESCE(PNTMT_07,0)as PNTMT_07,
          COALESCE(PNTMT_08,0)as PNTMT_08,
          COALESCE(PNTMT_09,0)as PNTMT_09,
          COALESCE(PNTMT_10,0)as PNTMT_10,
          COALESCE(PNTMT_11,0)as PNTMT_11,
          COALESCE(PNTMT_12,0)as PNTMT_12,
          COALESCE(PNTBT_01,0)as PNTBT_01,
          COALESCE(PNTBT_02,0)as PNTBT_02,
          COALESCE(PNTBT_03,0)as PNTBT_03,
          COALESCE(PNTBT_04,0)as PNTBT_04,
          COALESCE(PNTBT_05,0)as PNTBT_05,
          COALESCE(PNTBT_06,0)as PNTBT_06,
          COALESCE(PNTBT_07,0)as PNTBT_07,
          COALESCE(PNTBT_08,0)as PNTBT_08,
          COALESCE(PNTBT_09,0)as PNTBT_09,
          COALESCE(PNTBT_10,0)as PNTBT_10,
          COALESCE(PNTBT_11,0)as PNTBT_11,
          COALESCE(PNTBT_12,0)as PNTBT_12,
         CTMT.PAC DESCR
FROM BDGD_STAGING.MV_GENESIS_CTMT CTMT
LEFT JOIN BDGD_STAGING.TAB_PERTEC_CTMT STGPRD ON STGPRD.COD_ID= CTMT.COD_ID 
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB ON CTMT.COD_ID = TAB.CD_ALIMENTADOR
LEFT JOIN VA_AUX_ALI_CTMT PAC ON PAC.CD_ALIMENTADOR = CTMT.COD_ID
WHERE CTMT.COD_ID NOT IN (SELECT ALIMENTADOR FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NOVO)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQME_TEMP" ("COD_ID", "PAC", "DIST", "TIP_UNID", "FAS_CON", "TIPMED", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "SITCONT", "DAT_IMO", "DESCR", "R")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select distinct a.* from 
(select mv.*, rank() over (PARTITION BY cod_id order by fas_con, dat_imo, ti) r from mv_bdgd_eqme mv) a
where r = 1
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQTRSX" ("COD_ID", "SUB", "DIST", "TIP_UNID", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT SAP.EQUIPAMENTO COD_ID,
       SUB.SG_SUBESTACAO SUB,
       '5697' DIST,
       40 TIP_UNID,
       SAP.ODI,
       SAP.TI,
       SAP.CENTRO_MODULAR CM,
       SAP.TUC,
       SAP.A1,
       SAP.A2,
       SAP.A3,
       SAP.A4,
       SAP.A5,
       '00' as A6,
       SAP.IDUC,
       DECODE(SAP.DATA_IMOB, NULL, 'NIM', 'AT1') AS SITCONT,
       SAP.DATA_IMOB AS DAT_IMO,
       SAP.SUBESTACAO DESCR
FROM MV_BDGD_SAP SAP
     LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
          ON SUB.NR_SUBESTACAO = SUBSTR(SAP.SUBESTACAO, 4)
WHERE SAP.BDGD = 'EQTRSX' AND SUB.SG_SUBESTACAO IS NOT NULL
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_RAMLIG_L" ("COD_ID", "PN_CON_1", "PN_CON_2", "DIST", "PAC_1", "PAC_2", "UNI_TR_D", "CTMT", "FAS_CON", "UNI_TR_S", "SUB", "CONJ", "ARE_LOC", "TIP_CND", "POS", "ODI_FAS", "TI_FAS", "SITCONTFAS", "ODI_NEU", "TI_NEU", "SITCONTNEU", "COMP", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT SIGA.COD_ID,
         GENESIS.PN_CON AS "PN_CON_1",
         DECODE(GENESIS.PN_CON, 'FORCENULL', GENESIS.PN_CON, NULL) AS "PN_CON_2",
         SIGA.DIST AS "DIST",
         COALESCE( DECODE(GENESIS.PAC1,'912892','912893','680280','680281','2675107','2675111','703810','703811',GENESIS.PAC1),'D1-' ||SIGA.COD_ID) AS "PAC_1",
         COALESCE(GENESIS.PAC2, 'D2-' ||SIGA.COD_ID) AS "PAC_2",
         COALESCE(GENESIS.TRANSFORMADOR,0) AS "UNI_TR_D",
         --DECODE(NV.NOVO_ALIMENTADOR, NULL, GENESIS.ALIMENTADOR, NV.NOVO_ALIMENTADOR) AS "CTMT",
         COALESCE(ALI.NOVO_ALIMENTADOR, GENESIS.ALIMENTADOR,'0')  CTMT,
         DECODE(SUGERE.FAS_CON,'ACN','CAN',SUGERE.FAS_CON) as FAS_CON,
       COALESCE(ALI.UN_TR_S, SUB_CONEC.LOC_INST_SAP_TT,'0') AS "UNI_TR_S",
     COALESCE(GENESIS.SUBESTACAO,'0') AS "SUB", 
    GENESIS.CONJUNTO AS "CONJ",
    CASE
    WHEN GENESIS.ARE_LOC = 'U' THEN
    'UB'
    WHEN GENESIS.ARE_LOC = 'R' THEN
    'NU'
    ELSE GENESIS.ARE_LOC
    END AS "ARE_LOC",
    --DECODE(SEGCON.COD_ID, NULL, DECODE(GENESIS.FASE, 1, '34172-1F', 2, '34172-1F', 3, '34172-1F', 4, '34175-2F', 5, '34175-2F', 6, '34175-2F', 7, '34178-3F', '34178-3F'), SEGCON.COD_ID) AS "TIP_CND",
    --SEGCON.COD_ID AS "TIP_CND",
    coalesce(TIP_CNS_AUX.TIP_CNS_NEW, DECODE(GENESIS.FASE, 'AN', '34172-1F', 'BN', '34172-1F', 'CN', '34172-1F', 'ABN', '34175-2F', 'ACN', '34175-2F', 'CAN', '34175-2F', 'BCN', '34175-2F', 'ABCN', '34178-3F', '34178-3F'), '-1') AS TIP_CND,
    'PD' AS POS,
    SIGA.ODI_FAS AS "ODI_FAS",
    SIGA.TI_FAS AS "TI_FAS",
    'AT1' AS "SITCONTFAS",
    SIGA.ODI_NEU AS "ODI_NEU",
    SIGA.TI_NEU AS "TI_NEU",
    'AT1' AS "SITCONTNEU",
    SIGA.COMP AS "COMP",
    --SIGA.COD_ID AS "DESCR"
    GENESIS.ID AS "DESCR"
FROM BDGD_STAGING.MV_SIGA_RAM_LIG SIGA
INNER JOIN (
SELECT 
DISTINCT ID,CONTA,FASE,PN_CON,PAC1,PAC2,RAMAL,TRANSFORMADOR,ALIMENTADOR,ARE_LOC,SUBESTACAO,CONJUNTO,MUNICIPIO
FROM BDGD_STAGING.MV_GENESIS_RAM_LIG 
 ) GENESIS ON GENESIS.CONTA = SIGA.UC
LEFT JOIN TAB_BDGD_SEGCON_RAMAL SEGCON ON SEGCON.COD_ID = SIGA.TIP_CND
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT SUB_CONEC  ON SUB_CONEC.CD_ALIMENTADOR = GENESIS.ALIMENTADOR
LEFT JOIN BDGD_STAGING.tab_ramlig_aux TIP_CNS_AUX     ON SUBSTR(TIP_CNS_AUX.TIP_CND_SIGA, 1,INSTR(TIP_CNS_AUX.TIP_CND_SIGA, '-')-1) = SUBSTR(SIGA.TIP_CND, 1,INSTR(SIGA.TIP_CND, '-')-1) AND DECODE(GENESIS.FASE,'AN',1,'BN',1,'CN',1,'ABN',2,'ACN',2,'CAN',2,'BCN',2,'ABCN',3) = TIP_CNS_AUX.QTD_FASE
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ALI ON ALI.ALIMENTADOR = GENESIS.ALIMENTADOR
left JOIN (SELECT 
 DISTINCT FEATURE_ID,
          FASE_SUGERIDA AS "FAS_CON"
 FROM BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT )SUGERE ON SUGERE.FEATURE_ID=GENESIS.ID
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNTRS" ("COD_ID", "SUB", "BARR_1", "BARR_2", "BARR_3", "PAC_1", "PAC_2", "PAC_3", "DIST", "FAS_CON_P", "FAS_CON_S", "FAS_CON_T", "SIT_ATIV", "TIP_UNID", "POS", "ARE_LOC", "POT_NOM", "POT_F01", "POT_F02", "PER_FER", "TESTE", "PER_TOT", "BANC", "DAT_CON", "CONJ", "MUN", "TIP_TRAFO", "ALOC_PERD", "ENES_01", "ENES_02", "ENES_03", "ENES_04", "ENES_05", "ENES_06", "ENES_07", "ENES_08", "ENES_09", "ENES_10", "ENES_11", "ENES_12", "ENET_01", "ENET_02", "ENET_03", "ENET_04", "ENET_05", "ENET_06", "ENET_07", "ENET_08", "ENET_09", "ENET_10", "ENET_11", "ENET_12", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT sub.*, g.centroid_geo GEOMETRY
 FROM
(
     SELECT DECODE(SUB.ID, NULL, 'SE-'||SAP.EQUIPAMENTO, SUB.LOCAL_INST) COD_ID,
            COALESCE(SUB.SUB, SUB_2.SUB) AS SUB,
            DECODE(TT_BAR.LOC_INST_SAP_BAR_1, '0', '0',NULL, '0', TT_BAR.LOC_INST_SAP_BAR_1) AS "BARR_1",
            DECODE(TT_BAR.LOC_INST_SAP_BAR_2, '0', '0',NULL, '0', TT_BAR.LOC_INST_SAP_BAR_2) AS "BARR_2",
            DECODE(TT_BAR.LOC_INST_SAP_BAR_3, '0', '0',NULL, '0', TT_BAR.LOC_INST_SAP_BAR_3) AS "BARR_3",
            DECODE(SUB.NR_PTO_ELET_INICIO, NULL, 'D1-' || SAP.EQUIPAMENTO, 'SE-'||SUB.NR_PTO_ELET_INICIO) AS PAC_1,
            DECODE(SUB.NR_PTO_ELET_FIM, NULL, 'D2-' || SAP.EQUIPAMENTO, 'SE-'||SUB.NR_PTO_ELET_FIM) AS PAC_2,
            CASE
                WHEN SUB.LOCAL_INST = 'SE-0019961' THEN 'SE-SE-6047798'
                WHEN SUB.LOCAL_INST = 'SE-0020449' THEN 'SE-SE-6049197'
                WHEN SUB.LOCAL_INST = 'SE-0020450' THEN 'SE-SE-6049199'
                WHEN SUB.LOCAL_INST = 'SE-0020451' THEN 'SE-SE-6049202'
                ELSE 'D3-' || SAP.EQUIPAMENTO 
            END  AS PAC_3,
            '5697' AS "DIST",
            'ABC' AS "FAS_CON_P",
            'ABC' AS "FAS_CON_S",
            '0' AS "FAS_CON_T",
            TSITATI.COD_ID AS "SIT_ATIV",
            '41' AS "TIP_UNID",
            'PD' AS "POS",
            TARE.COD_ID AS "ARE_LOC",
            to_number(SAP.POT_NOM)/1000 AS "POT_NOM",
            SAP.POT_F01 AS "POT_F01",
            SAP.POT_F02 AS "POT_F02",
            
            TRUNC (
                      to_number( coalesce(REPLACE(SAP.PER_FER,',','.'),'0'))/
                CASE 
                  WHEN SAP.POT_NOM is null THEN 1
                  WHEN to_number(SAP.POT_NOM)=0 THEN 1
                  ELSE to_number(SAP.POT_NOM)END / 10,3 ) as PER_FER,
                  SAP.PER_TOT as teste,
            TRUNC (
                      to_number( coalesce(replace(SAP.PER_TOT, ',', '.'),'0'))/
              
                      CASE 
                         WHEN SAP.POT_NOM is null THEN 1
                         WHEN to_number(SAP.POT_NOM)=0 THEN 1
                         ELSE to_number(SAP.POT_NOM) 
                         END/ 10
                         ,3 ) as PER_TOT,
            0 AS "BANC",
            SAP.DATA_CONEXAO AS "DAT_CON",
            COALESCE(SUB.CONJ, SUB_2.CONJ) AS "CONJ",
            COALESCE(SUB.MUNICIPIO, SUB_2.MUNICIPIO) AS "MUN",
            'T' AS "TIP_TRAFO",
            TTEN_PERDA.COD_ID AS "ALOC_PERD",
            NVL(ENERGIA.ENES_01, 0) AS "ENES_01",
            NVL(ENERGIA.ENES_02, 0) AS "ENES_02",
            NVL(ENERGIA.ENES_03, 0) AS "ENES_03",
            NVL(ENERGIA.ENES_04, 0) AS "ENES_04",
            NVL(ENERGIA.ENES_05, 0) AS "ENES_05",
            NVL(ENERGIA.ENES_06, 0) AS "ENES_06",
            NVL(ENERGIA.ENES_07, 0) AS "ENES_07",
            NVL(ENERGIA.ENES_08, 0) AS "ENES_08",
            NVL(ENERGIA.ENES_09, 0) AS "ENES_09",
            NVL(ENERGIA.ENES_10, 0) AS "ENES_10",
            NVL(ENERGIA.ENES_11, 0) AS "ENES_11",
            NVL(ENERGIA.ENES_12, 0) AS "ENES_12",
            NVL(ENERGIA.ENET_01, 0) AS "ENET_01",
            NVL(ENERGIA.ENET_02, 0) AS "ENET_02",
            NVL(ENERGIA.ENET_03, 0) AS "ENET_03",
            NVL(ENERGIA.ENET_04, 0) AS "ENET_04",
            NVL(ENERGIA.ENET_05, 0) AS "ENET_05",
            NVL(ENERGIA.ENET_06, 0) AS "ENET_06",
            NVL(ENERGIA.ENET_07, 0) AS "ENET_07",
            NVL(ENERGIA.ENET_08, 0) AS "ENET_08",
            NVL(ENERGIA.ENET_09, 0) AS "ENET_09",
            NVL(ENERGIA.ENET_10, 0) AS "ENET_10",
            NVL(ENERGIA.ENET_11, 0) AS "ENET_11",
            NVL(ENERGIA.ENET_12, 0) AS "ENET_12",
            DECODE(SUB.ID, NULL,'Reserva Técnica' ,SAP.LOCAL_INSTALACAO)AS "DESCR"
          FROM (SELECT * FROM MV_BDGD_SAP WHERE BDGD = 'EQTRS' AND LOCAL_INSTALACAO LIKE 'SE-0%') SAP
          LEFT JOIN GEN_BDGD2019.MV_EQPTOS_SUBESTACAO SUB ON SAP.LOCAL_INSTALACAO = SUB.LOCAL_INST
          LEFT JOIN BDGD_STAGING.TAB_TT_BAR TT_BAR ON TT_BAR.LOC_INST_SAP_TT = SAP.LOCAL_INSTALACAO
          LEFT JOIN BDGD_STAGING.TAB_ENERGIA_UNTRS_2 ENERGIA ON ENERGIA.LOC_INSTALACAO = SAP.LOCAL_INSTALACAO
          LEFT JOIN TTEN_PERDA ON TTEN_PERDA.TEN = SAP.TEN_PRI
          LEFT JOIN TSITATI ON TSITATI.SIT_OPER = SAP.SIT_OPER
          LEFT JOIN tclaten ON trim(upper(SAP.CLAS_TEN)) = trim(upper(tclaten.TEN))
          LEFT JOIN (
                      SELECT ID_LOCAL_INST_SUBESTACAO, CONJ, MUNICIPIO, ARE_LOC, SUB 
                      FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO 
                      GROUP BY ID_LOCAL_INST_SUBESTACAO, CONJ, MUNICIPIO, ARE_LOC, SUB
                    ) SUB_2 ON SUB_2.ID_LOCAL_INST_SUBESTACAO = SUBESTACAO
          LEFT JOIN TARE ON TARE.ARE_LOC = COALESCE(SUB.ARE_LOC, SUB_2.ARE_LOC)
WHERE tclaten.cod_id >= 11
) SUB
LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO g on sub.sub = g.sg_subestacao
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_PT" ("COD_ID", "DIST", "CATEG", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND NEXT null
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
PT.COD_ID,
PT.DIST,
PT.CATEG,
NVL(PT.ENE_01, 0) AS "ENE_01",
NVL(PT.ENE_02, 0) AS "ENE_02",
NVL(PT.ENE_03, 0) AS "ENE_03",
NVL(PT.ENE_04, 0) AS "ENE_04",
NVL(PT.ENE_05, 0) AS "ENE_05",
NVL(PT.ENE_06, 0) AS "ENE_06",
NVL(PT.ENE_07, 0) AS "ENE_07",
NVL(PT.ENE_08, 0) AS "ENE_08",
NVL(PT.ENE_09, 0) AS "ENE_09",
NVL(PT.ENE_10, 0) AS "ENE_10",
NVL(PT.ENE_11, 0) AS "ENE_11",
NVL(PT.ENE_12, 0) AS "ENE_12",
PT.DESCR
FROM BDGD_STAGING.TAB_PERTEC_PT PT
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQCR_TEMP" ("COD_ID", "SUB", "DIST", "PAC_1", "PAC_2", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
eqcr.cod_id,
coalesce(ajust.sub,eqcr.sub) as sub,
eqcr.dist,
coalesce(ajust.pac_1,eqcr.pac_1) as pac_1,
coalesce(ajust.pac_2,eqcr.pac_2) as pac_2,
eqcr.odi,
eqcr.ti,
eqcr.cm,
eqcr.tuc,
eqcr.a1,
eqcr.a2,
eqcr.a3,
eqcr.a4,
eqcr.a5,
eqcr.a6,
eqcr.iduc,
eqcr.sitcont,
eqcr.dat_imo,
eqcr.descr
from mv_bdgd_eqcr eqcr left join 
(
select distinct local_inst,eqpto.sub,ctmt,ten_nom,uni_tr_s,'D-'||LOCAL_INST as pac_1,pac as pac_2 from
(select * from 
GEN_BDGD2019.mv_eqptos_subestacao 
where id_tipo_modulo != 1 and 
id_tn_nominal_eqpto < 6 and 
tipo_eqpto in ('BC')) EQPTO 
inner join (select sub,
cod_id as ctmt,
pac,
uni_tr_s,
decode(ten_nom,72,5,62,4,49,3,NULL) as ten_nom
from mv_bdgd_ctmt where cod_id in (
select min(COD_ID) from mv_bdgd_ctmt group by sub,ten_nom)) ctmt on ctmt.sub = eqpto.sub and ctmt.ten_nom = eqpto.id_tn_nominal_eqpto ) ajust 
on ajust.local_inst = eqcr.descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNSEAT" ("COD_ID", "DIST", "PAC_1", "PAC_2", "FAS_CON", "SIT_ATIV", "TIP_UNID", "P_N_OPE", "CAP_ELO", "COR_NOM", "TLCD", "DAT_CON", "POS", "SUB", "CONJ", "MUN", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT A.*,
       G.sub_geometry GEOMETRY
FROM
(SELECT '' || sub.LOCAL_INST COD_ID,
       '5697' DIST,
       'SE-' || SUB.NR_PTO_ELET_INICIO PAC_1,
       'SE-' || SUB.NR_PTO_ELET_FIM PAC_2,
       'ABC' FAS_CON,
       'AT' SIT_ATIV,
       DECODE(S.TUC, 160, DECODE(S.A1, 6, 34, 3, 22, 1, DECODE(S.A4, 3, 33, 1, 34), 2, DECODE(S.A4, 3, 33, 1, 34)), 210, 29, 540, 16) TIP_UNID,
       'F' P_N_OPE,
       NVL(S.ELO_FSV, '0') CAP_ELO,
       TCOR.COD_ID AS "COR_NOM",
       DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD,
       S.DATA_CONEXAO DAT_CON,
       'PD' POS,
       SUB.SUB  SUB,
       SUB.CONJ CONJ,
       SUB.MUNICIPIO MUN,
       S.LOCAL_INSTALACAO DESCR
FROM (
      SELECT S.LOCAL_INSTALACAO,MAX(S.COR_NOM) COR_NOM, MAX(S.STATUS_SE) STATUS_SE, MAX(S.ELO_FSV) ELO_FSV, MAX(S.A1) A1, MAX(S.A4) A4, MAX(S.DATA_CONEXAO) DATA_CONEXAO, S.TUC,S.SUBESTACAO
      FROM MV_BDGD_SAP S WHERE S.BDGD = 'EQSE' AND S.A2 >= 60 GROUP BY S.LOCAL_INSTALACAO, S.TUC,S.SUBESTACAO) S
      RIGHT JOIN (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto in ('CBY','CD','CF','CN','CO','CQ','CT','CU','CUA','DJ','FU','RL') AND id_tn_nominal_eqpto >= '6') SUB
           ON SUB.LOCAL_INST = S.LOCAL_INSTALACAO
     /*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN CON
          ON CON.PAC2 = S.LOCAL_INSTALACAO
     LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
          ON NODE.LOC_INST_SAP = CON.PAC2
     LEFT JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB SUB
          ON 'SE-'||NR_SUB=S.SUBESTACAO*/
          LEFT JOIN TCOR
          ON TCOR.CORR = S.COR_NOM
GROUP BY
        sub.LOCAL_INST,
       'SE-' || sub.NR_PTO_ELET_FIM,
       'SE-' || NR_PTO_ELET_INICIO,
       '5697',
       'SE-' || NR_PTO_ELET_FIM,
       DECODE(S.TUC, 160, DECODE(S.A1, 6, 34, 3, 22, 1, DECODE(S.A4, 3, 33, 1, 34), 2, DECODE(S.A4, 3, 33, 1, 34)), 210, 29, 540, 16),
       NVL(S.ELO_FSV, '0'),
       TCOR.COD_ID,
       DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0'),
       S.DATA_CONEXAO,
       SUB.SUB,
       SUB.CONJ,
       SUB.MUNICIPIO,
       S.LOCAL_INSTALACAO
       ) A
INNER JOIN GEN_BDGD2019.MV_EQPTOS_SUBESTACAO G
     ON G.local_inst = A.cod_id
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNREAT" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "PAC_1", "PAC_2", "SUB", "CONJ", "MUN", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
''||SUB.LOCAL_INST as COD_ID,
5697 as DIST,
'ABCN'  as FAS_CON,
'AT'  as SIT_ATIV,
14 as TIP_UNID,
'SE-'||SUB.NR_PTO_ELET_INICIO as PAC_1,
'SE-'||SUB.NR_PTO_ELET_FIM as PAC_2,
SUB.SUB,
SUB.CONJ,
SUB.MUNICIPIO as MUN,
CASE 
  WHEN SAP.DATA_CONEXAO='00/00/0000' THEN '01/01/0001' 
  ELSE SAP.DATA_CONEXAO
  END AS  DAT_CON,
0 as BANC,
'PD' as POS,
SAP.LOCAL_INSTALACAO as DESCR,
SUB.SUB_GEOMETRY as GEOMETRY
FROM
(SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto = 'RG' and id_tn_nominal_eqpto >= '6') SUB
LEFT JOIN 
    (SELECT * FROM MV_BDGD_SAP WHERE OBJ_TECNICO in ('TRT')) SAP 
        ON SUB.LOCAL_INST = SAP.LOCAL_INSTALACAO
/*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN SUB         ON SAP.LOCAL_INSTALACAO=SUB.PAC2
INNER JOIN BDGD_STAGING.TAB_NODE_SUB NS          ON NS.LOC_INST_SAP=SUB.PAC2
INNER JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB GEN ON  'SE-'||GEN.NR_SUB = SAP.SUBESTACAO*/
--WHERE
--SAP.SUBESTACAO is not null AND
--SAP.bdgd = 'EQRE'          AND
--OBJ_TECNICO in ('TRT') --AND
--SAP.A2>=34;"
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNTRD_2" ("COD_ID", "DIST", "PAC_1", "PAC_2", "PAC_3", "FAS_CON_P", "FAS_CON_S", "FAS_CON_T", "SIT_ATIV", "TIP_UNID", "POS", "ATRB_PER", "TEN_LIN_SE", "CAP_ELO", "CAP_CHA", "TAP", "ARE_LOC", "CONF", "POSTO", "POT_NOM", "PER_FER", "PER_TOT", "DAT_CON", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "BANC", "TIP_TRAFO", "MRT", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
CAST(GENESIS.ID AS VARCHAR2(20)) AS "COD_ID",
'5697' AS "DIST",
CASE
WHEN GENESIS.PAC1 is null or GENESIS.PAC1=0 THEN 'D1-'|| CAST(S.EQUIPAMENTO AS VARCHAR2(20))
ELSE CAST(GENESIS.PAC1 AS VARCHAR2(20)) END PAC_1,
CASE
WHEN GENESIS.PAC2 is null or GENESIS.PAC2=0 THEN 'D2-'|| CAST(S.EQUIPAMENTO AS VARCHAR2(20))
ELSE CAST(GENESIS.PAC2 AS VARCHAR2(20)) END PAC_2,
'D3-'|| CAST(S.EQUIPAMENTO AS VARCHAR2(20)) AS "PAC_3",
--TFASCON_AT.COD_ID AS "FAS_CON_P",
--TFASCON_BT.COD_ID AS "FAS_CON_S",
SUGERE.FAS_CON_P,
SUGERE.FAS_CON_S,
SUGERE.FAS_CON_T,
--NULL AS "FAS_CON_T",
'AT' AS "SIT_ATIV",
'38' AS "TIP_UNID",
TPOS.COD_ID AS "POS",
DECODE(AUX.ID_GRUPO_TENSAO_UND_CNS, NULL, '1', '2') AS "ATRB_PER",
--CAST(REPLACE(TTENLINSEC.COD_ID,',','.') AS NUMBER) AS "TEN_LIN_SE",
DECODE(LENGTH(SUGERE.FAS_CON_S), 2, 0.44, 4, DECODE(CAST(REPLACE(TTENLINSEC.COD_ID,',','.') AS NUMBER), 0.22, 0.22, 0.38), '-1') TEN_LIN_SE,
COALESCE(TCAPELFU.COD_ID,'0') AS "CAP_ELO",
COALESCE(TCOR.COD_ID,'0') AS "CAP_CHA",
1 AS "TAP",
DECODE(TARE.COD_ID, NULL, DECODE(LENGTH(GENESIS.FAS_CON_P), 3, 'U', 'NU'), TARE.COD_ID) AS "ARE_LOC",
'RA' AS "CONF",
TPOSTOTRAN.COD_ID AS "POSTO",
CAST(REPLACE(REPLACE(UPPER(POT_NOM), ' KVA',''),',','.') AS NUMBER) AS "POT_NOM",
CAST(REPLACE(S.PER_FER,',','.') AS NUMBER) AS "PER_FER",
CAST(REPLACE(S.PER_TOT,',','.') AS NUMBER) AS "PER_TOT",
S.DATA_CONEXAO AS "DAT_CON",
DECODE(NV.NOVO_ALIMENTADOR, NULL, GENESIS.ALIMENTADOR, NV.NOVO_ALIMENTADOR) AS "CTMT",
DECODE(NV.UN_TR_S, NULL, DECODE(NODE.OBJECTID, NULL, '0', 'SE-' || NODE.OBJECTID), NV.UN_TR_S) as UNI_TR_S,
GENESIS.SUB AS "SUB",
GENESIS.CONJ AS "CONJ",
GENESIS.MUNICIPIO AS "MUN",
'0' AS "BANC",
--COALESCE(TTRANF.COD_ID,'0') AS "TIP_TRAFO",
DECODE(LENGTH(SUGERE.FAS_CON_S), 2, 'MT', 4, 'T', '-1') TIP_TRAFO,
DECODE(LENGTH(GENESIS.FAS_CON_P), 1, '1', '0')  AS "MRT",
S.LOCAL_INSTALACAO || ' - ' || GENESIS.LOCZ || ' - ' || GENESIS.REGIONAL AS "DESCR",
GENESIS.GEOMETRY
FROM MV_PRE_EQTRD_GENESIS GENESIS
LEFT JOIN (SELECT * FROM  MV_BDGD_SAP WHERE OBJ_TECNICO = 'TD' AND (LOCAL_INSTALACAO LIKE 'RD-%' OR LOCAL_INSTALACAO LIKE 'O-%'))  S ON GENESIS.EQUIPAMENTO = S.LOCAL_INSTALACAO
LEFT JOIN  MV_PRE_AUX_TRD_CNS AUX
ON AUX.NR_LOCZ_EQPTO_RD = GENESIS.LOCZ AND AUX.AREA_REGL_RSPDD_ID=GENESIS.REGIONAL
--LEFT JOIN TFASCON_AT ON TFASCON_AT.FASES = GENESIS.FAS_CON_P
--LEFT JOIN TFASCON_BT ON TFASCON_BT.FASES = GENESIS.FAS_CON_S
LEFT JOIN TPOS       ON TPOS.POS = GENESIS.POS
LEFT JOIN TARE       ON TARE.ARE_LOC = GENESIS.ARE_LOC
LEFT JOIN TPOSTOTRAN ON TPOSTOTRAN.POSTO = GENESIS.TIPO
LEFT JOIN TTRANF     ON TTRANF.TIPO = GENESIS.FASE
LEFT JOIN TTENLINSEC ON TTENLINSEC.TEN = GENESIS.TEN_LIN_SEC
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT CON_SUB ON CON_SUB.CD_ALIMENTADOR = GENESIS.ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE      ON NODE.LOC_INST_SAP = CON_SUB.LOC_INST_SAP_TT
LEFT JOIN MV_PRE_TRAFO_CHAVE FTS ON FTS.LOCAL_INSTALACAO = S.LOCAL_INSTALACAO
LEFT JOIN TCOR ON TCOR.CORR = FTS.COR_NOM
LEFT JOIN TCAPELFU ON TCAPELFU.CAP = FTS.ELO_FSV
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV ON NV.ALIMENTADOR = GENESIS.ALIMENTADOR AND (S.LOCAL_INSTALACAO LIKE 'RD%' OR S.LOCAL_INSTALACAO LIKE 'O%')
LEFT JOIN (SELECT
 DISTINCT FEATURE_ID,
          FASE_TRAFO_PRIMARIO AS "FAS_CON_P",
          FASE_TRAFO_SECUNDARIO AS "FAS_CON_S",
          FASE_TRAFO_TERCIARIO AS "FAS_CON_T"
 FROM BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT )SUGERE ON SUGERE.FEATURE_ID=GENESIS.ID
WHERE GENESIS.ID NOT IN (SELECT ID_TRAFO FROM BDGD_STAGING.MV_GENESIS_TRAFO_CNS_MT)


UNION
ALL

SELECT 'SE-' || NODE.OBJECTID AS "COD_ID",
'5697' AS "DIST",
DECODE(TERCIARIO.PAC1, NULL, (DECODE(PRIMARIO_SECUNDARIO.PAC1, NULL, 'D1-'||NODE.OBJECTID, 'SE-' || PRIMARIO_SECUNDARIO.PAC1)), 'SE-' || TERCIARIO.PAC1) AS "PAC_1",
DECODE(TERCIARIO.PAC2_FID, NULL, (DECODE(PRIMARIO_SECUNDARIO.PAC2_FID, NULL, 'D2-'||NODE.OBJECTID, 'SE-' || PRIMARIO_SECUNDARIO.PAC2_FID)), 'SE-' || TERCIARIO .PAC2_FID) AS "PAC_2",
DECODE(TERCIARIO.PAC3, NULL, 'D3-'||NODE.OBJECTID, 'SE-'||TERCIARIO.PAC3) AS "PAC_3",
'ABC' AS "FAS_CON_P",
'ABC' AS "FAS_CON_S",
DECODE(TERCIARIO.PAC3, null, '0', 'ABC') AS "FAS_CON_T",
TSITATI.COD_ID AS "SIT_ATIV",
'41' AS "TIP_UNID",
'PD' AS "POS",
'1' AS "ATRB_PER",
0 AS "TEN_LIN_SEC",
'0' AS "CAP_ELO",
'0' AS "CAP_CHA",
1 AS "TAP",
TARE.COD_ID AS "ARE_LOC",
'0' AS "CONF",
'0' AS "POSTO",
CAST(REPLACE(REPLACE(UPPER(POT_NOM), ' KVA',''),',','.') AS NUMBER) AS "POT_NOM",
CAST(REPLACE(S.PER_FER,',','.') AS NUMBER) AS "PER_FER",
CAST(REPLACE(S.PER_TOT,',','.') AS NUMBER) AS "PER_TOT",
S.DATA_CONEXAO AS "DAT_CON",
DECODE(NV.NOVO_ALIMENTADOR, NULL, TAT.CD_ALIMENTADOR, NV.NOVO_ALIMENTADOR) AS "CTMT",
DECODE(NV.UN_TR_S, NULL, DECODE(NODE.OBJECTID, NULL, '0', 'SE-' || NODE.OBJECTID), NV.UN_TR_S) as UNI_TR_S,
GENESIS.SUB AS "SUB",
GENESIS.CONJ AS "CONJ",
GENESIS.MUNICIPIO AS "MUN",
'0' AS "BANC",
'T' AS "TIP_TRAFO",
'0' AS "MRT",
S.LOCAL_INSTALACAO || ' - ' || S.POT_NOM || ' - ' || S.TEN_PRI || ' - '|| S.TEN_SEC AS "DESCR",
NODE.SHAPE AS "GEOMETRY"
FROM BDGD_STAGING.TAB_NODE_SUB NODE
LEFT JOIN
    (SELECT *FROM MV_BDGD_SAP
    WHERE BDGD='EQTRS') S
    ON NODE.LOC_INST_SAP = S.LOCAL_INSTALACAO
LEFT JOIN
    (SELECT ID_BAR_1,
         ID_BAR_2,
         ID_BAR_3,
         LOC_INST_SAP_TT
    FROM BDGD_STAGING.TAB_TT_BAR
    GROUP BY  ID_BAR_1, ID_BAR_2, ID_BAR_3, LOC_INST_SAP_TT) CON_SUB
    ON CON_SUB.LOC_INST_SAP_TT = S.LOCAL_INSTALACAO
LEFT JOIN TSITATI
    ON TSITATI.SIT_OPER = S.SIT_OPER
LEFT JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB GENESIS
    ON 'SE-' || GENESIS.NR_SUB = S.SUBESTACAO
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAT
    ON TAT.LOC_INST_SAP_ALI = NODE.ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
    ON NV.ALIMENTADOR = TAT.CD_ALIMENTADOR
LEFT JOIN TARE
    ON TARE.ARE_LOC = GENESIS.ARE_LOC
LEFT JOIN TTEN_PERDA
    ON TTEN_PERDA.TEN = S.TEN_PRI
LEFT JOIN TCLATEN
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN
    (SELECT PAC1_FID AS "PAC1",
         PAC2_FID AS "PAC2_FID",
         PAC2,
        PAC3_FID AS " PAC3"
    FROM
        (SELECT PAC1_FID,
         PAC2_FID,
        PAC2,
         LAG(PAC1_FID,
        1,
        0)
            OVER (PARTITION BY PAC2_FID
        ORDER BY  PAC2_FID) AS PAC3_FID
        FROM BDGD_STAGING.tab_bdgd_sub_conn
        WHERE PAC2 IN
            (SELECT LOCAL_INSTALACAO
            FROM MV_BDGD_SAP
            WHERE BDGD = 'EQTRS' ))
            WHERE PAC3_FID = '0') PRIMARIO_SECUNDARIO
            ON PRIMARIO_SECUNDARIO.PAC2 = s.local_instalacao
    LEFT JOIN
    (SELECT PAC1_FID AS "PAC1",
         PAC2_FID AS "PAC2_FID",
         PAC2,
        PAC3_FID AS "PAC3"
    FROM
        (SELECT PAC1_FID,
         PAC2_FID,
        PAC2,
         LAG(PAC1_FID,
        1,
        0)
            OVER (PARTITION BY PAC2_FID
        ORDER BY  PAC2_FID) AS PAC3_FID
        FROM BDGD_STAGING.tab_bdgd_sub_conn
        WHERE PAC2 IN
            (SELECT LOCAL_INSTALACAO
            FROM MV_BDGD_SAP
            WHERE BDGD = 'EQTRS' ))
            WHERE PAC3_FID <> '0') TERCIARIO
            ON TERCIARIO.PAC2 = s.local_instalacao
    WHERE S.BDGD = 'EQTRS'
        AND S.LOCAL_INSTALACAO LIKE 'SE%'
        AND TCLATEN.COD_ID < 11

----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UCBT_TEMP" ("COD_ID", "DIST", "PAC", "CEG", "PN_CON", "UNI_TR_D", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CLAS_SUB", "CNAE", "TIP_CC", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "DAT_CON", "CAR_INST", "LIV", "ARE_LOC", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "SEMRED", "DESCR", "GEOMETRY", "R")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select * from (select mv.*, rank() over (PARTITION BY cod_id order by ROWID) r from mv_bdgd_ucbt_temp_1 mv) where r = 1--where cod_id in (SELECT COD_ID/*, count(*)*/ FROM MV_BDGD_UCBT HAVING COUNT(*) > 1 GROUP BY COD_ID)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_PIP_TEMP" ("COD_ID", "DIST", "MUN", "CONJ", "SUB", "UNI_TR_S", "CTMT", "UNI_TR_D", "PN_CON", "CLAS_SUB", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "ARE_LOC", "PAC", "TIP_CC", "CAR_INST", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "LIV", "SEMRED", "DAT_CON", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
select
pip.COD_ID,
pip.DIST,
pip.MUN,
pip.CONJ,
pip.SUB,
pip.UNI_TR_S,
pip.CTMT,
pip.UNI_TR_D,
pip.PN_CON,
pip.CLAS_SUB,
Coalesce(alt_fas_con.fas_con_sugerida,pip.FAS_CON,'0') as FAS_CON,
pip.GRU_TEN,
pip.TEN_FORN,
pip.GRU_TAR,
pip.SIT_ATIV,
pip.ARE_LOC,
pip.PAC,
pip.TIP_CC,
pip.CAR_INST,
pip.ENE_01,
pip.ENE_02,
pip.ENE_03,
pip.ENE_04,
pip.ENE_05,
pip.ENE_06,
pip.ENE_07,
pip.ENE_08,
pip.ENE_09,
pip.ENE_10,
pip.ENE_11,
pip.ENE_12,
pip.DIC,
pip.FIC,
pip.LIV,
pip.SEMRED,
pip.DAT_CON,
pip.DESCR
from mv_bdgd_pip pip
left join (select * from mv_bdgd_ajust_fase_bt where entidade = 'PIP') alt_fas_con
on alt_fas_con.cod_id = pip.cod_id
)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNCRMT_BKP" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "POT_NOM", "PAC_1", "PAC_2", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "ARE_LOC", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
SELECT 
CAST (GEN.COD_ID as VARCHAR(20)) as COD_ID,
GEN.DIST,
GEN.FAS_CON,
GEN.SIT_ATIV,
GEN.TIP_UNID,
CAST(SAP.POT_NOM as VARCHAR(3))as POT_NOM,
CAST(GEN.PAC_1 as VARCHAR(20)) as PAC_1,
CAST(GEN.PAC_2 as VARCHAR(20)) as PAC_2,
DECODE(NV.NOVO_ALIMENTADOR, NULL, GEN.CD_ALIMENTADOR, NV.NOVO_ALIMENTADOR,'0') AS "CTMT",
DECODE(NV.UN_TR_S, NULL, 'SE-'||BAR.ID_TT, NV.UN_TR_S) as UNI_TR_S,
COALESCE(GEN.SUB,'0') as SUB,
GEN.CONJ,
GEN.MUN,
GEN.ARE_LOC,
LOCINS.DAT_CON,
GEN.BANC,
GEN.POS,
GEN.DESCR,
GEN.GEOMETRY
FROM
(
SELECT
LOCAL_INSTALACAO,
MAX(POT.COD_ID) as POT_NOM
FROM MV_BDGD_SAP SAP
LEFT JOIN TPOTRTV POT ON POT.POT=SAP.A4_TEC
   WHERE SAP.bdgd = 'EQCR' AND 
   SAP.LOCAL_INSTALACAO is not null AND
   LOCAL_INSTALACAO not like 'AX-%'
GROUP BY LOCAL_INSTALACAO
)   SAP
INNER JOIN 
(
SELECT LOCAL_INSTALACAO, 
MAX (
CASE 
  WHEN SAP.DATA_CONEXAO='00/00/0000' THEN '01/01/0001' 
  ELSE SAP.DATA_CONEXAO END 
  )as  DAT_CON
FROM MV_BDGD_SAP SAP
   WHERE SAP.bdgd = 'EQCR' AND 
   SAP.LOCAL_INSTALACAO is not null AND
   LOCAL_INSTALACAO NOT LIKE 'AX-%'
GROUP BY LOCAL_INSTALACAO
) LOCINS ON LOCINS.LOCAL_INSTALACAO = SAP.LOCAL_INSTALACAO
INNER JOIN
(SELECT * FROM BDGD_STAGING.MV_GENESIS_UNCRMT)  GEN   ON  GEN.DESCR          = SAP.LOCAL_INSTALACAO
 LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT                   BAR   ON  BAR.CD_ALIMENTADOR = GEN.CD_ALIMENTADOR
 LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS             NV    ON  NV.ALIMENTADOR     = GEN.CD_ALIMENTADOR
)
UNION ALL
--SUBESTAÇÃO
(
SELECT
'SE-'||NS.OBJECTID as COD_ID,
5697 as DIST,
'ABC'  as FAS_CON,
'AT'  as SIT_ATIV,
14 as TIP_UNID,
POT.COD_ID as POT_NOM,
'SE-'||SUB.PAC1_FID as PAC_1,
'SE-'||SUB.PAC2_FID as PAC_2,
'0' as CTMT,
DECODE(NODE2.OBJECTID,NULL, '0', 'SE-' || NODE2.OBJECTID) AS UNI_TR_S, 
COALESCE(GEN.SUB,'0') as SUB,
GEN.CONJ,
GEN.MUNICIPIO as MUN,
TARE.COD_ID as ARE_LOC,
CASE 
  WHEN SAP.DATA_CONEXAO='00/00/0000' THEN '01/01/0001' 
  ELSE SAP.DATA_CONEXAO END as  DAT_CON,
0 as BANC,
'PD' as POS,
SAP.LOCAL_INSTALACAO as DESCR,
NS.SHAPE as GEOMETRY
FROM
(SELECT * FROM MV_BDGD_SAP WHERE GRUPO_DE_PLANEJAMENTO='GSE') SAP 
INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN SUB         ON SAP.LOCAL_INSTALACAO=SUB.PAC2
INNER JOIN BDGD_STAGING.TAB_NODE_SUB NS          ON NS.LOC_INST_SAP=SUB.PAC2
LEFT JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB GEN ON  'SE-'||GEN.NR_SUB = SAP.SUBESTACAO
LEFT JOIN TARE TARE ON TARE.ARE_LOC=GEN.ARE_LOC
LEFT JOIN TPOTRTV POT ON POT.POT=SAP.A4_TEC
LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2 ON NODE2.LOC_INST_SAP = NS.UN_TR_S
WHERE
SAP.bdgd = 'EQCR'           
)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_BAR" ("COD_ID", "SUB", "DIST", "TEN_NOM", "POS", "PAC", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
--decode(CON_SUB.pac2_fid, null, 'SE-' || NODE.OBJECTID, 'SE-' || CON_SUB.pac2_fid) AS "COD_ID",
SUB.LOCAL_INST COD_ID,
SUB.SUB AS "SUB",
'5697' AS DIST,
TTEN.COD_ID AS "TEN_NOM",
'PD' AS POS,
--decode(SUB.NR_PTO_ELET_FIM, null, 'SE-' || NODE.OBJECTID,*/ '' || SUB.NR_PTO_ELET_FIM) AS "PAC",
'' || SUB.NR_PTO_ELET_FIM AS "PAC",
S.ODI AS "ODI",
S.TI AS "TI",
S.CENTRO_MODULAR AS "CM",
S.TUC AS "TUC",
NVL(S.A1,'00') AS "A1",
NVL(S.A2, '00') AS "A2",
NVL(S.A3,'00') AS "A3",
NVL(S.A4,'00') AS "A4",
NVL(S.A5,'00') AS "A5",
NVL(S.A6, '00') AS "A6",
S.IDUC AS "IDUC",
'AT1' AS "SITCONT",
S.LOCAL_INSTALACAO AS "DESCR"
--NODE.OBJECTID descr_1
FROM (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto = 'BTO') SUB 
INNER JOIN
(SELECT * FROM (SELECT S1.*,
         RANK()
            OVER (PARTITION BY LOCAL_INSTALACAO
        ORDER BY  DATA_CONEXAO) RANK
        FROM MV_BDGD_SAP S1
        WHERE LOCAL_INSTALACAO LIKE 'SE%'
                AND TUC='135'
                AND OBJ_TECNICO='BAR'
                 AND BDGD = 'BAR') A2
        WHERE RANK = 1) S
        ON SUB.LOCAL_INST = S.LOCAL_INSTALACAO
LEFT JOIN TTEN
ON TRIM(S.TEN_PRI) = TTEN.TEN
/*INNER JOIN (
    SELECT PAC2,PAC2_FID 
    FROM BDGD_STAGING.TAB_BDGD_SUB_CONN 
    GROUP BY PAC2,PAC2_FID) CON_SUB
           ON CON_SUB.PAC2 = S.LOCAL_INSTALACAO
LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
           ON NODE.LOC_INST_SAP = S.LOCAL_INSTALACAO           
LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
           ON 'SE-' || SUB.NR_SUBESTACAO = S.SUBESTACAO*/
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_BE" ("COD_ID", "DIST", "SUB_GRP", "ORG_ENER", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND NEXT null
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
BE.COD_ID,
'5697' AS DIST,
BE.SUB_GRP,
BE.ORG_ENER,
NVL(BE.ENE_01, 0) AS "ENE_01",
NVL(BE.ENE_02, 0) AS "ENE_02",
NVL(BE.ENE_03, 0) AS "ENE_03",
NVL(BE.ENE_04, 0) AS "ENE_04",
NVL(BE.ENE_05, 0) AS "ENE_05",
NVL(BE.ENE_06, 0) AS "ENE_06",
NVL(BE.ENE_07, 0) AS "ENE_07",
NVL(BE.ENE_08, 0) AS "ENE_08",
NVL(BE.ENE_09, 0) AS "ENE_09",
NVL(BE.ENE_10, 0) AS "ENE_10",
NVL(BE.ENE_11, 0) AS "ENE_11",
NVL(BE.ENE_12, 0) AS "ENE_12",
BE.DESCR AS "DESCR"
FROM BDGD_STAGING.TAB_PERTEC_BE BE
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_CONJ" ("COD_ID", "DIST", "NOM", "SIST_INTE", "SIST_SUBT", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT * FROM BDGD_STAGING.MV_GENESIS_CONJ
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UGAT" ("COD_ID", "PN_CON", "DIST", "PAC", "CTAT", "CEG", "CONJ", "MUN", "SUB", "LGRD", "BRR", "CEP", "CNAE", "FAS_CON", "GRU_TEN", "TEN_FORN", "SIT_ATIV", "DAT_CON", "POT_INST", "POT_CONT", "DEM_P_01", "DEM_P_02", "DEM_P_03", "DEM_P_04", "DEM_P_05", "DEM_P_06", "DEM_P_07", "DEM_P_08", "DEM_P_09", "DEM_P_10", "DEM_P_11", "DEM_P_12", "DEM_F_01", "DEM_F_02", "DEM_F_03", "DEM_F_04", "DEM_F_05", "DEM_F_06", "DEM_F_07", "DEM_F_08", "DEM_F_09", "DEM_F_10", "DEM_F_11", "DEM_F_12", "ENE_P_01", "ENE_P_02", "ENE_P_03", "ENE_P_04", "ENE_P_05", "ENE_P_06", "ENE_P_07", "ENE_P_08", "ENE_P_09", "ENE_P_10", "ENE_P_11", "ENE_P_12", "ENE_F_01", "ENE_F_02", "ENE_F_03", "ENE_F_04", "ENE_F_05", "ENE_F_06", "ENE_F_07", "ENE_F_08", "ENE_F_09", "ENE_F_10", "ENE_F_11", "ENE_F_12", "DIC", "FIC", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT /*+ LEADING(V_CNS_MAPA_AT) USE_NL(V_CNS_MAPA_AT VS_PRI_RAMAL_LIG_CNS) INDEX(VS_PRI_RAMAL_LIG_CNS SDX_PRI_RAMAL_LIG_CNS_F)*/ SIGA.COD_ID,
GEN.PN_CON,
SIGA.DIST,
'AT-' || GEN.PAC PAC,
DECODE(GEN.CTAT,'120', '303',GEN.CTAT ) CTAT,
SIGA.CEG,
GEN.CONJ,
SIGA.MUN,
GEN.SUB,
SIGA.LGRD,
SIGA.BRR,
SIGA.CEP,
NVL(SIGA.CNAE, '0') CNAE,
SIGA.FAS_CON,
SIGA.GRU_TEN,
SIGA.TEN_FORN,
SIGA.SIT_ATIV,
SIGA.DAT_CON,
TO_NUMBER(SIGA.POT_INST) POT_INST,
TO_NUMBER(SIGA.POT_CONT) POT_CONT,
SIGA.DEM_P_01,
SIGA.DEM_P_02,
SIGA.DEM_P_03,
SIGA.DEM_P_04,
SIGA.DEM_P_05,
SIGA.DEM_P_06,
SIGA.DEM_P_07,
SIGA.DEM_P_08,
SIGA.DEM_P_09,
SIGA.DEM_P_10,
SIGA.DEM_P_11,
SIGA.DEM_P_12,
SIGA.DEM_F_01,
SIGA.DEM_F_02,
SIGA.DEM_F_03,
SIGA.DEM_F_04,
SIGA.DEM_F_05,
SIGA.DEM_F_06,
SIGA.DEM_F_07,
SIGA.DEM_F_08,
SIGA.DEM_F_09,
SIGA.DEM_F_10,
SIGA.DEM_F_11,
SIGA.DEM_F_12,
SIGA.ENE_P_01,
SIGA.ENE_P_02,
SIGA.ENE_P_03,
SIGA.ENE_P_04,
SIGA.ENE_P_05,
SIGA.ENE_P_06,
SIGA.ENE_P_07,
SIGA.ENE_P_08,
SIGA.ENE_P_09,
SIGA.ENE_P_10,
SIGA.ENE_P_11,
SIGA.ENE_P_12,
SIGA.ENE_F_01,
SIGA.ENE_F_02,
SIGA.ENE_F_03,
SIGA.ENE_F_04,
SIGA.ENE_F_05,
SIGA.ENE_F_06,
SIGA.ENE_F_07,
SIGA.ENE_F_08,
SIGA.ENE_F_09,
SIGA.ENE_F_10,
SIGA.ENE_F_11,
SIGA.ENE_F_12,
TO_NUMBER(REPLACE(SIGA.DIC, ',', '.')) DIC,
TO_NUMBER(SIGA.FIC) FIC,
'NR_CONTA_CNS_GENESIS: ' || GEN.COD_ID || ', ' || GEN.DESCR DESCR,
GEN.GEOMETRY
FROM MV_SIGA_UG_AT SIGA
INNER JOIN BDGD_STAGING.MV_GENESIS_UCAT GEN
ON GEN.COD_ID = SIGA.COD_ID
--LEFT JOIN MV_TAB_UC_DIC_FIC DIC_FIC
--     ON DIC_FIC.NR_CONTA_CNS = GEN.COD_ID;";"""""";""";;
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UCBT_TEMP_1" ("COD_ID", "DIST", "PAC", "CEG", "PN_CON", "UNI_TR_D", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CLAS_SUB", "CNAE", "TIP_CC", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "DAT_CON", "CAR_INST", "LIV", "ARE_LOC", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "SEMRED", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
ucbt.cod_id,
ucbt.dist,
ucbt.pac,
ucbt.ceg,
ucbt.pn_con,
ucbt.uni_tr_d,
ucbt.ctmt,
ucbt.uni_tr_s,
ucbt.sub,
ucbt.conj,
ucbt.mun,
ucbt.lgrd,
ucbt.brr,
ucbt.cep,
ucbt.clas_sub,
ucbt.cnae,
ucbt.tip_cc,
coalesce(alt_fas_con.fas_con_sugerida,ucbt.fas_con) as fas_con,
ucbt.gru_ten,
ucbt.ten_forn,
ucbt.gru_tar,
ucbt.sit_ativ,
ucbt.dat_con,
ucbt.car_inst,
ucbt.liv,
ucbt.are_loc,
ucbt.ene_01,
ucbt.ene_02,
ucbt.ene_03,
ucbt.ene_04,
ucbt.ene_05,
ucbt.ene_06,
ucbt.ene_07,
ucbt.ene_08,
ucbt.ene_09,
ucbt.ene_10,
ucbt.ene_11,
ucbt.ene_12,
ucbt.dic,
ucbt.fic,
ucbt.semred,
ucbt.descr,
ucbt.geometry
from mv_bdgd_ucbt ucbt left join  (select * from mv_bdgd_ajust_fase_bt where entidade = 'RAMLIG') alt_fas_con
on alt_fas_con.pac_2 = ucbt.pac
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UCMT_TESTE" ("COD_ID", "PN_CON", "DIST", "PAC", "CEG", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CLAS_SUB", "CNAE", "TIP_CC", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "DAT_CON", "CAR_INST", "LIV", "ARE_LOC", "DEM_01", "DEM_02", "DEM_03", "DEM_04", "DEM_05", "DEM_06", "DEM_07", "DEM_08", "DEM_09", "DEM_10", "DEM_11", "DEM_12", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "SEMRED", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select
ucmt.cod_id,
ucmt.pn_con,
ucmt.dist,
ucmt.pac,
ucmt.ceg,
ucmt.ctmt,
ucmt.uni_tr_s,
ucmt.sub,
ucmt.conj,
ucmt.mun,
ucmt.lgrd,
ucmt.brr,
ucmt.cep,
COALESCE(ajust_clas_sub.cod_ref_clas_sub_clas_rcl,ucmt.clas_sub,'0') as clas_sub,
ucmt.cnae,
ucmt.tip_cc,
ucmt.fas_con,
ucmt.gru_ten,
ucmt.ten_forn,
ucmt.gru_tar,
ucmt.sit_ativ,
ucmt.dat_con,
ucmt.car_inst,
ucmt.liv,
ucmt.are_loc,
ucmt.dem_01,
ucmt.dem_02,
ucmt.dem_03,
ucmt.dem_04,
ucmt.dem_05,
ucmt.dem_06,
ucmt.dem_07,
ucmt.dem_08,
ucmt.dem_09,
ucmt.dem_10,
ucmt.dem_11,
ucmt.dem_12,
ucmt.ene_01,
ucmt.ene_02,
ucmt.ene_03,
ucmt.ene_04,
ucmt.ene_05,
ucmt.ene_06,
ucmt.ene_07,
ucmt.ene_08,
ucmt.ene_09,
ucmt.ene_10,
ucmt.ene_11,
ucmt.ene_12,
ucmt.dic,
ucmt.fic,
ucmt.semred,
ucmt.descr,
ucmt.geometry
from mv_bdgd_ucmt ucmt
left join
(
select * from (select mv.*, rank() over (PARTITION BY cod_id order by ROWID) r from dif_clas_sub_bdgd_siga mv) 
where r = 1 and clas_sub = 'CO1' and cod_ref_clas_sub_clas_rcl like 'RU%' and entidade = 'UCMT'
) ajust_clas_sub  
on ajust_clas_sub.cod_id = ucmt.cod_id
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNSEMT_BKP_2" ("COD_ID", "DIST", "PAC_1", "PAC_2", "FAS_CON", "SIT_ATIV", "TIP_UNID", "P_N_OPE", "CAP_ELO", "COR_NOM", "TLCD", "DAT_CON", "POS", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "ARE_LOC", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT   TO_CHAR(GEN.COD_ID) COD_ID,
            GEN.DIST,
            COALESCE(TO_CHAR(GEN.PAC_1),
            'DESC_' || TO_CHAR(GEN.COD_ID)) PAC_1, 
            COALESCE(TO_CHAR(GEN.PAC_2),
            'DESC_' || TO_CHAR(GEN.COD_ID)) PAC_2, 
            GEN.FAS_CON, 'AT' SIT_ATIV, 
            GEN.TIP_UNID, GEN.P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            NVL(TCOR.COD_ID, '0') COR_NOM,
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(SAP.DATA_CONEXAO,'01/01/1900', '24/10/2010', SAP.DATA_CONEXAO) DAT_CON, 
            GEN.POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,GEN.CTMT,'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,TAB.LOC_INST_SAP_TT,'0') AS "UNI_TR_S",
            COALESCE(GEN.SUB,'0') as SUB, 
            GEN.CONJ, 
            GEN.MUN, 
            GEN.ARE_LOC, 
            SAP.LOCAL_INSTALACAO AS "DESCR", 
            GEN.GEOMETRY
    FROM BDGD_STAGING.MV_GENESIS_UNSEMT_1 GEN
    LEFT JOIN MV_BDGD_SAP SAP
        ON SAP.EQUIPAMENTO = GEN.NR_EQUIPAMENTO
    LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
        ON TAB.CD_ALIMENTADOR = GEN.CTMT
    LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
        ON NV.ALIMENTADOR = TAB.CD_ALIMENTADOR
    LEFT JOIN TCAPELFU
        ON TCAPELFU.CAP = REPLACE(SAP.ELO_FSV, ',0', '')
    LEFT JOIN TCOR
        ON TCOR.CORR = SAP.COR_NOM
    WHERE SAP.A2 < 60
            OR SAP.A2 IS NULL
    UNION
    ALL
    SELECT TO_CHAR(GEN.COD_ID),
            GEN.DIST,
            COALESCE(TO_CHAR(GEN.PAC_1),
            'DESC_'||TO_CHAR(GEN.COD_ID)) PAC_1, 
            COALESCE(TO_CHAR(GEN.PAC_2),'DESC_'||TO_CHAR(GEN.COD_ID)) PAC_2, 
            GEN.FAS_CON, 'AT' SIT_ATIV, 
            GEN.TIP_UNID, GEN.P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            NVL(TCOR.COD_ID, '0') COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(SAP.DATA_CONEXAO,'01/01/1900', '24/10/2010', SAP.DATA_CONEXAO) DAT_CON, 
            GEN.POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,GEN.CTMT,'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,TAB.LOC_INST_SAP_TT,'0') AS "UNI_TR_S",
            COALESCE(GEN.SUB,'0') as SUB, 
            GEN.CONJ, 
            GEN.MUN, GEN.ARE_LOC, 
            SAP.LOCAL_INSTALACAO AS "DESCR", 
            GEN.GEOMETRY
    FROM BDGD_STAGING.MV_GENESIS_UNSEMT_2 GEN
    LEFT JOIN 
        (SELECT *
        FROM MV_BDGD_SAP S
        WHERE S.BDGD = 'EQSE'
                AND S.OBJ_TECNICO IN ('BF','CD', 'CDA', 'CDP', 'CDT', 'CO', 'FA', 'FF', 'FP', 'FR', 'FT', 'FU')) SAP
        ON SAP.LOCAL_INSTALACAO = GEN.DESCR
    LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
        ON TAB.CD_ALIMENTADOR = GEN.CTMT
    LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
        ON NV.ALIMENTADOR = TAB.CD_ALIMENTADOR
    LEFT JOIN TCAPELFU
        ON TCAPELFU.CAP = REPLACE(SAP.ELO_FSV, ',0', '')
    LEFT JOIN TCOR
        ON TCOR.CORR = SAP.COR_NOM
    WHERE SAP.A2 < 60
            OR SAP.A2 IS NULL
    UNION
    ALL
    -----Subestacao
    select k.cod_id,
           k.dist,
           coalesce(p.pac_1, k.pac_1) pac_1,
           coalesce(p.pac_2, k.pac_2) pac_2,
           k.fas_con,
           k.SIT_ATIV, 
           k.TIP_UNID, 
           k.P_N_OPE, 
           k.CAP_ELO, 
           k.COR_NOM,
           k.TLCD, 
           k.DAT_CON, 
           k.POS, 
           k.CTMT, 
           k.UNI_TR_S,
           k.SUB, 
           k.CONJ, 
           k.MUN, 
           k.ARE_LOC, 
           k.DESCR, 
           k.GEOMETRY
    from
    (
    SELECT A.*,
            G.sub_geometry GEOMETRY
        FROM (SELECT'' || sub.ID COD_ID, '5697' DIST,
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_INICIO) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_INICIO)
            ELSE 'D1-'||sub.NR_PTO_ELET_INICIO
            END AS PAC_1, 
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_FIM) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_FIM)
            ELSE 'D2-'||sub.NR_PTO_ELET_FIM
            END AS PAC_2, 
            'ABC' FAS_CON, 
            'AT' SIT_ATIV, 
            DECODE(S.TUC, 160, DECODE(S.A1, 3, 22, 34), 210, 29, 345, 32) TIP_UNID, 
            decode(sub.situacao_operacional, 1, 'A', 2, 'F', 3, 'F', '-1') P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            TCOR.COD_ID COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(S.DATA_CONEXAO,'01/01/1900', '24/10/2010', S.DATA_CONEXAO) DAT_CON, 
            'PD' POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,DECODE(SUB.ID_TIPO_MODULO,1,'0',SUB.CD_ALIMENTADOR),'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
            COALESCE(SUB.sub,'0') SUB, 
            sub.conj CONJ, 
            sub.municipio MUN, 
            DECODE(SUB.ARE_LOC, 'U', 'UB', 'R', 'NU', NULL) ARE_LOC,
            Sub.LOCAL_INST DESCR--, --NODE.SHAPE GEOMETRY
        FROM 
            (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto in ('CBY','CD','CF','CN','CO','CQ','CT','CU','CUA','DJ','FU','RL') AND id_tn_nominal_eqpto < '6' AND (FEATURE_TYPE != 'LocalInstalacaoEqptoOperTandem' AND FEATURE_TYPE !='LocalInstalacaoEqptoOperByPass')) SUB
            LEFT JOIN 
            (SELECT A2.EQUIPAMENTO,
            A2.STATUS_SE,
            A2.COR_NOM,
            A2.A1,
            A2.TUC,
            A2.A3,
            A2.A2,
            A2.BDGD,
            A2.ELO_FSV,
            A2.DATA_CONEXAO,
            A2.LOCAL_INSTALACAO
            FROM 
                (SELECT S1.*,
            ROW_NUMBER()
                    OVER (PARTITION BY LOCAL_INSTALACAO
                ORDER BY  DATA_CONEXAO DESC) ROW_NUMBER
                FROM MV_BDGD_SAP S1
                WHERE LOCAL_INSTALACAO LIKE 'SE%'
                        AND BDGD ='EQSE' ) A2
                WHERE ROW_NUMBER = 1
                GROUP BY  A2.LOCAL_INSTALACAO,A2.COR_NOM, A2.STATUS_SE, A2.A1, A2.TUC, A2.A3, A2.A2, A2.BDGD, A2.ELO_FSV, A2.DATA_CONEXAO, A2.EQUIPAMENTO ) S
                    ON S.LOCAL_INSTALACAO = SUB.LOCAL_INST
                /*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN CON
                    ON CON.PAC2 = S.LOCAL_INSTALACAO
                LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
                    ON NODE.LOC_INST_SAP = CON.PAC2*/                LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
                    ON TAB.CD_ALIMENTADOR = SUB.CD_ALIMENTADOR
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_ALIMENTADOR ALI
                    ON ALI.CD_ALIMENTADOR = TAB.CD_ALIMENTADOR
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
                    --ON SUB.NR_SUBESTACAO = TO_NUMBER(SUBSTR(NODE.LOC_INST_SAP, 4, 3))
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONJUNTO_ANEEL CONJ
                    --ON CONJ.PRI_CONJUNTO_CNS_ID = SUB.PRI_CONJUNTO_CNS_ID
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_MUNICIPIO_ANEEL MUN
                    --ON MUN.PRI_MUNICIPIO_ID = SUB.PRI_MUNICIPIO_ID
                LEFT JOIN (SELECT distinct substr(alimentador,1,3) as SUB, novo_alimentador,un_tr_s FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) NV ON NV.SUB = SUB.SUB
                --LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2
                    --ON NODE2.LOC_INST_SAP = NODE.UN_TR_S
                LEFT JOIN TCAPELFU
                    ON TCAPELFU.CAP = REPLACE(S.ELO_FSV, ',0', '')
                LEFT JOIN TCOR
                    ON TCOR.CORR = S.COR_NOM
                --WHERE S.BDGD = 'EQSE'
                        --AND S.A2 < 60
                GROUP BY  
                SUB.ID,
                SUB.LOCAL_INST,
                sub.nr_pto_elet_fim, 
                S.STATUS_SE, 
                S.A1, 
                S.TUC, 
                TCOR.COD_ID, 
                S.A3, 
                TCAPELFU.COD_ID, 
                S.DATA_CONEXAO, 
                sub.CD_ALIMENTADOR, 
                sub.local_inst_tt, 
                SUB.sub, 
                sub.conj, 
                sub.municipio, 
                SUB.ARE_LOC, 
                S.LOCAL_INSTALACAO, 
                NV.NOVO_ALIMENTADOR,
                NR_PTO_ELET_INICIO,
                sub.situacao_operacional,
                --NODE.UN_TR_S,
                --NODE2.OBJECTID, 
                NV.UN_TR_S,
                SUB.ID_TIPO_MODULO ) A
            LEFT JOIN GEN_BDGD2019.MV_EQPTOS_SUBESTACAO G
            ON G.local_inst = A.DESCR
    UNION ALL --CHAVE TANDEM   
       SELECT A.*,
            G.sub_geometry GEOMETRY
        FROM (SELECT'' || sub.ID COD_ID, '5697' DIST,
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_INICIO) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_INICIO)
            ELSE 'D1-'||sub.NR_PTO_ELET_INICIO
            END AS PAC_1, 
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_FIM) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_FIM)
            ELSE 'D2-'||sub.NR_PTO_ELET_FIM
            END AS PAC_2, 
            'ABC' FAS_CON, 
            'AT' SIT_ATIV, 
            DECODE(S.TUC, 160, DECODE(S.A1, 3, 22, 34), 210, 29, 345, 32) TIP_UNID, 
            decode(sub.situacao_operacional, 1, 'A', 2, 'F', 3, 'F', '-1') P_N_OPE, 
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            TCOR.COD_ID COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(S.DATA_CONEXAO,'01/01/1900', '24/10/2010', S.DATA_CONEXAO) DAT_CON, 
            'PD' POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,DECODE(SUB.ID_TIPO_MODULO,1,'0',SUB.CD_ALIMENTADOR),'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
            COALESCE(SUB.sub,'0') SUB, 
            sub.conj CONJ, 
            sub.municipio MUN, 
            DECODE(SUB.ARE_LOC, 'U', 'UB', 'R', 'NU', NULL) ARE_LOC,
            Sub.LOCAL_INST DESCR--, --NODE.SHAPE GEOMETRY
        FROM 
            (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto in ('CBY','CD','CF','CN','CO','CQ','CT','CU','CUA','DJ','FU','RL') AND id_tn_nominal_eqpto < '6' AND FEATURE_TYPE = 'LocalInstalacaoEqptoOperTandem' AND ID_SITUACAO_OPER_EQPTO = 2) SUB
            LEFT JOIN 
            (SELECT A2.EQUIPAMENTO,
            A2.STATUS_SE,
            A2.COR_NOM,
            A2.A1,
            A2.TUC,
            A2.A3,
            A2.A2,
            A2.BDGD,
            A2.ELO_FSV,
            A2.DATA_CONEXAO,
            A2.LOCAL_INSTALACAO
            FROM 
                (SELECT S1.*,
            ROW_NUMBER()
                    OVER (PARTITION BY LOCAL_INSTALACAO
                ORDER BY  DATA_CONEXAO DESC) ROW_NUMBER
                FROM MV_BDGD_SAP S1
                WHERE LOCAL_INSTALACAO LIKE 'SE%'
                        AND BDGD ='EQSE' ) A2
                WHERE ROW_NUMBER = 1
                GROUP BY  A2.LOCAL_INSTALACAO,A2.COR_NOM, A2.STATUS_SE, A2.A1, A2.TUC, A2.A3, A2.A2, A2.BDGD, A2.ELO_FSV, A2.DATA_CONEXAO, A2.EQUIPAMENTO ) S
                    ON S.LOCAL_INSTALACAO = SUB.LOCAL_INST
                /*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN CON
                    ON CON.PAC2 = S.LOCAL_INSTALACAO
                LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
                    ON NODE.LOC_INST_SAP = CON.PAC2*/                LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
                    ON TAB.CD_ALIMENTADOR = SUB.CD_ALIMENTADOR
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_ALIMENTADOR ALI
                    ON ALI.CD_ALIMENTADOR = TAB.CD_ALIMENTADOR
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
                    --ON SUB.NR_SUBESTACAO = TO_NUMBER(SUBSTR(NODE.LOC_INST_SAP, 4, 3))
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONJUNTO_ANEEL CONJ
                    --ON CONJ.PRI_CONJUNTO_CNS_ID = SUB.PRI_CONJUNTO_CNS_ID
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_MUNICIPIO_ANEEL MUN
                    --ON MUN.PRI_MUNICIPIO_ID = SUB.PRI_MUNICIPIO_ID
               LEFT JOIN (SELECT distinct substr(alimentador,1,3) as SUB, novo_alimentador,un_tr_s FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) NV ON NV.SUB = SUB.SUB
                --LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2
                    --ON NODE2.LOC_INST_SAP = NODE.UN_TR_S
                LEFT JOIN TCAPELFU
                    ON TCAPELFU.CAP = REPLACE(S.ELO_FSV, ',0', '')
                LEFT JOIN TCOR
                    ON TCOR.CORR = S.COR_NOM
                --WHERE S.BDGD = 'EQSE'
                        --AND S.A2 < 60
                GROUP BY  
                SUB.ID,
                SUB.LOCAL_INST,
                sub.nr_pto_elet_fim, 
                S.STATUS_SE, 
                S.A1, 
                S.TUC, 
                TCOR.COD_ID, 
                S.A3, 
                TCAPELFU.COD_ID, 
                S.DATA_CONEXAO, 
                sub.CD_ALIMENTADOR, 
                sub.local_inst_tt, 
                SUB.sub, 
                sub.conj, 
                sub.municipio, 
                SUB.ARE_LOC, 
                S.LOCAL_INSTALACAO, 
                NV.NOVO_ALIMENTADOR,
                NR_PTO_ELET_INICIO,
                sub.situacao_operacional,
                --NODE.UN_TR_S,
                --NODE2.OBJECTID, 
                NV.UN_TR_S,
                SUB.ID_TIPO_MODULO ) A
            LEFT JOIN (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE ID_SITUACAO_OPER_EQPTO = 2) G
            ON G.local_inst = A.DESCR
    UNION ALL --CHAVE BY PASS    
    SELECT A.*,
            G.sub_geometry GEOMETRY
        FROM (SELECT'' || sub.ID COD_ID, '5697' DIST,
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_INICIO) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_INICIO)
            ELSE 'D1-'||sub.NR_PTO_ELET_INICIO
            END AS PAC_1, 
            CASE
            WHEN MAX(SUB.NR_PTO_ELET_FIM) IS NOT NULL THEN
            'SE-' || MAX(SUB.NR_PTO_ELET_FIM)
            ELSE 'D2-'||sub.NR_PTO_ELET_FIM
            END AS PAC_2, 
            'ABC' FAS_CON, 
            'AT' SIT_ATIV, 
            DECODE(S.TUC, 160, DECODE(S.A1, 3, 22, 34), 210, 29, 345, 32) TIP_UNID, 
            decode(sub.situacao_operacional, 1, 'A', 2, 'F', 3, 'F', '-1') P_N_OPE,
            NVL(TCAPELFU.COD_ID, '0') CAP_ELO, 
            TCOR.COD_ID COR_NOM, 
            DECODE(TUC, 540, '1', 345, '1', 210, '1', 160, '0', '0') TLCD, 
            DECODE(S.DATA_CONEXAO,'01/01/1900', '24/10/2010', S.DATA_CONEXAO) DAT_CON, 
            'PD' POS, 
            COALESCE(NV.NOVO_ALIMENTADOR,DECODE(SUB.ID_TIPO_MODULO,1,'0',SUB.CD_ALIMENTADOR),'0') AS "CTMT", 
            COALESCE(NV.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
            COALESCE(SUB.sub,'0') SUB, 
            sub.conj CONJ, 
            sub.municipio MUN, 
            DECODE(SUB.ARE_LOC, 'U', 'UB', 'R', 'NU', NULL) ARE_LOC,
            Sub.LOCAL_INST DESCR--, --NODE.SHAPE GEOMETRY
        FROM 
            (SELECT * FROM (SELECT A.*, RANK() OVER (PARTITION BY LOCAL_INST ORDER BY ID) R FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO A WHERE FEATURE_TYPE = 'LocalInstalacaoEqptoOperByPass') WHERE R != 1) SUB
            LEFT JOIN 
            (SELECT A2.EQUIPAMENTO,
            A2.STATUS_SE,
            A2.COR_NOM,
            A2.A1,
            A2.TUC,
            A2.A3,
            A2.A2,
            A2.BDGD,
            A2.ELO_FSV,
            A2.DATA_CONEXAO,
            A2.LOCAL_INSTALACAO
            FROM 
                (SELECT S1.*,
            ROW_NUMBER()
                    OVER (PARTITION BY LOCAL_INSTALACAO
                ORDER BY  DATA_CONEXAO DESC) ROW_NUMBER
                FROM MV_BDGD_SAP S1
                WHERE LOCAL_INSTALACAO LIKE 'SE%'
                        AND BDGD ='EQSE' ) A2
                WHERE ROW_NUMBER = 1
                GROUP BY  A2.LOCAL_INSTALACAO,A2.COR_NOM, A2.STATUS_SE, A2.A1, A2.TUC, A2.A3, A2.A2, A2.BDGD, A2.ELO_FSV, A2.DATA_CONEXAO, A2.EQUIPAMENTO ) S
                    ON S.LOCAL_INSTALACAO = SUB.LOCAL_INST
                /*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN CON
                    ON CON.PAC2 = S.LOCAL_INSTALACAO
                LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE
                    ON NODE.LOC_INST_SAP = CON.PAC2*/                LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
                    ON TAB.CD_ALIMENTADOR = SUB.CD_ALIMENTADOR
                LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_ALIMENTADOR ALI
                    ON ALI.CD_ALIMENTADOR = TAB.CD_ALIMENTADOR
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB
                    --ON SUB.NR_SUBESTACAO = TO_NUMBER(SUBSTR(NODE.LOC_INST_SAP, 4, 3))
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONJUNTO_ANEEL CONJ
                    --ON CONJ.PRI_CONJUNTO_CNS_ID = SUB.PRI_CONJUNTO_CNS_ID
                --LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_MUNICIPIO_ANEEL MUN
                    --ON MUN.PRI_MUNICIPIO_ID = SUB.PRI_MUNICIPIO_ID
                LEFT JOIN (SELECT distinct substr(alimentador,1,3) as SUB, novo_alimentador,un_tr_s FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) NV ON NV.SUB = SUB.SUB
                --LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2
                    --ON NODE2.LOC_INST_SAP = NODE.UN_TR_S
                LEFT JOIN TCAPELFU
                    ON TCAPELFU.CAP = REPLACE(S.ELO_FSV, ',0', '')
                LEFT JOIN TCOR
                    ON TCOR.CORR = S.COR_NOM
                --WHERE S.BDGD = 'EQSE'
                        --AND S.A2 < 60
                GROUP BY  
                SUB.ID,
                SUB.LOCAL_INST,
                sub.nr_pto_elet_fim, 
                S.STATUS_SE, 
                S.A1, 
                S.TUC, 
                TCOR.COD_ID, 
                S.A3, 
                TCAPELFU.COD_ID, 
                S.DATA_CONEXAO, 
                sub.CD_ALIMENTADOR, 
                sub.local_inst_tt, 
                SUB.sub, 
                sub.conj, 
                sub.municipio, 
                SUB.ARE_LOC, 
                S.LOCAL_INSTALACAO, 
                NV.NOVO_ALIMENTADOR,
                NR_PTO_ELET_INICIO,
                sub.situacao_operacional,
                --NODE.UN_TR_S,
                --NODE2.OBJECTID, 
                NV.UN_TR_S,
                SUB.ID_TIPO_MODULO ) A
            LEFT JOIN (SELECT * FROM (SELECT A.*, RANK() OVER (PARTITION BY LOCAL_INST ORDER BY ID) R FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO A WHERE TIPO_EQPTO = 'CBY') WHERE R != 2) G
            ON G.local_inst = A.DESCR) k
            left join tab_aux_pac_se_re p
                    on p.loc_inst = descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_VALIDA_DIFF_MT" ("ID", "ID_CONNECTED", "ID_ENERGIZED", "OLD_FEAT_NUM")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT * FROM BDGD_STAGING.TAB_BDGD_VALIDA_DIFF_MT
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNTRD" ("COD_ID", "DIST", "PAC_1", "PAC_2", "PAC_3", "FAS_CON_P", "FAS_CON_S", "FAS_CON_T", "SIT_ATIV", "TIP_UNID", "POS", "ATRB_PER", "TEN_LIN_SE", "CAP_ELO", "CAP_CHA", "TAP", "ARE_LOC", "CONF", "POSTO", "POT_NOM", "PER_FER", "PER_TOT", "DAT_CON", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "BANC", "TIP_TRAFO", "MRT", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
CAST(GENESIS.ID AS VARCHAR2(20)) AS "COD_ID",
'5697' AS "DIST",
CASE
WHEN GENESIS.PAC1 is null or GENESIS.PAC1=0 THEN 'D1-'|| CAST(S.EQUIPAMENTO AS VARCHAR2(20))
ELSE CAST(GENESIS.PAC1 AS VARCHAR2(20)) END PAC_1,
CASE
WHEN GENESIS.PAC2 is null or GENESIS.PAC2=0 THEN 'D2-'|| CAST(S.EQUIPAMENTO AS VARCHAR2(20))
ELSE CAST(GENESIS.PAC2 AS VARCHAR2(20)) END PAC_2,
'D3-'|| COALESCE(CAST(S.EQUIPAMENTO AS VARCHAR2(20)),CAST(GENESIS.ID AS VARCHAR2(20))) AS "PAC_3",
--TFASCON_AT.COD_ID AS "FAS_CON_P",
--TFASCON_BT.COD_ID AS "FAS_CON_S",
SUGERE.FAS_CON_P,
SUGERE.FAS_CON_S,
SUGERE.FAS_CON_T,
--NULL AS "FAS_CON_T",
'AT' AS "SIT_ATIV",
'38' AS "TIP_UNID",
TPOS.COD_ID AS "POS",
CASE 
WHEN AUX.ID_GRUPO_TENSAO_UND_CNS is not null THEN '1'
ELSE CAST(GENESIS.ID_PRPR_TRAFO_DIST as VARCHAR(1)) END AS "ATRB_PER",
--CAST(REPLACE(TTENLINSEC.COD_ID,',','.') AS NUMBER) AS "TEN_LIN_SE",
DECODE(LENGTH(SUGERE.FAS_CON_S), 2, 0.44, 4, DECODE(CAST(REPLACE(TTENLINSEC.COD_ID,',','.') AS NUMBER), 0.22, 0.22, 0.38), '-1') TEN_LIN_SE,
COALESCE(TCAPELFU.COD_ID,TCAPEL.CAP_ELO,'1H')as CAP_ELO,
COALESCE(TCOR.COD_ID,'24') as CAP_CHA,
1 AS "TAP",
DECODE(TARE.COD_ID, NULL, DECODE(LENGTH(GENESIS.FAS_CON_P), 3, 'U', 'NU'), TARE.COD_ID) AS "ARE_LOC",
'RA' AS "CONF",
TPOSTOTRAN.COD_ID AS "POSTO",
COALESCE(CAST(REPLACE(REPLACE(UPPER(S.POT_NOM), ' KVA',''),',','.') AS NUMBER),TCAPEL.POT_NOM,10) as POT_NOM,
Coalesce(CAST(REPLACE(S.PER_FER,',','.') AS NUMBER) ,TCAPEL.PERFER,85)as PER_FER,
COALESCE(CAST(REPLACE(S.PER_TOT,',','.') AS NUMBER), TCAPEL.PERTOT,300)as PER_TOT,
COALESCE(S.DATA_CONEXAO,'20/10/2010') AS "DAT_CON",
COALESCE(NV.NOVO_ALIMENTADOR,GENESIS.ALIMENTADOR,'0') AS "CTMT",
COALESCE(NV.UN_TR_S,CON_SUB.LOC_INST_SAP_TT,'0') AS "UNI_TR_S",
GENESIS.SUB AS "SUB",
GENESIS.CONJ AS "CONJ",
GENESIS.MUNICIPIO AS "MUN",
'0' AS "BANC",
--DECODE(LENGTH(SUGERE.FAS_CON_P), 1, 'MT', 2, 'B', 3, 'T', '0') TIP_TRAFO,
DECODE(LENGTH(SUGERE.FAS_CON_P), 1, 'MT', 2, 'MT', 3, 'T', '0') TIP_TRAFO,
DECODE(LENGTH(SUGERE.FAS_CON_P), 1, '1', '0')  AS "MRT",

'SAP: ' || S.LOCAL_INSTALACAO || ' - GENESIS:' || GENESIS.EQUIPAMENTO || '-' || GENESIS.LOCZ || ' - ' || GENESIS.REGIONAL AS "DESCR",
GENESIS.GEOMETRY
FROM MV_PRE_EQTRD_GENESIS GENESIS
LEFT JOIN (SELECT * FROM  MV_BDGD_SAP WHERE OBJ_TECNICO = 'TD' AND (LOCAL_INSTALACAO LIKE 'RD-%' OR LOCAL_INSTALACAO LIKE 'O-%'))  S ON GENESIS.EQUIPAMENTO = S.LOCAL_INSTALACAO
LEFT JOIN  MV_PRE_AUX_TRD_CNS AUX
ON AUX.NR_LOCZ_EQPTO_RD = GENESIS.LOCZ AND AUX.AREA_REGL_RSPDD_ID=GENESIS.REGIONAL
--LEFT JOIN TFASCON_AT ON TFASCON_AT.FASES = GENESIS.FAS_CON_P
--LEFT JOIN TFASCON_BT ON TFASCON_BT.FASES = GENESIS.FAS_CON_S
LEFT JOIN TPOS       ON TPOS.POS = GENESIS.POS
LEFT JOIN TARE       ON TARE.ARE_LOC = GENESIS.ARE_LOC
LEFT JOIN TPOSTOTRAN ON TPOSTOTRAN.POSTO = GENESIS.TIPO
LEFT JOIN TTRANF     ON TTRANF.TIPO = GENESIS.FASE
LEFT JOIN TTENLINSEC ON TTENLINSEC.TEN = GENESIS.TEN_LIN_SEC
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT CON_SUB ON CON_SUB.CD_ALIMENTADOR = GENESIS.ALIMENTADOR
--LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE      ON NODE.LOC_INST_SAP = CON_SUB.LOC_INST_SAP_TT
LEFT JOIN MV_PRE_TRAFO_CHAVE FTS ON FTS.LOCAL_INSTALACAO = S.LOCAL_INSTALACAO
LEFT JOIN TCOR ON TCOR.CORR = FTS.COR_NOM
LEFT JOIN TCAPELFU ON TCAPELFU.CAP = FTS.ELO_FSV
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV ON NV.ALIMENTADOR = GENESIS.ALIMENTADOR AND (S.LOCAL_INSTALACAO LIKE 'RD%' OR S.LOCAL_INSTALACAO LIKE 'O%')
LEFT JOIN TAB_BDGD_CAP_ELO_PARTICULAR TCAPEL ON  TCAPEL.COD_ID=GENESIS.ID  
LEFT JOIN (SELECT
 DISTINCT FEATURE_ID,
          FASE_TRAFO_PRIMARIO AS "FAS_CON_P",
          FASE_TRAFO_SECUNDARIO AS "FAS_CON_S",
          FASE_TRAFO_TERCIARIO AS "FAS_CON_T"
 FROM BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT )SUGERE ON SUGERE.FEATURE_ID=GENESIS.ID
WHERE GENESIS.ID NOT IN (SELECT ID_TRAFO FROM BDGD_STAGING.MV_GENESIS_TRAFO_CNS_MT)


UNION
ALL

SELECT DECODE(SUB.ID, NULL, 'SE-'||SAP.EQUIPAMENTO, SUB.LOCAL_INST) COD_ID,
'5697' AS "DIST",
DECODE(SUB.NR_PTO_ELET_INICIO, NULL, 'D1-' || SAP.EQUIPAMENTO, 'SE-'||SUB.NR_PTO_ELET_INICIO) AS PAC_1,
DECODE(SUB.NR_PTO_ELET_FIM, NULL, 'D2-' || SAP.EQUIPAMENTO, 'SE-'||SUB.NR_PTO_ELET_FIM) AS PAC_2,
'D3-' || SAP.EQUIPAMENTO AS PAC_3,
'ABC' AS "FAS_CON_P",
'ABC' AS "FAS_CON_S",
'0' AS "FAS_CON_T",
TSITATI.COD_ID AS "SIT_ATIV",
'41' AS "TIP_UNID",
'PD' AS "POS",
'1' AS "ATRB_PER",
CAST(COALESCE(REPLACE(REPLACE(UPPER(SAP.TEN_SEC), ' KV',''),',','.'),'0') as NUMBER)AS "TEN_LIN_SE",
'0' AS "CAP_ELO",
'0' AS "CAP_CHA",
1 AS "TAP",
TARE.COD_ID AS "ARE_LOC",
'0' AS "CONF",
'0' AS "POSTO",
CAST(REPLACE(REPLACE(UPPER(SAP.POT_NOM), ' KVA',''),',','.') AS NUMBER) AS "POT_NOM",
CAST(REPLACE(SAP.PER_FER,',','.') AS NUMBER) AS "PER_FER",
CAST(REPLACE(SAP.PER_TOT,',','.') AS NUMBER) AS "PER_TOT",
SAP.DATA_CONEXAO AS "DAT_CON",
COALESCE(N_ALIM.NOVO_ALIMENTADOR,SUB.CD_ALIMENTADOR,'0') AS "CTMT",
COALESCE(N_ALIM.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
COALESCE(SUB.SUB,'0') as SUB,
SUB.CONJ AS "CONJ",
SUB.MUNICIPIO as "MUN",
'0' AS "BANC",
'T' AS "TIP_TRAFO",
'0' AS "MRT",
SAP.LOCAL_INSTALACAO || ' - ' || SAP.POT_NOM || ' - ' || SAP.TEN_PRI || ' - '|| SAP.TEN_SEC AS "DESCR",
g.centroid_geo AS "GEOMETRY"
FROM (SELECT * FROM MV_BDGD_SAP WHERE LOCAL_INSTALACAO LIKE 'SE-0%' AND BDGD = 'EQTRS') SAP
INNER JOIN (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE TIPO_EQPTO = 'TT' AND id_tn_nominal_eqpto < 6) SUB
ON SAP.LOCAL_INSTALACAO = SUB.LOCAL_INST
LEFT JOIN (SELECT distinct 
           substr(alimentador,1,3) as SUB,
            novo_alimentador,un_tr_s 
        FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) N_ALIM  ON N_ALIM.SUB = SUB.SUB
LEFT JOIN TSITATI
    ON TSITATI.SIT_OPER = SAP.SIT_OPER
LEFT JOIN TARE
    ON TARE.ARE_LOC = SUB.ARE_LOC
LEFT JOIN TTEN_PERDA
    ON TTEN_PERDA.TEN = SAP.TEN_PRI
LEFT JOIN TCLATEN
    ON TCLATEN.TEN = SAP.CLAS_TEN
LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO g
on sub.sub = g.sg_subestacao

/* codifo de substação antigo 2019
SELECT 'SE-' || NODE.OBJECTID AS "COD_ID",
'5697' AS "DIST",
DECODE(TERCIARIO.PAC1, NULL, (DECODE(PRIMARIO_SECUNDARIO.PAC1, NULL, 'D1-'||NODE.OBJECTID, 'SE-' || PRIMARIO_SECUNDARIO.PAC1)), 'SE-' || TERCIARIO.PAC1) AS "PAC_1",
DECODE(TERCIARIO.PAC2_FID, NULL, (DECODE(PRIMARIO_SECUNDARIO.PAC2_FID, NULL, 'D2-'||NODE.OBJECTID, 'SE-' || PRIMARIO_SECUNDARIO.PAC2_FID)), 'SE-' || TERCIARIO .PAC2_FID) AS "PAC_2",
DECODE(TERCIARIO.PAC3, NULL, 'D3-'||NODE.OBJECTID, 'SE-'||TERCIARIO.PAC3) AS "PAC_3",
'ABC' AS "FAS_CON_P",
'ABC' AS "FAS_CON_S",
DECODE(TERCIARIO.PAC3, null, '0', 'ABC') AS "FAS_CON_T",
TSITATI.COD_ID AS "SIT_ATIV",
'41' AS "TIP_UNID",
'PD' AS "POS",
'1' AS "ATRB_PER",
0 AS "TEN_LIN_SEC",
'0' AS "CAP_ELO",
'0' AS "CAP_CHA",
1 AS "TAP",
TARE.COD_ID AS "ARE_LOC",
'0' AS "CONF",
'0' AS "POSTO",
CAST(REPLACE(REPLACE(UPPER(S.POT_NOM), ' KVA',''),',','.') AS NUMBER) AS "POT_NOM",
CAST(REPLACE(S.PER_FER,',','.') AS NUMBER) AS "PER_FER",
CAST(REPLACE(S.PER_TOT,',','.') AS NUMBER) AS "PER_TOT",
S.DATA_CONEXAO AS "DAT_CON",
DECODE(NV.NOVO_ALIMENTADOR, NULL, TAT.CD_ALIMENTADOR, NV.NOVO_ALIMENTADOR) AS "CTMT",
DECODE(NV.UN_TR_S, NULL, DECODE(NODE.OBJECTID, NULL, '0', 'SE-' || NODE.OBJECTID), NV.UN_TR_S) as UNI_TR_S,
GENESIS.SUB AS "SUB",
GENESIS.CONJ AS "CONJ",
GENESIS.MUNICIPIO AS "MUN",
'0' AS "BANC",
'T' AS "TIP_TRAFO",
'0' AS "MRT",
S.LOCAL_INSTALACAO || ' - ' || S.POT_NOM || ' - ' || S.TEN_PRI || ' - '|| S.TEN_SEC AS "DESCR",
NODE.SHAPE AS "GEOMETRY"
FROM BDGD_STAGING.TAB_NODE_SUB NODE
LEFT JOIN
    (SELECT *FROM MV_BDGD_SAP
    WHERE BDGD='EQTRS') S
    ON NODE.LOC_INST_SAP = S.LOCAL_INSTALACAO
LEFT JOIN
    (SELECT ID_BAR_1,
         ID_BAR_2,
         ID_BAR_3,
         LOC_INST_SAP_TT
    FROM BDGD_STAGING.TAB_TT_BAR
    GROUP BY  ID_BAR_1, ID_BAR_2, ID_BAR_3, LOC_INST_SAP_TT) CON_SUB
    ON CON_SUB.LOC_INST_SAP_TT = S.LOCAL_INSTALACAO
LEFT JOIN TSITATI
    ON TSITATI.SIT_OPER = S.SIT_OPER
LEFT JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB GENESIS
    ON 'SE-' || GENESIS.NR_SUB = S.SUBESTACAO
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAT
    ON TAT.LOC_INST_SAP_ALI = NODE.ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
    ON NV.ALIMENTADOR = TAT.CD_ALIMENTADOR
LEFT JOIN TARE
    ON TARE.ARE_LOC = GENESIS.ARE_LOC
LEFT JOIN TTEN_PERDA
    ON TTEN_PERDA.TEN = S.TEN_PRI
LEFT JOIN TCLATEN
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN
    (SELECT PAC1_FID AS "PAC1",
         PAC2_FID AS "PAC2_FID",
         PAC2,
        PAC3_FID AS " PAC3"
    FROM
        (SELECT PAC1_FID,
         PAC2_FID,
        PAC2,
         LAG(PAC1_FID,
        1,
        0)
            OVER (PARTITION BY PAC2_FID
        ORDER BY  PAC2_FID) AS PAC3_FID
        FROM BDGD_STAGING.tab_bdgd_sub_conn
        WHERE PAC2 IN
            (SELECT LOCAL_INSTALACAO
            FROM MV_BDGD_SAP
            WHERE BDGD = 'EQTRS' ))
            WHERE PAC3_FID = '0') PRIMARIO_SECUNDARIO
            ON PRIMARIO_SECUNDARIO.PAC2 = s.local_instalacao
    LEFT JOIN
    (SELECT PAC1_FID AS "PAC1",
         PAC2_FID AS "PAC2_FID",
         PAC2,
        PAC3_FID AS "PAC3"
    FROM
        (SELECT PAC1_FID,
         PAC2_FID,
        PAC2,
         LAG(PAC1_FID,
        1,
        0)
            OVER (PARTITION BY PAC2_FID
        ORDER BY  PAC2_FID) AS PAC3_FID
        FROM BDGD_STAGING.tab_bdgd_sub_conn
        WHERE PAC2 IN
            (SELECT LOCAL_INSTALACAO
            FROM MV_BDGD_SAP
            WHERE BDGD = 'EQTRS' ))
            WHERE PAC3_FID <> '0') TERCIARIO
            ON TERCIARIO.PAC2 = s.local_instalacao
    WHERE S.BDGD = 'EQTRS'
        AND S.LOCAL_INSTALACAO LIKE 'SE%'
        AND TCLATEN.COD_ID < 11;*/
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_ARAT" ("OBJECTID", "SHAPE", "COD_ID", "DIST", "FUN_PR", "FUN_TE", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "SHAPE"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "SHAPE"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  WITH PRIMARY KEY USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT "BDGD2019_ARAT"."OBJECTID" "OBJECTID","BDGD2019_ARAT"."SHAPE" "SHAPE","BDGD2019_ARAT"."COD_ID" "COD_ID","BDGD2019_ARAT"."DIST" "DIST","BDGD2019_ARAT"."FUN_PR" "FUN_PR","BDGD2019_ARAT"."FUN_TE" "FUN_TE","BDGD2019_ARAT"."DESCR" "DESCR" FROM "BDGD_STAGING"."BDGD2019_ARAT" "BDGD2019_ARAT"
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQTRD_2" ("COD_ID", "DIST", "PAC_1", "PAC_2", "PAC_3", "CLAS_TEN", "POT_NOM", "LIG", "FAS_CON", "TEN_PRI", "TEN_SEC", "TEN_TER", "LIG_FAS_P", "LIG_FAS_S", "LIG_FAS_T", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "SITCONT", "DAT_IMO", "PER_FER", "PER_TOT", "R", "XHL", "XHT", "XLT", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT S.EQUIPAMENTO AS "COD_ID",
'5697' AS "DIST",
CASE
WHEN GENESIS.PAC1 is null or GENESIS.PAC1=0 THEN 'D1-'||S.EQUIPAMENTO
ELSE CAST(GENESIS.PAC1 AS VARCHAR2(20)) END AS PAC_1,
CASE
WHEN GENESIS.PAC2 is null or GENESIS.PAC2=0 THEN 'D2-'||S.EQUIPAMENTO
ELSE CAST(GENESIS.PAC2 AS VARCHAR2(20)) END AS PAC_2,
'D3-'||S.EQUIPAMENTO AS "PAC_3",
TCLATEN.COD_ID AS "CLAS_TEN",
TPOTAPRT.COD_ID AS "POT_NOM",
DECODE(GENESIS.FAS_CON_P, 'ABC', '2','6') AS "LIG",
SUGERE.FAS_CON_P AS "FAS_CON",
NVL(T1.COD_ID,0) AS "TEN_PRI",
--NVL(T2.COD_ID,0) AS "TEN_SEC",
DECODE(LENGTH(SUGERE.FAS_CON_S), 2, '17', 4, DECODE(T2.COD_ID, '10', '10', '15'), -1) TEN_SEC,
'0' AS "TEN_TER",
SUGERE.FAS_CON_P AS "LIG_FAS_P",
SUGERE.FAS_CON_S AS "LIG_FAS_S",
SUGERE.FAS_CON_T  AS "LIG_FAS_T",
DECODE(GENESIS.TIPO, '1', '190400', '2', '190400', '190401') AS "ODI",
DECODE((GENESIS.TIPO||GENESIS.ARE_LOC), '1U', '40',
                                '1R', '41',
                                '2U', '40',
                                '2R', '41',
                                '3R', '43',
                                '4R', '43',
                                '5R', '43',
                                '6R', '43',
                                '3U', '42',
                                '4U', '42',
                                '5U', '42',
                                '6U', '42') AS "TI",
NVL(S.CENTRO_MODULAR, '999') AS "CM",
NVL(S.TUC, '000') AS "TUC",
NVL(S.A1, '00') AS "A1",
NVL(S.A2, '00') AS "A2",
NVL(S.A3, '00') AS "A3",
NVL(S.A4, '00') AS "A4",
NVL(S.A5, '00') AS "A5",
NVL(S.A6, '00') AS "A6",
DECODE(S.DATA_IMOB, NULL, 'NIM', 'AT1') AS "SITCONT",
S.DATA_IMOB AS "DAT_IMO",
TRUNC(REPLACE(S.PER_FER, ',','.'),3) AS "PER_FER",
TRUNC(REPLACE(S.PER_TOT,',','.'),3) AS "PER_TOT",
TRUNC(REPLACE(S.R,',','.'),3) AS "R",
TRUNC(REPLACE(S.XHL,',','.'),3) AS "XHL",
CAST(NULL AS VARCHAR2(1)) AS "XHT",
CAST(NULL AS VARCHAR2(1)) AS "XLT",
S.LOCAL_INSTALACAO AS "DESCR"
FROM MV_BDGD_SAP S
LEFT JOIN BDGD_STAGING.MV_GENESIS_EQTRD GENESIS
ON GENESIS.EQUIPAMENTO = S.LOCAL_INSTALACAO
LEFT JOIN TCLATEN
ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TPOTAPRT
ON TPOTAPRT.POT = S.POT_NOM
--LEFT JOIN TLIG
--ON TLIG.DESCR = S.LIG
--LEFT JOIN TFASCON_AT
--ON TFASCON_AT.FASES = GENESIS.FAS_CON_P
--LEFT JOIN TFASCON_BT
--ON TFASCON_BT.FASES = GENESIS.FAS_CON_S
LEFT JOIN TTEN T1 ON T1.TEN = S.TEN_PRI
LEFT JOIN TTEN T2 ON T2.TEN = S.TEN_SEC
LEFT JOIN (SELECT
 DISTINCT FEATURE_ID,
          FASE_TRAFO_PRIMARIO AS "FAS_CON_P",
          FASE_TRAFO_SECUNDARIO AS "FAS_CON_S",
          FASE_TRAFO_TERCIARIO AS "FAS_CON_T"
 FROM BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT )SUGERE ON SUGERE.FEATURE_ID=GENESIS.ID
where s.bdgd = 'EQTRD'
AND (S.LOCAL_INSTALACAO LIKE 'RD%' OR S.LOCAL_INSTALACAO LIKE 'O-%')

UNION ALL

SELECT S.EQUIPAMENTO AS "COD_ID",
       '5697' AS "DIST",
       DECODE(TERCIARIO.PAC1, NULL, (DECODE(PRIMARIO_SECUNDARIO.PAC1, NULL, 'D1-'||S.EQUIPAMENTO, 'SE-' || PRIMARIO_SECUNDARIO.PAC1)), 'SE-' || TERCIARIO.PAC1) AS "PAC_1",
       DECODE(TERCIARIO.PAC2_FID, NULL, (DECODE(PRIMARIO_SECUNDARIO.PAC2_FID, NULL, 'D2-'||S.EQUIPAMENTO, 'SE-' || PRIMARIO_SECUNDARIO.PAC2_FID)), 'SE-' || TERCIARIO.PAC2_FID) AS "PAC_2",
       DECODE(TERCIARIO.PAC3, NULL, 'D3-'||S.EQUIPAMENTO, 'SE-'||TERCIARIO.PAC3) AS "PAC_3",
       TCLATEN.COD_ID AS "CLAS_TEN",
       TPOTAPRT.COD_ID AS "POT_NOM",
       TLIG.COD_ID AS "LIG",
       'ABC' AS "FAS_CON",
       T1.COD_ID AS "TEN_PRI",
       T2.COD_ID AS "TEN_SEC",
       NVL(T3.COD_ID, '0') AS "TEN_TER",
       'ABC' AS "LIG_FAS_P",
       'ABC' AS "LIG_FAS_S",
       '0' AS "LIG_FAS_T",
        S.ODI AS "ODI",
        S.TI AS "TI",
        S.CENTRO_MODULAR AS "CM",
        S.TUC AS "TUC",
        NVL(S.A1,'00') AS "A1",
        NVL(S.A2, '00') AS "A2",
        NVL(S.A3, '00') AS "A3",
        NVL(S.A4, '00') AS "A4",
        NVL(S.A5, '00') AS "A5",
        NVL(S.A6, '00') AS "A6",
        DECODE(S.DATA_IMOB, NULL, 'NIM', 'AT1') AS "SITCONT",
        S.DATA_IMOB AS "DAT_IMO",
        TRUNC(REPLACE(S.PER_FER, ',','.'),3) AS "PER_FER",
        TRUNC(REPLACE(S.PER_TOT,',','.'),3) AS "PER_TOT",
        TRUNC(REPLACE(S.R,',','.'),3) AS "R",
        TRUNC(REPLACE(S.XHL,',','.'),3) AS "XHL",
        CAST(NULL AS VARCHAR2(1)) AS "XHT",
        CAST(NULL AS VARCHAR2(1)) AS "XLT",
       S.local_instalacao AS "DESCR"
FROM MV_BDGD_SAP S
     LEFT JOIN TCLATEN
          ON TCLATEN.TEN = S.CLAS_TEN
     LEFT JOIN TPOTAPRT
          ON TPOTAPRT.POT = S.POT_NOM
     LEFT JOIN TLIG
          ON TLIG.DESCR = S.LIG
     LEFT JOIN TTEN T1
          ON T1.TEN = S.TEN_PRI
     LEFT JOIN TTEN T2
          ON T2.TEN = S.TEN_SEC
     LEFT JOIN TTEN T3
          ON T3.TEN = S.TEN_TER
     LEFT JOIN (SELECT PAC1_FID AS "PAC1", PAC2_FID AS "PAC2_FID", PAC2,PAC3_FID AS "PAC3" FROM (
                    SELECT PAC1_FID, PAC2_FID,PAC2,
                    LAG(PAC1_FID,1,0) over (PARTITION BY PAC2_FID order by PAC2_FID) AS PAC3_FID
                    FROM BDGD_STAGING.tab_bdgd_sub_conn
                    WHERE PAC2 IN (SELECT LOCAL_INSTALACAO FROM MV_BDGD_SAP WHERE BDGD = 'EQTRS' ))
                where PAC3_FID = '0') PRIMARIO_SECUNDARIO
    ON PRIMARIO_SECUNDARIO.PAC2 = s.local_instalacao
     LEFT JOIN (SELECT PAC1_FID AS "PAC1", PAC2_FID AS "PAC2_FID", PAC2,PAC3_FID AS "PAC3" FROM (
                    SELECT PAC1_FID, PAC2_FID,PAC2,
                    LAG(PAC1_FID,1,0) over (PARTITION BY PAC2_FID order by PAC2_FID) AS PAC3_FID
                    FROM BDGD_STAGING.tab_bdgd_sub_conn
                    WHERE PAC2 IN (SELECT LOCAL_INSTALACAO FROM MV_BDGD_SAP WHERE BDGD = 'EQTRS' ))
                where PAC3_FID <> '0') TERCIARIO
    ON TERCIARIO.PAC2 = s.local_instalacao
where s.bdgd = 'EQTRS'
AND S.LOCAL_INSTALACAO LIKE 'SE%'
AND TCLATEN.COD_ID < 11
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_PNT" ("COD_ID", "DIST", "SUB_GRP", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND NEXT null
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
PNT.COD_ID,
PNT.DIST,
PNT.CATEG AS "SUB_GRP",
NVL(PNT.ENE_01, 0) AS "ENE_01",
NVL(PNT.ENE_02, 0) AS "ENE_02",
NVL(PNT.ENE_03, 0) AS "ENE_03",
NVL(PNT.ENE_04, 0) AS "ENE_04",
NVL(PNT.ENE_05, 0) AS "ENE_05",
NVL(PNT.ENE_06, 0) AS "ENE_06",
NVL(PNT.ENE_07, 0) AS "ENE_07",
NVL(PNT.ENE_08, 0) AS "ENE_08",
NVL(PNT.ENE_09, 0) AS "ENE_09",
NVL(PNT.ENE_10, 0) AS "ENE_10",
NVL(PNT.ENE_11, 0) AS "ENE_11",
NVL(PNT.ENE_12, 0) AS "ENE_12",
PNT.DESCR
FROM BDGD_STAGING.TAB_PERTEC_PNT PNT
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNREMT_BKP" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "PAC_1", "PAC_2", "UNI_TR_S", "CTMT", "SUB", "CONJ", "MUN", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
SELECT
CAST (GEN.COD_ID as VARCHAR(20)) as COD_ID,
GEN.DIST,
GEN.FAS_CON,
GEN.SIT_ATIV,
GEN.TIP_UNID,
CASE 
WHEN GEN.PAC1 IS NULL THEN '0'
ELSE CAST (GEN.PAC1  as VARCHAR(20))END  as PAC_1,
CASE 
WHEN GEN.PAC2 IS NULL THEN '0'
ELSE CAST (GEN.PAC2  as VARCHAR(20))END  as PAC_2,
DECODE(NV.UN_TR_S, NULL, 'SE-'||BAR.ID_TT, NV.UN_TR_S) as UNI_TR_S,
DECODE(NV.NOVO_ALIMENTADOR, NULL, GEN.CTMT, NV.NOVO_ALIMENTADOR) AS "CTMT",
CAST (GEN.SUB as VARCHAR(20)) as SUB,
GEN.CONJ,
GEN.MUN,
TO_CHAR(TO_DATE(SAP.DAT_CON, 'DD/MM/RR'), 'DD/MM/YYYY') AS DAT_CON,
GEN.BANC,
'PD' as POS,
GEN.DESCR,
GEN.GEOMETRY
FROM BDGD_STAGING.MV_GENESIS_UNREMT GEN 
INNER JOIN 
(
SELECT LOCAL_INSTALACAO, 
MAX (
CASE 
  WHEN SAP.DATA_CONEXAO='00/00/0000' THEN TO_DATE('01/01/0001','YYYY-MM-DD') 
  ELSE TO_DATE(SAP.DATA_CONEXAO,'DD/MM/YYYY')END 
  )as  DAT_CON
FROM MV_BDGD_SAP SAP
WHERE
   OBJ_TECNICO in ('RG', 'TRT') AND  
   LOCAL_INSTALACAO is not null AND
   SAP.A2<=34
GROUP BY LOCAL_INSTALACAO
) SAP ON SAP.LOCAL_INSTALACAO = GEN.DESCR
INNER JOIN
( SELECT * FROM BDGD_STAGING.MV_GENESIS_UNREMT)  GEN ON  GEN.DESCR = SAP.LOCAL_INSTALACAO
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT BAR ON BAR.CD_ALIMENTADOR=GEN.CD_ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV
ON NV.ALIMENTADOR = GEN.CD_ALIMENTADOR
)

UNION ALL

(
SELECT 
SUBST.COD_ID,
SUBST.DIST,
SUBST.FAS_CON,
SUBST.SIT_ATIV,
SUBST.TIP_UNID,
SUBST.PAC_1,
SUBST.PAC_2,
SUBST.UNI_TR_S,
SUBST.CTMT,
SUBST.SUB,
SUBST.CONJ,
SUBST.MUN,
SUBST.DAT_CON,
SUBST.BANC,
SUBST.POS,
SUBST.DESCR,
GEO.GEOMETRY
FROM
(
SELECT
DISTINCT
'SE-'||NS.OBJECTID as COD_ID,
5697 as DIST,
'ABC'  as FAS_CON,
'AT'  as SIT_ATIV,
14 as TIP_UNID,
CASE 
WHEN SUB.PAC1_FID is null THEN '0'
ELSE 'SE-'||SUB.PAC1_FID END as PAC_1,
CASE 
WHEN SUB.PAC2_FID is null THEN '0'
ELSE 'SE-'||SUB.PAC2_FID END as PAC_2,
DECODE(NODE2.OBJECTID,NULL, '0', 'SE-' || NODE2.OBJECTID) AS UNI_TR_S, 
DECODE(NV.NOVO_ALIMENTADOR, NULL, BAR.CD_ALIMENTADOR, NV.NOVO_ALIMENTADOR) AS "CTMT",
GEN.SUB,
GEN.CONJ,
GEN.MUNICIPIO as MUN,
TO_CHAR(TO_DATE(DT_CNX.DAT_CON, 'DD/MM/RR'), 'DD/MM/YYYY') AS DAT_CON,
1 as BANC,
'PD' as POS,
SAP.LOCAL_INSTALACAO as DESCR,
SUB.PAC2
FROM
MV_BDGD_SAP SAP 
INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN SUB         ON SAP.LOCAL_INSTALACAO=SUB.PAC2
INNER JOIN BDGD_STAGING.TAB_NODE_SUB NS          ON NS.LOC_INST_SAP=SUB.PAC2
INNER JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB GEN ON  'SE-'||GEN.NR_SUB = SAP.SUBESTACAO
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT  BAR ON BAR.LOC_INST_SAP_ALI = NS.ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2 ON NODE2.LOC_INST_SAP = NS.UN_TR_S
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV ON NV.ALIMENTADOR = BAR.CD_ALIMENTADOR
LEFT JOIN
(
 SELECT LOCAL_INSTALACAO, 
    MAX (
    CASE 
    WHEN SAP.DATA_CONEXAO='00/00/0000' THEN TO_DATE('01/01/0001','YYYY-MM-DD') 
    ELSE TO_DATE(SAP.DATA_CONEXAO,'DD/MM/YYYY')END 
  )as  DAT_CON
  FROM MV_BDGD_SAP SAP
  WHERE
   OBJ_TECNICO in ('RG', 'TRT') AND  
   LOCAL_INSTALACAO is not null AND
   SAP.A2<=34
  GROUP BY LOCAL_INSTALACAO
)  DT_CNX ON DT_CNX.LOCAL_INSTALACAO=SAP.LOCAL_INSTALACAO
WHERE
OBJ_TECNICO in ('RG','TRT') AND
SAP.A2<60
) SUBST
INNER JOIN  
(SELECT NS.LOC_INST_SAP,NS.SHAPE as GEOMETRY FROM BDGD_STAGING.TAB_NODE_SUB NS ) GEO ON GEO.LOC_INST_SAP=SUBST.PAC2
)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNTRS_BKP" ("COD_ID", "SUB", "BARR_1", "BARR_2", "BARR_3", "PAC_1", "PAC_2", "PAC_3", "DIST", "FAS_CON_P", "FAS_CON_S", "FAS_CON_T", "SIT_ATIV", "TIP_UNID", "POS", "ARE_LOC", "POT_NOM", "POT_F01", "POT_F02", "PER_FER", "PER_TOT", "BANC", "DAT_CON", "CONJ", "MUN", "TIP_TRAFO", "ALOC_PERD", "ENES_01", "ENES_02", "ENES_03", "ENES_04", "ENES_05", "ENES_06", "ENES_07", "ENES_08", "ENES_09", "ENES_10", "ENES_11", "ENES_12", "ENET_01", "ENET_02", "ENET_03", "ENET_04", "ENET_05", "ENET_06", "ENET_07", "ENET_08", "ENET_09", "ENET_10", "ENET_11", "ENET_12", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT 'SE-' || NODE.OBJECTID AS "COD_ID", 
GENESIS.SUB AS "SUB", 
DECODE(CON_SUB.ID_BAR_1, 0, '0', 'SE-' || CON_SUB.ID_BAR_1) AS "BARR_1", 
DECODE(CON_SUB.ID_BAR_2, 0, '0', 'SE-' || CON_SUB.ID_BAR_2) AS "BARR_2", 
DECODE(CON_SUB.ID_BAR_3, 0, '0','SE-' || CON_SUB.ID_BAR_3) AS "BARR_3", 
DECODE(TERCIARIO.PAC1, NULL, (DECODE(PRIMARIO_SECUNDARIO.PAC1, NULL, 'D1-'||NODE.OBJECTID, 'SE-' || PRIMARIO_SECUNDARIO.PAC1)), 'SE-' || TERCIARIO.PAC1) AS "PAC_1", 
DECODE(TERCIARIO.PAC2_FID, NULL, (DECODE(PRIMARIO_SECUNDARIO.PAC2_FID, NULL, 'D2-'||NODE.OBJECTID, 'SE-' || PRIMARIO_SECUNDARIO.PAC2_FID)), 'SE-' || TERCIARIO .PAC2_FID) AS "PAC_2", 
DECODE(TERCIARIO.PAC3, NULL, 'D3-'||NODE.OBJECTID, 'SE-'||TERCIARIO.PAC3) AS "PAC_3", 
'5697' AS "DIST", 
'ABC' AS "FAS_CON_P", 
'ABC' AS "FAS_CON_S", 
DECODE(TERCIARIO.PAC3, null, '0', 'ABC') AS "FAS_CON_T", 
TSITATI.COD_ID AS "SIT_ATIV", 
'41' AS "TIP_UNID", 
'PD' AS "POS", 
TARE.COD_ID AS "ARE_LOC", 
replace(S.POT_NOM, ',', '.')/1000 AS "POT_NOM", 
replace(S.POT_F01, ',', '.')/1000 AS "POT_F01", 
replace(S.POT_F02, ',', '.')/1000 AS "POT_F02", 
TRUNC((replace(S.PER_FER, ',', '.')/replace(S.POT_NOM, ',', '.'))/10,3) AS "PER_FER", 
TRUNC((replace(S.PER_TOT, ',', '.')/replace(S.POT_NOM, ',', '.'))/10,3) AS "PER_TOT", 
0 AS "BANC", 
S.DATA_CONEXAO AS "DAT_CON", 
GENESIS.CONJ AS "CONJ", 
GENESIS.MUNICIPIO AS "MUN", 
'T' AS "TIP_TRAFO", 
TTEN_PERDA.COD_ID AS "ALOC_PERD", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_1_PER_KWH, 0) AS "ENES_01", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_2_PER_KWH, 0) AS "ENES_02", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_3_PER_KWH, 0) AS "ENES_03", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_4_PER_KWH, 0) AS "ENES_04", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_5_PER_KWH, 0) AS "ENES_05", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_6_PER_KWH, 0) AS "ENES_06", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_7_PER_KWH, 0) AS "ENES_07", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_8_PER_KWH, 0) AS "ENES_08", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_9_PER_KWH, 0) AS "ENES_09", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_10_PER_KWH, 0) AS "ENES_10", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_11_PER_KWH, 0) AS "ENES_11", 
NVL(ENERGIA.ENERGIA_ATIVA_SEC_12_PER_KWH, 0) AS "ENES_12", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_1_PER_KWH, 0) AS "ENET_01", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_2_PER_KWH, 0) AS "ENET_02", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_3_PER_KWH, 0) AS "ENET_03", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_4_PER_KWH, 0) AS "ENET_04", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_5_PER_KWH, 0) AS "ENET_05", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_6_PER_KWH, 0) AS "ENET_06", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_7_PER_KWH, 0) AS "ENET_07", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_8_PER_KWH, 0) AS "ENET_08", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_9_PER_KWH, 0) AS "ENET_09", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_10_PER_KWH, 0) AS "ENET_10", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_11_PER_KWH, 0) AS "ENET_11", 
NVL(ENERGIA.ENERGIA_ATIVA_TER_12_PER_KWH, 0) AS "ENET_12", 
S.LOCAL_INSTALACAO AS "DESCR", 
NODE.SHAPE AS "GEOMETRY"
FROM BDGD_STAGING.TAB_NODE_SUB NODE
LEFT JOIN 
    (SELECT *FROM MV_BDGD_SAP
    WHERE BDGD='EQTRS') S
    ON NODE.LOC_INST_SAP = S.LOCAL_INSTALACAO
LEFT JOIN 
    (SELECT ID_BAR_1,
         ID_BAR_2,
         ID_BAR_3,
         LOC_INST_SAP_TT
    FROM BDGD_STAGING.TAB_TT_BAR
    GROUP BY  ID_BAR_1, ID_BAR_2, ID_BAR_3, LOC_INST_SAP_TT) CON_SUB
    ON CON_SUB.LOC_INST_SAP_TT = S.LOCAL_INSTALACAO
LEFT JOIN TSITATI
    ON TSITATI.SIT_OPER = S.SIT_OPER
LEFT JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB GENESIS
    ON 'SE-' || GENESIS.NR_SUB = S.SUBESTACAO
LEFT JOIN TARE
    ON TARE.ARE_LOC = GENESIS.ARE_LOC
LEFT JOIN BDGD_STAGING.TAB_ENERGIA_UNTRS ENERGIA
    ON ENERGIA.EQUIP = S.EQUIPAMENTO
LEFT JOIN TTEN_PERDA
    ON TTEN_PERDA.TEN = S.TEN_PRI
LEFT JOIN TCLATEN
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN 
    (SELECT PAC1_FID AS "PAC1",
         PAC2_FID AS "PAC2_FID",
         PAC2,
        PAC3_FID AS " PAC3"
    FROM 
        (SELECT PAC1_FID,
         PAC2_FID,
        PAC2,
         LAG(PAC1_FID,
        1,
        0)
            OVER (PARTITION BY PAC2_FID
        ORDER BY  PAC2_FID) AS PAC3_FID
        FROM BDGD_STAGING.tab_bdgd_sub_conn
        WHERE PAC2 IN 
            (SELECT LOCAL_INSTALACAO
            FROM MV_BDGD_SAP
            WHERE BDGD = 'EQTRS' ))
            WHERE PAC3_FID = '0') PRIMARIO_SECUNDARIO
            ON PRIMARIO_SECUNDARIO.PAC2 = s.local_instalacao
    LEFT JOIN 
    (SELECT PAC1_FID AS "PAC1",
         PAC2_FID AS "PAC2_FID",
         PAC2,
        PAC3_FID AS "PAC3"
    FROM 
        (SELECT PAC1_FID,
         PAC2_FID,
        PAC2,
         LAG(PAC1_FID,
        1,
        0)
            OVER (PARTITION BY PAC2_FID
        ORDER BY  PAC2_FID) AS PAC3_FID
        FROM BDGD_STAGING.tab_bdgd_sub_conn
        WHERE PAC2 IN 
            (SELECT LOCAL_INSTALACAO
            FROM MV_BDGD_SAP
            WHERE BDGD = 'EQTRS' ))
            WHERE PAC3_FID <> '0') TERCIARIO
            ON TERCIARIO.PAC2 = s.local_instalacao
    WHERE S.BDGD = 'EQTRS'
        AND S.LOCAL_INSTALACAO LIKE 'SE%'
        AND TCLATEN.COD_ID >= 11
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQCR" ("COD_ID", "SUB", "DIST", "PAC_1", "PAC_2", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (SELECT CAST(SAP.EQUIPAMENTO AS VARCHAR(20)) AS COD_ID,
         GEN.SUB,
         5697 AS DIST,
        CASE
        WHEN GEN.PAC_1 is NULL
            OR GEN.PAC_1='0' THEN
        'D1-'||SAP.EQUIPAMENTO
        ELSE CAST(GEN.PAC_1 AS VARCHAR(20))END AS PAC_1,
        CASE
        WHEN GEN.PAC_1 is NULL
            OR GEN.PAC_1='0' THEN
        'D2-'||SAP.EQUIPAMENTO
        ELSE CAST(GEN.PAC_2 AS VARCHAR(20))END AS PAC_2, ODI,
        CASE
        WHEN GEN.TI is NULL THEN
        '0'
        ELSE CAST(GEN.TI AS VARCHAR(20))END AS TI, 
        CENTRO_MODULAR AS CM, 
        TUC, 
        A1, 
        A2, 
        A3, 
        A4, 
        COALESCE(A5,'00') AS A5, 
        COALESCE(A6,'00') AS A6, 
        IDUC, 
        DECODE(SAP.DATA_IMOB, NULL, 'NIM', 'AT1') AS SITCONT,
        SAP.DATA_IMOB AS DAT_IMO, 
        GEN.DESCR
    FROM MV_BDGD_SAP SAP
    INNER JOIN BDGD_STAGING.MV_GENESIS_V_BDGD_EQCR GEN
        ON SAP.LOCAL_INSTALACAO = GEN.COD_ID
    WHERE SAP.BDGD = 'EQCR'
            AND SAP.LOCAL_INSTALACAO LIKE 'RD%' )
UNION
( 
SELECT CAST (SAP.EQUIPAMENTO AS VARCHAR(20)) AS COD_ID,
         VSSBT.SG_SUBESTACAO AS SUB,
         5697 AS DIST,        
    CASE
    WHEN TSC.NR_PTO_ELET_INICIO is NULL
        OR TSC.NR_PTO_ELET_INICIO='0' THEN
    'D1-'||SAP.EQUIPAMENTO
    ELSE 'SE-'||TSC.NR_PTO_ELET_INICIO
    END AS PAC_1,
    CASE
    WHEN TSC.NR_PTO_ELET_FIM is NULL
        OR TSC.NR_PTO_ELET_FIM='0' THEN
    'D2-'||SAP.EQUIPAMENTO
    WHEN TSC.NR_PTO_ELET_FIM is NULL THEN
    '0'
    ELSE 'SE-'||TSC.NR_PTO_ELET_FIM
    END AS PAC_2, ODI, COALESCE(TI,'0') AS TI, 
    CENTRO_MODULAR AS CM, 
    TUC, 
    A1, 
    A2, 
    A3, 
    A4, 
    COALESCE(A5,'00') AS A5, 
    COALESCE(A6,'00') AS A6, 
    IDUC, 
    DECODE(SAP.DATA_IMOB, NULL, 'NIM', 'AT1') AS SITCONT,
    SAP.DATA_IMOB AS DAT_IMO, 
    SAP.LOCAL_INSTALACAO AS DESCR
FROM MV_BDGD_SAP SAP
LEFT JOIN GEN_BDGD2019.MV_EQPTOS_SUBESTACAO TSC
    ON TSC.LOCAL_INST=SAP.LOCAL_INSTALACAO AND TSC.TIPO_EQPTO='BC'AND TSC.ID_TN_NOMINAL_EQPTO<6
LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO VSSBT
    ON 'SE-0000'||VSSBT.NR_SUBESTACAO=SAP.SUBESTACAO
WHERE 
SAP.bdgd = 'EQCR'
AND SAP.LOCAL_INSTALACAO LIKE 'SE-0%' 
        )
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNREMT" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "PAC_1", "PAC_2", "UNI_TR_S", "CTMT", "SUB", "CONJ", "MUN", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
SELECT
CAST (GEN.COD_ID as VARCHAR(20)) as COD_ID,
GEN.DIST,
GEN.FAS_CON,
GEN.SIT_ATIV,
GEN.TIP_UNID,
CASE 
WHEN GEN.PAC1 IS NULL THEN '0'
ELSE CAST (GEN.PAC1  as VARCHAR(20))END  as PAC_1,
CASE 
WHEN GEN.PAC2 IS NULL THEN '0'
ELSE CAST (GEN.PAC2  as VARCHAR(20))END  as PAC_2,
COALESCE(NV.UN_TR_S,BAR.LOC_INST_SAP_TT,'0') AS "UNI_TR_S", 
--DECODE(NV.UN_TR_S, NULL, 'SE-'||BAR.ID_TT, NV.UN_TR_S) as UNI_TR_S,
COALESCE(NV.NOVO_ALIMENTADOR,GEN.CTMT,'0') AS "CTMT",
--DECODE(NV.NOVO_ALIMENTADOR, NULL, GEN.CTMT, NV.NOVO_ALIMENTADOR) AS "CTMT",
CAST (GEN.SUB as VARCHAR(20)) as SUB,
GEN.CONJ,
GEN.MUN,
TO_CHAR(TO_DATE(SAP.DAT_CON, 'DD/MM/RR'), 'DD/MM/YYYY') AS DAT_CON,
GEN.BANC,
'PD' as POS,
GEN.DESCR,
GEN.GEOMETRY
FROM BDGD_STAGING.MV_GENESIS_UNREMT GEN 
INNER JOIN 
(
SELECT LOCAL_INSTALACAO, 
       MAX(SAP.DATA_CONEXAO) DAT_CON
/*MAX (
CASE 
  WHEN SAP.DATA_CONEXAO='00/00/0000' THEN TO_DATE('01/01/0001','YYYY-MM-DD') 
  ELSE TO_DATE(SAP.DATA_CONEXAO,'DD/MM/YYYY')END 
  )as  DAT_CON
  */
FROM MV_BDGD_SAP SAP
WHERE
   OBJ_TECNICO in ('RG', 'TRT') AND  
   LOCAL_INSTALACAO is not null AND
   SAP.A2<=34
GROUP BY LOCAL_INSTALACAO
) SAP ON SAP.LOCAL_INSTALACAO = GEN.DESCR
INNER JOIN ( SELECT * FROM BDGD_STAGING.MV_GENESIS_UNREMT)  GEN ON  GEN.DESCR = SAP.LOCAL_INSTALACAO 
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT BAR ON BAR.CD_ALIMENTADOR=GEN.CD_ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV ON NV.ALIMENTADOR = GEN.CD_ALIMENTADOR
)UNION ALL
--SUBESTACAO
(
SELECT 
SUBST.COD_ID,
SUBST.DIST,
SUBST.FAS_CON,
SUBST.SIT_ATIV,
SUBST.TIP_UNID,
coalesce(p.pac_1, SUBST.PAC_1) PAC_1,
coalesce(p.pac_2, SUBST.PAC_2) PAC_2,
--SUBST.PAC_2,
SUBST.UNI_TR_S,
SUBST.CTMT,
SUBST.SUB,
SUBST.CONJ,
SUBST.MUN,
SUBST.DAT_CON,
SUBST.BANC,
SUBST.POS,
SUBST.DESCR,
GEO.SUB_GEOMETRY GEOMETRY
FROM
(--INI
SELECT
DISTINCT
''||SUB.ID as COD_ID,
5697 as DIST,
'ABC'  as FAS_CON,
'AT'  as SIT_ATIV,
14 as TIP_UNID,
CASE 
WHEN SUB.NR_PTO_ELET_INICIO is null THEN '0'
ELSE 'SE-'||SUB.NR_PTO_ELET_INICIO END as PAC_1,
CASE 
WHEN SUB.NR_PTO_ELET_FIM is null THEN '0'
ELSE 'SE-'||SUB.NR_PTO_ELET_FIM END as PAC_2,
COALESCE(N_ALIM.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
--DECODE(SUB.LOCAL_INST_TT,NULL, '0', '' || SUB.LOCAL_INST_TT) AS UNI_TR_S, 
COALESCE(N_ALIM.NOVO_ALIMENTADOR,DECODE(SUB.ID_TIPO_MODULO,1,'0',SUB.CD_ALIMENTADOR),'0') AS "CTMT",
--DECODE(NV.NOVO_ALIMENTADOR, NULL, SUB.CD_ALIMENTADOR, NV.NOVO_ALIMENTADOR) AS "CTMT",
SUB.SUB,
SUB.CONJ,
SUB.MUNICIPIO as MUN,
--TO_CHAR(TO_DATE(DT_CNX.DAT_CON, 'DD/MM/RR'), 'DD/MM/YYYY') AS DAT_CON,
SAP.DATA_CONEXAO DAT_CON,
1 as BANC,
'PD' as POS,
SAP.LOCAL_INSTALACAO as DESCR
--SUB.SUB_GEOMETRY GEOMETRY
--SUB.PAC2FROM
FROM
(SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto = 'RG' and id_tn_nominal_eqpto < '6') SUB
LEFT JOIN (SELECT * FROM (SELECT S.*, RANK()  OVER (PARTITION BY LOCAL_INSTALACAO ORDER BY DATA_CONEXAO) RA FROM MV_BDGD_SAP S WHERE OBJ_TECNICO in ('RG','TRT')) WHERE RA = 1)  SAP 
    ON SAP.LOCAL_INSTALACAO = SUB.LOCAL_INST
/*INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN SUB         ON SAP.LOCAL_INSTALACAO=SUB.PAC2
INNER JOIN BDGD_STAGING.TAB_NODE_SUB NS          ON NS.LOC_INST_SAP=SUB.PAC2
INNER JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB GEN ON  'SE-'||GEN.NR_SUB = SAP.SUBESTACAO
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT  BAR ON BAR.LOC_INST_SAP_ALI = NS.ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_NODE_SUB NODE2 ON NODE2.LOC_INST_SAP = NS.UN_TR_S*/
--LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NV ON NV.ALIMENTADOR = SUB.CD_ALIMENTADOR
LEFT JOIN (SELECT distinct substr(alimentador,1,3) as SUB, novo_alimentador,un_tr_s FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) N_ALIM ON N_ALIM.SUB = SUB.SUB
/*LEFT JOIN
(
 SELECT LOCAL_INSTALACAO, 
    /*MAX (
    CASE 
    WHEN SAP.DATA_CONEXAO='00/00/0000' THEN TO_DATE('01/01/0001','YYYY-MM-DD') 
    ELSE TO_DATE(SAP.DATA_CONEXAO,'DD/MM/YYYY')END 
  )as  DAT_CON*/
/*  MAX(SAP.DATA_CONEXAO) DAT_CON 
  FROM MV_BDGD_SAP SAP
  WHERE
   OBJ_TECNICO in ('RG', 'TRT') AND  
   LOCAL_INSTALACAO is not null --AND
   --SAP.A2<=34
  GROUP BY LOCAL_INSTALACAO
)  DT_CNX ON DT_CNX.LOCAL_INSTALACAO=SAP.LOCAL_INSTALACAO*/
--WHERE
--OBJ_TECNICO in ('RG','TRT') 
--AND SAP.A2<60
) SUBST --FIM
LEFT JOIN  
GEN_BDGD2019.MV_EQPTOS_SUBESTACAO GEO ON GEO.LOCAL_INST=SUBST.DESCR
left join tab_aux_pac_se_re p
    on p.loc_inst = descr
)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQME" ("COD_ID", "PAC", "DIST", "TIP_UNID", "FAS_CON", "TIPMED", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "SITCONT", "DAT_IMO", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT 
  CAST(EM.cod_id AS VARCHAR(20)) AS COD_ID,
    CASE
    WHEN VWEQ.PAC IS NULL
        OR VWEQ.PAC = 0 
        THEN Replace(EM.COD_ID, '0001-', 'DESC-')
    ELSE Cast('BT-' ||VWEQ.PAC AS VARCHAR2(20 byte))
    END AS PAC, 
    CAST(EM.dist AS NUMBER(38, 0)) AS DIST,
    CAST(EM.tip_unid AS VARCHAR2(3 byte))AS TIP_UNID,
    CAST(SUGERE.FAS_CON AS VARCHAR2(4 byte)) AS FAS_CON,
    CAST(EM.tipmed AS NUMBER(38, 0)) AS TIPMED,
    CAST(EM.odi AS VARCHAR2(99 byte)) AS ODI,
    CAST(EM.ti AS VARCHAR2(2 byte)) AS TI,
    CAST(EM.cm AS VARCHAR(3)) AS CM,
    CAST(EM.tuc AS VARCHAR(3)) AS TUC,
    CAST(EM.a1 AS VARCHAR(2)) AS A1,
    CAST(EM.a2 AS VARCHAR(2)) AS A2,
    CAST(EM.a3 AS VARCHAR(2)) AS A3,
    CAST(EM.a4 AS VARCHAR(2)) AS A4,
    CAST(EM.a5 AS VARCHAR(2)) AS A5,
    CAST(EM.a6 AS VARCHAR(2)) AS A6,
    CAST(EM.sitcont AS VARCHAR2(3 byte)) AS SITCONT,
    TO_CHAR(TO_DATE(EM.DAT_IMO, 'DD/MM/RR'), 'DD/MM/YYYY') AS DAT_IMO,
    CAST(EM.descr AS VARCHAR2(255 byte)) AS DESCR
FROM MV_SIGA_EQU_MED EM
LEFT JOIN BDGD_STAGING.MV_GENESIS_EQME VWEQ
ON TO_CHAR(VWEQ.NR_CONTA_CNS) = EM.DESCR 
LEFT JOIN (SELECT
 DISTINCT FEATURE_ID,
          FASE_SUGERIDA AS "FAS_CON"
 FROM BDGD_STAGING.MV_GENESIS_SUGERE_FASE_BT )SUGERE ON SUGERE.FEATURE_ID = VWEQ.RAMAL_LIGACAO
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_SSDBT_TEMP" ("COD_ID", "PN_CON_1", "PN_CON_2", "UNI_TR_D", "CTMT", "UNI_TR_S", "SUB", "CONJ", "ARE_LOC", "FAS_CON", "DIST", "PAC_1", "PAC_2", "TIP_CND", "POS", "ODI_FAS", "TI_FAS", "SITCONTFAS", "ODI_NEU", "TI_NEU", "SITCONTNEU", "COMP", "DESCR", "GEOMETRY", "R")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select * from (select mv.*, rank() over (PARTITION BY cod_id order by ROWID) r from mv_bdgd_ssdbt mv) where r = 1
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_BASE" ("OBJECTID", "DIST", "DAT_INC", "DAT_FNL", "DAT_EXT", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND NEXT null
  WITH PRIMARY KEY USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT "BDGD2020_BASE"."OBJECTID" "OBJECTID","BDGD2020_BASE"."DIST" "DIST","BDGD2020_BASE"."DAT_INC" "DAT_INC","BDGD2020_BASE"."DAT_FNL" "DAT_FNL","BDGD2020_BASE"."DAT_EXT" "DAT_EXT","BDGD2020_BASE"."DESCR" "DESCR" FROM "BDGD_STAGING"."BDGD2020_BASE" "BDGD2020_BASE"
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_INDGER" ("COD_ID", "DIST", "MUN", "MES", "ANO", "NCM", "NFEMC", "NFECDCL", "NFECDSL", "NFE", "NFEAU", "NFEAR", "NLSBR", "NLEBR", "NLSBU", "NLEBU", "NLSGA", "NLEGA", "NVLBU", "NVLBR", "NVLBUP", "NVLBRP", "NRUSAR", "NRUEDPAR", "NRUSAU", "NRUEDPAU", "NRNSAR", "NRNEDPAR", "NRNSAU", "NRNEDPAU", "NCR", "NCRES", "NCIND", "NCSP", "NCPP", "NCIP", "NCCO", "NCCP", "NCSDE", "NCRDE", "MINVC", "MAXVC", "VMC", "VTP", "NSA", "NEA", "NIMP", "NSM", "NPAD", "NII", "NTOI", "NACDFPR", "NMORFPR", "NACDFTR", "NMORFTR", "NACDPOP", "NMORPOP", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
  COD_ID,
  DIST,
  MUN,
  MES,
  ANO,
  NCM,
  NFEMC,
  NFECDCL,
  NFECDSL,
  NFE,
  NFEAU,
  NFEAR,
  NLSBR,
  NLEBR,
  NLSBU,
  NLEBU,
  NLSGA,
  NLEGA,
  NVLBU,
  NVLBR,
  NVLBUP,
  NVLBRP,
  NRUSAR,
  NRUEDPAR,
  NRUSAU,
  NRUEDPAU,
  NRNSAR,
  NRNEDPAR,
  NRNSAU,
  NRNEDPAU,
  NCR,
  NCRES,
  NCIND,
  NCSP,
  NCPP,
  NCIP,
  NCCO,
  NCCP,
  NCSDE,
  NCRDE,
  MINVC,
  MAXVC,
  VMC,
  VTP,
  NSA,
  NEA,
  NIMP,
  NSM,
  NPAD,
  NII,
  NTOI,
  NACDFPR,
  NMORFPR,
  NACDFTR,
  NMORFTR,
  NACDPOP,
  NMORPOP,
  DESCR
   FROM BDGD_INDICADORES_GERENCIAIS@BDGD_BI_PROD
 WHERE 
 ANO='2019'
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UCAT" ("COD_ID", "PN_CON", "DIST", "PAC", "CEG", "CTAT", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CLAS_SUB", "CNAE", "TIP_CC", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "DAT_CON", "CAR_INST", "LIV", "ARE_LOC", "DEM_P_01", "DEM_P_02", "DEM_P_03", "DEM_P_04", "DEM_P_05", "DEM_P_06", "DEM_P_07", "DEM_P_08", "DEM_P_09", "DEM_P_10", "DEM_P_11", "DEM_P_12", "DEM_F_01", "DEM_F_02", "DEM_F_03", "DEM_F_04", "DEM_F_05", "DEM_F_06", "DEM_F_07", "DEM_F_08", "DEM_F_09", "DEM_F_10", "DEM_F_11", "DEM_F_12", "ENE_P_01", "ENE_P_02", "ENE_P_03", "ENE_P_04", "ENE_P_05", "ENE_P_06", "ENE_P_07", "ENE_P_08", "ENE_P_09", "ENE_P_10", "ENE_P_11", "ENE_P_12", "ENE_F_01", "ENE_F_02", "ENE_F_03", "ENE_F_04", "ENE_F_05", "ENE_F_06", "ENE_F_07", "ENE_F_08", "ENE_F_09", "ENE_F_10", "ENE_F_11", "ENE_F_12", "DIC", "FIC", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
SELECT
       SIGA.COD_ID,
       DECODE(SIGA.COD_ID,19889386,12681392,12355505,2664721,45556905,2672754, GEN.PN_CON) PN_CON,
       SIGA.DIST,
       DECODE(SIGA.COD_ID,45556905,'AT-5853797',19889386,'AT-6059846',12355505,'AT-5854298', 'AT-' || GEN.PAC)  PAC, 
       SIGA.CEG,  
    CASE
       WHEN SIGA.COD_ID=45556905 THEN '247'
       WHEN SIGA.COD_ID=12355505 THEN '117'
       WHEN SIGA.COD_ID=19889386 THEN '329'
    ELSE CAST (DECODE(GEN.CTAT,'129','319',GEN.CTAT) as VARCHAR(3)) END as CTAT,    
    DECODE(SIGA.COD_ID,45556905,'JVU',12355505,'CCI',19889386,'PTI',GEN.SUB) SUB,
    DECODE(SIGA.COD_ID,45556905,13372,12355505,13300,19889386,13426,GEN.CONJ) CONJ,
    SIGA.MUN, SIGA.LGRD, SIGA.BRR, SIGA.CEP,
    CASE
    WHEN SIGA.COD_ID IN (12337949, 40728325, 42179248, 45063860, 48721400, 48881432,28076002,42829455,12337892) THEN
    'CSPS'
    ELSE SIGA.CLAS_SUB
    END CLAS_SUB, NVL(SIGA.CNAE, 0) CNAE,
    CASE
    WHEN SIGA.COD_ID IN (12337892,45063860,48721400,12337949,42179248,40728325,12337892) THEN  'TIP-AC-A3'
    WHEN SIGA.COD_ID IN (48881432,28076002,42829455) THEN  'TIP-AC-A2'
    WHEN SIGA.COD_ID = 27061010 THEN 'TIP-A1'
    WHEN SIGA.COD_ID IN (19889386,12355505,19865568,45556905,12352190,43340654,45503402,45032744,47447054,12354908,28542500,22576798,51214390,42245615,44118041, 46855973,45086615,28665857,29279063,45004295,44857529,50708357,40194401,43813617,42400815,27788955, 45556905,28665849,12352719,45032841,45004449,43145584,18940396,46806514,44829622,52005760,41980672, 41996722,12352336,42465844,45703258,45513718,44430436,44601940,12354886,32070418,41385570,12352980, 41987634,46708296,25147278,47344140,46335090,19865568,29216940,12351313,44922649,44533669,28665865, 29268223,29209081,44964031,12353413,41136421,42598585,28333285,42952796,44830019,12353090) THEN 'TIP-A2'
    WHEN SIGA.COD_ID IN (24676730,24694593,31902380,12351968,31499666,31159814,12353588,46465580,31039304,12352557,12351461,29353301,49263791, 12353669,12353235,12353901,12352930,28962304,49110898,31900522,43227610,40566406,46343662,25161912, 43995960,12355254,42826944,25112539,12353677,40588345,12353995,31877156,27880550) THEN 'TIP-A3'
    ELSE '-1'
    END TIP_CC, --SIGA.TIP_CC,
    SIGA.FAS_CON,
    SIGA.GRU_TEN,
    SIGA.TEN_FORN,
    SIGA.GRU_TAR,
    SIGA.SIT_ATIV,
    SIGA.DAT_CON,
    CASE
    WHEN TO_NUMBER(replace(SIGA.CAR_INST, ',', '.')) < 8 THEN
    '8'
    WHEN TO_NUMBER(replace(SIGA.CAR_INST, ',', '.')) IS NULL THEN
    '8'
    ELSE replace(SIGA.CAR_INST, ',', '.')
    END CAR_INST,
    SIGA.LIV,
    GEN.ARE_LOC,
    
    COALESCE(SIGA.DEM_P_01,'0') AS DEM_P_01,
    COALESCE(SIGA.DEM_P_02,'0') AS DEM_P_02,
    COALESCE(SIGA.DEM_P_03,'0') AS DEM_P_03,
    COALESCE(SIGA.DEM_P_04,'0') AS DEM_P_04,
    COALESCE(SIGA.DEM_P_05,'0') AS DEM_P_05,
    COALESCE(SIGA.DEM_P_06,'0') AS DEM_P_06,
    COALESCE(SIGA.DEM_P_07,'0') AS DEM_P_07,
    COALESCE(SIGA.DEM_P_08,'0') AS DEM_P_08,
    COALESCE(SIGA.DEM_P_09,'0') AS DEM_P_09,
    COALESCE(SIGA.DEM_P_10,'0') AS DEM_P_10,
    COALESCE(SIGA.DEM_P_11,'0') AS DEM_P_11,
    COALESCE(SIGA.DEM_P_12,'0') AS DEM_P_12,
    COALESCE(SIGA.DEM_f_01,'0') AS DEM_f_01,
    COALESCE(SIGA.DEM_f_02,'0') AS DEM_f_02,
    COALESCE(SIGA.DEM_f_03,'0') AS DEM_f_03,
    COALESCE(SIGA.DEM_f_04,'0') AS DEM_f_04,
    COALESCE(SIGA.DEM_f_05,'0') AS DEM_f_05,
    COALESCE(SIGA.DEM_f_06,'0') AS DEM_f_06,
    COALESCE(SIGA.DEM_f_07,'0') AS DEM_f_07,
    COALESCE(SIGA.DEM_f_08,'0') AS DEM_f_08,
    COALESCE(SIGA.DEM_f_09,'0') AS DEM_f_09,
    COALESCE(SIGA.DEM_f_10,'0') AS DEM_f_10,
    COALESCE(SIGA.DEM_f_11,'0') AS DEM_f_11,
    COALESCE(SIGA.DEM_f_12,'0') AS DEM_f_12,
     COALESCE(SIGA.ENE_P_01,'0') as ENE_P_01,
     COALESCE(SIGA.ENE_P_02,'0') as ENE_P_02,
     COALESCE(SIGA.ENE_P_03,'0') as ENE_P_03,
     COALESCE(SIGA.ENE_P_04,'0') as ENE_P_04,
     COALESCE(SIGA.ENE_P_05,'0') as ENE_P_05,
     COALESCE(SIGA.ENE_P_06,'0') as ENE_P_06,
     COALESCE(SIGA.ENE_P_07,'0') as ENE_P_07,
     COALESCE(SIGA.ENE_P_08,'0') as ENE_P_08,
     COALESCE(SIGA.ENE_P_09,'0') as ENE_P_09,
     COALESCE(SIGA.ENE_P_10,'0') as ENE_P_10,
     COALESCE(SIGA.ENE_P_11,'0') as ENE_P_11,
     COALESCE(SIGA.ENE_P_12,'0') as ENE_P_12,
     COALESCE(SIGA.ENE_F_01,'0') as ENE_F_01,
     COALESCE(SIGA.ENE_F_02,'0') as ENE_F_02,
     COALESCE(SIGA.ENE_F_03,'0') as ENE_F_03,
     COALESCE(SIGA.ENE_F_04,'0') as ENE_F_04,
     COALESCE(SIGA.ENE_F_05,'0') as ENE_F_05,
     COALESCE(SIGA.ENE_F_06,'0') as ENE_F_06,
     COALESCE(SIGA.ENE_F_07,'0') as ENE_F_07,
     COALESCE(SIGA.ENE_F_08,'0') as ENE_F_08,
     COALESCE(SIGA.ENE_F_09,'0') as ENE_F_09,
     COALESCE(SIGA.ENE_F_10,'0') as ENE_F_10,
     COALESCE(SIGA.ENE_F_11,'0') as ENE_F_11,
     COALESCE(SIGA.ENE_F_12,'0') as ENE_F_12,
    TO_NUMBER(replace(SIGA.DIC, ',', '.')) DIC,
    TO_NUMBER(replace(SIGA.FIC, ',', '.')) FIC,
     'NR_CONTA_CNS_GENESIS: ' || GEN.COD_ID || ', ' || GEN.DESCR DESCR,
     GEN.GEOMETRY
FROM BDGD_STAGING.MV_GENESIS_UCAT GEN
INNER JOIN MV_SIGA_UC_AT SIGA ON GEN.COD_ID = SIGA.COD_ID
WHERE
SIGA.GRU_TAR in ('A1','A2','A3'))
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_BAY" ("COD_ID", "DIST", "SUB_GRP", "POS", "SUB", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT 
sap.local_instalacao as COD_ID,
5697 as DIST,
CASE
WHEN sap.denominacao like '%230KV%' THEN 'A1'
WHEN sap.denominacao like '%138KV%' THEN 'A2A'
WHEN sap.denominacao like '%88KV%' THEN 'A2B'
WHEN sap.denominacao like '%69KV%' THEN 'A3'
WHEN sap.denominacao like '%34,5KV%' THEN 'A3A'
WHEN sap.denominacao like '%23KV%' THEN 'A4'
WHEN sap.denominacao like '%13,8KV%' THEN 'A4'
WHEN sap.denominacao like '%5KV%' THEN 'A4'
ELSE '0'
END SUB_GRP,
'PD' AS POS,
SUB.SG_SUBESTACAO AS SUB,
denominacao AS DESCR
FROM 
(
  select * from bdgd.mv_bdgd_sap 
  where 
    obj_tecnico = 'MSE' 
    and local_instalacao like 'SE-0%' 
    and denominacao not like '%GERAL%' 
    and status_se = 'CRI.' 
    and status_ue = 'OPER'
) SAP 
LEFT JOIN BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB ON sap.subestacao = 'SE-0000'||SUB.NR_SUBESTACAO


/* VERSÃO 2019
  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_BAY" 
  AS (SELECT cod_id,
           5697 AS DIST,
           CASE
             WHEN ten_nom = 72 THEN 'A3A'
             ELSE 'A4'
           END  AS SUB_GRP,
           'PD' AS POS,
           sub,
           nom  AS DESCR
    FROM   mv_bdgd_ctmt)
   UNION
   (SELECT Trim(Replace(Replace(sap.local_instalacao, 'SE-', ''), ' ', '-'))AS
           COD_ID,
           5697                                                             AS
           DIST
           ,
           CASE
             WHEN sap.denominacao LIKE '%138%' THEN 'A2A'
             WHEN sap.denominacao LIKE '%13,8KV%' THEN 'A4'
             WHEN sap.denominacao LIKE '%23KV%' THEN 'A4'
             WHEN sap.denominacao LIKE '%5KV%' THEN 'A4'
             WHEN sap.denominacao LIKE '%69%' THEN 'A3'
             WHEN sap.denominacao LIKE '%34,5KV%' THEN 'A3A'
             WHEN sap.local_instalacao LIKE '%SE-252-LT%' THEN 'A3'
             WHEN sap.local_instalacao LIKE '%SE-506-LI%' THEN 'A2A'
             WHEN sap.local_instalacao LIKE '%SE-111-LT%' THEN 'A2A'
             WHEN sap.local_instalacao LIKE '%SE-338-LI%' THEN 'A4'
             WHEN sap.local_instalacao LIKE '%SE-338-AL%' THEN 'A4'
             ELSE '0'
           END
           SUB_GRP
           ,
           CASE
             WHEN sap.local_instalacao LIKE '%LI%' THEN 'T'
             WHEN sap.local_instalacao LIKE '%SE-559%' THEN 'CS'
             WHEN sap.local_instalacao LIKE '%SE-252%' THEN 'CS'
             WHEN sap.local_instalacao LIKE '%SE-251%' THEN 'CS'
             WHEN sap.local_instalacao LIKE '%SE-355%' THEN 'CS'
             WHEN sap.local_instalacao LIKE '%SE-351%' THEN 'CS'
             WHEN sap.local_instalacao LIKE '%SE-452%' THEN 'CS'
             WHEN sap.local_instalacao LIKE '%SE-454%' THEN 'CS'
             ELSE 'PD'
           END                                                              POS,
           CASE
             WHEN sap.local_instalacao LIKE '%SE-559%' THEN 'TBI'
             WHEN sap.local_instalacao LIKE '%SE-252%' THEN 'TPY'
             WHEN sap.local_instalacao LIKE '%SE-251%' THEN 'PBS'
             WHEN sap.local_instalacao LIKE '%SE-355%' THEN 'TKA'
             WHEN sap.local_instalacao LIKE '%SE-351%' THEN 'CMS'
             --WHEN sap.local_instalacao LIKE '%SE-452%' THEN 'KBN'
             WHEN sap.local_instalacao LIKE '%SE-454%' THEN 'KMY'
             WHEN sap.local_instalacao LIKE '%SE-258%' THEN 'WFO'
             ELSE SUB.sub
           END                                                              AS
           SUB,
           sap.local_instalacao
           ||'/'
           ||sap.denominacao                                                AS
           DESCR
    FROM   bdgd_staging.sap SAP
           left join bdgd_staging.MV_GENESIS_EQPTO_SUB SUB
                  ON 'SE-'
                     ||SUB.nr_sub = sap.subestacao
    WHERE  obj_tecnico = 'MSE'
           AND ( sap.local_instalacao LIKE '%LI %'
                  OR sap.local_instalacao LIKE '%LT %'
                  OR sap.local_instalacao LIKE 'LT-%'
                  OR sap.local_instalacao LIKE '%RM%'
                  OR sap.local_instalacao LIKE '% SA%'
                  OR sap.local_instalacao LIKE '%SE %' )
           AND status_se = 'CRI.'
           AND status_ue = 'OPER'
           AND odi <> 'DUMMY'
           AND sap.local_instalacao NOT LIKE '%SE-212%'
           AND sap.local_instalacao NOT LIKE '%332%'
           AND sap.local_instalacao NOT LIKE '%SE-452%');
           */
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQME_TESTE" ("COD_ID", "PAC", "DIST", "TIP_UNID", "FAS_CON", "TIPMED", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "SITCONT", "DAT_IMO", "DESCR", "R")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT 
eqme.cod_id,
coalesce(uc_ug.pac,eqme.pac) as pac,
eqme.dist,
eqme.tip_unid,
coalesce(uc_ug.fas_con,eqme.fas_con) as fas_con,
eqme.tipmed,
eqme.odi,
eqme.ti,
eqme.cm,
eqme.tuc,
eqme.a1,
eqme.a2,
eqme.a3,
eqme.a4,
eqme.a5,
eqme.a6,
eqme.sitcont,
eqme.dat_imo,
eqme.descr,
eqme.r
from mv_bdgd_eqme_temp eqme
left join 
(
select cod_id,pac,fas_con from mv_bdgd_ucbt_temp
union all
select cod_id,pac,fas_con from mv_bdgd_ucmt where cod_id != '19889386'
union all
select cod_id,pac,fas_con from mv_bdgd_ucat
union all
select cod_id,pac,fas_con from mv_bdgd_ugat
union all
select cod_id,pac,fas_con from mv_bdgd_ugmt
) uc_ug on uc_ug.cod_id = eqme.descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQTRM" ("COD_ID", "SUB", "DIST", "PAC", "TIP_UNID", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select distinct * from (
SELECT
NVL(S.EQUIPAMENTO, '0') AS "COD_ID",
SUB.SUB AS "SUB",
'5697' AS "DIST",
DECODE(SUB.NR_PTO_ELET_INICIO, NULL, 'DESC-'||S.EQUIPAMENTO, ('' || SUB.NR_PTO_ELET_INICIO)) AS "PAC",
CASE
WHEN S.TUC='575' AND S.A1='20' THEN '44'
ELSE TUNI.COD_ID END AS "TIP_UNID",
S.ODI AS "ODI",
S.TI AS "TI",
S.CENTRO_MODULAR AS "CM",
S.TUC AS "TUC",
S.A1 AS "A1",
S.A2 AS "A2",
S.A3 AS "A3",
S.A4 AS "A4",
S.A5 AS "A5",
S.A6 AS "A6",
S.IDUC AS "IDUC",
DECODE(S.DATA_IMOB, NULL, 'NIM', 'AT1') AS "SITCONT",
S.DATA_IMOB AS "DAT_IMO",
S.LOCAL_INSTALACAO AS "DESCR"
FROM (SELECT * FROM GEN_BDGD2019.mv_eqptos_subestacao WHERE tipo_eqpto in ('TC','TP')) SUB
     INNER JOIN (SELECT * FROM MV_BDGD_SAP WHERE  BDGD = 'EQTRM' AND LOCAL_INSTALACAO LIKE 'SE-0%') S
        ON SUB.LOCAL_INST = S.LOCAL_INSTALACAO
LEFT JOIN TUNI
ON
TUNI.UNIDADE = S.TIP_UNID


UNION ALL

SELECT
SIGA.COD_ID,
COALESCE(UCMT.SUB,'0') as SUB,
SIGA.DIST,
COALESCE(SIGA.PAC,UCMT.PAC,'0') as PAC,
CASE
WHEN SIGA.TUC='575' AND SIGA.A1='20' THEN '44'
ELSE SIGA.TIP_UNID END AS "TIP_UNID",
SIGA.ODI,
SIGA.TI,
SIGA.CM,
SIGA.TUC,
SIGA.A1,
SIGA.A2,
SIGA.A3,
SIGA.A4,
SIGA.A5,
SIGA.A6,
SIGA.IDUC,
SIGA.SITCONT,
SIGA.DAT_IMO,
SIGA.DESCR
FROM MV_SIGA_EQU_TRA_MED SIGA
INNER JOIN MV_BDGD_UCMT UCMT ON UCMT.COD_ID=SIGA.DESCR
WHERE
SIGA.TUC <>'295'
)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_PONNOT" ("COD_ID", "DIST", "TIP_PN", "POS", "ESTR", "MAT", "ESF", "ALT", "ARE_LOC", "CONJ", "MUN", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "SITCONT", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT * FROM BDGD_STAGING.MV_GENESIS_PONNOT
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_SUB" ("COD_ID", "DIST", "POS", "NOM", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND NEXT null
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT *
FROM BDGD_STAGING.MV_GENESIS_SUB
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UCBT" ("COD_ID", "DIST", "PAC", "CEG", "PN_CON", "UNI_TR_D", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CLAS_SUB", "CNAE", "TIP_CC", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "DAT_CON", "CAR_INST", "LIV", "ARE_LOC", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "SEMRED", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT SIGA.COD_ID,
         SIGA.DIST,
        'BT-' || GEN.PAC PAC, SIGA.CEG, GEN.PN_CON, GEN.UNI_TR_D, --COALESCE(ALI.NOVO_ALIMENTADOR, GEN.CTMT) CTMT, --COALESCE(ALI.UN_TR_S, NVL2(TAB.ID_TT, 'SE-' || TAB.ID_TT, '0')) UNI_TR_S, 
         COALESCE(NA.NOVO_ALIMENTADOR, GEN.CTMT,'0')CTMT,
         COALESCE(NA.UN_TR_S,TAB.LOC_INST_SAP_TT,'0') AS UNI_TR_S,
        --NVL2(TAB.ID_TT, 'SE-' || TAB.ID_TT, '0') UNI_TR_S,
          GEN.SUB, 
          GEN.CONJ, 
          SIGA.MUN, 
          SIGA.LGRD, 
          SIGA.BRR, 
          SIGA.CEP, 
          SIGA.CLAS_SUB,         
          NVL(SIGA.CNAE, '0') CNAE,
    TIP.TIP_CC, 
    SIGA.FASE_SUGERIDA_CONSUMIDOR AS FAS_CON, 
    SIGA.GRU_TEN, SIGA.TEN_FORN, 
    SIGA.GRU_TAR, 
    SIGA.SIT_ATIV, 
    SIGA.DAT_CON,
    CASE
   -- WHEN to_number(replace(car_inst, '.', ',')) < 8 THEN
    WHEN car_inst < 8 THEN
    '8'
    WHEN SIGA.CAR_INST IS NULL THEN
    '8'
    ELSE SIGA.CAR_INST
    END CAR_INST, 
    SIGA.LIV, 
    GEN.ARE_LOC, 
    COALESCE(SIGA.ENE_01,'0') as ENE_01,
    COALESCE(SIGA.ENE_02,'0') as ENE_02, 
    COALESCE(SIGA.ENE_03,'0') as ENE_03,
    COALESCE(SIGA.ENE_04,'0') as ENE_04,
    COALESCE(SIGA.ENE_05,'0') as ENE_05,
    COALESCE(SIGA.ENE_06,'0') as ENE_06,
    COALESCE(SIGA.ENE_07,'0') as ENE_07,
    COALESCE(SIGA.ENE_08,'0') as ENE_08,
    COALESCE(SIGA.ENE_09,'0') as ENE_09,
    COALESCE(SIGA.ENE_10,'0') as ENE_10,
    COALESCE(SIGA.ENE_11,'0') as ENE_11,
    COALESCE(SIGA.ENE_12,'0') as ENE_12,
    COALESCE(TO_NUMBER(DIC_FIC.DIC),0) DIC, 
    COALESCE(TO_NUMBER(DIC_FIC.FIC),0) FIC, 
    GEN.SEMRED, 'NR_CONTA_CNS_GENESIS: ' || GEN.COD_ID || ',' || GEN.DESCR DESCR,
    GEN.GEOMETRY GEOMETRY
FROM BDGD_STAGING.MV_GENESIS_UCBT GEN
INNER JOIN BDGD_STAGING.MV_SIGA_UC_BT SIGA      ON GEN.COD_ID = SIGA.COD_ID
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB       ON GEN.CTMT = TAB.CD_ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS NA  ON NA.ALIMENTADOR=TAB.CD_ALIMENTADOR    
LEFT JOIN MV_TAB_UC_DIC_FIC DIC_FIC             ON DIC_FIC.NR_CONTA_CNS = GEN.COD_ID
LEFT JOIN (select distinct * from TAB_TIP_CC_DAIMON ) TIP             ON SIGA.COD_ID = TIP.COD_ID
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQSE_TEMP" ("COD_ID", "DIST", "PAC_1", "PAC_2", "CLAS_TEN", "ELO_FSV", "MEI_ISO", "FAS_CON", "COR_NOM", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "ABER_CRG", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
eqse.cod_id,
eqse.dist,
decode(coalesce(ajust.pac_1,eqse.pac_1),'1547154','SE-SE-6029071',coalesce(ajust.pac_1,eqse.pac_1)) as pac_1,
coalesce(ajust.pac_2,eqse.pac_2) as pac_2,
eqse.clas_ten,
eqse.elo_fsv,
eqse.mei_iso,
eqse.fas_con,
eqse.cor_nom,
eqse.odi,
eqse.ti,
eqse.cm,
eqse.tuc,
eqse.a1,
eqse.a2,
eqse.a3,
eqse.a4,
eqse.a5,
eqse.a6,
eqse.iduc,
eqse.sitcont,
eqse.dat_imo,
eqse.aber_crg,
eqse.descr
from mv_bdgd_eqse eqse left join
(
select distinct local_inst,eqpto.sub,ctmt,ten_nom,uni_tr_s,'D-'||LOCAL_INST as pac_1,pac as pac_2 from
(select * from 
GEN_BDGD2019.mv_eqptos_subestacao 
where id_tipo_modulo != 1 and 
id_tn_nominal_eqpto < 6 and 
tipo_eqpto in ('CD', 'CF', 'CT', 'CQ', 'CN', 'CU', 'CO', 'CUA', 'DJ', 'FU' , 'RL', 'CBY')) EQPTO 
inner join (select sub,
cod_id as ctmt,
pac,
uni_tr_s,
decode(ten_nom,72,5,62,4,49,3,NULL) as ten_nom
from mv_bdgd_ctmt where cod_id in (
select min(COD_ID) from mv_bdgd_ctmt group by sub,ten_nom)) ctmt on ctmt.sub = eqpto.sub and ctmt.ten_nom = eqpto.id_tn_nominal_eqpto ) ajust 
on ajust.local_inst = eqse.descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_RAMLIG_TEMP" ("COD_ID", "PN_CON_1", "PN_CON_2", "DIST", "PAC_1", "PAC_2", "UNI_TR_D", "CTMT", "FAS_CON", "UNI_TR_S", "SUB", "CONJ", "ARE_LOC", "TIP_CND", "POS", "ODI_FAS", "TI_FAS", "SITCONTFAS", "ODI_NEU", "TI_NEU", "SITCONTNEU", "COMP", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
ramlig.cod_id,
ramlig.pn_con_1,
ramlig.pn_con_2,
ramlig.dist,
ramlig.pac_1,
ramlig.pac_2,
ramlig.uni_tr_d,
ramlig.ctmt,
coalesce(alt_fas_con.fas_con_sugerida,ramlig.fas_con) as fas_con,
ramlig.uni_tr_s,
ramlig.sub,
ramlig.conj,
ramlig.are_loc,
coalesce(alt_fas_con.TIP_CND_SUGERIDA,ramlig.tip_cnd) as tip_cnd,
ramlig.pos,
ramlig.odi_fas,
ramlig.ti_fas,
ramlig.sitcontfas,
ramlig.odi_neu,
ramlig.ti_neu,
ramlig.sitcontneu,
ramlig.comp,
ramlig.descr
from mv_bdgd_ramlig ramlig left join (select * from mv_bdgd_ajust_fase_bt where entidade = 'RAMLIG') alt_fas_con
on alt_fas_con.cod_id = ramlig.cod_id
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_AJUST_FASE_BT" ("COD_ID", "PAC_1", "PAC_2", "FAS_CON", "ENTIDADE", "FAS_CON_PAI", "COD_ID_PAI", "ENTIDADE_PAI", "PAC_2_PAI", "TESTE_FASE", "FAS_CON_SUGERIDA", "TIP_CND_SUGERIDA")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
select aux.*,
CASE
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'AN' THEN 'AN'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'BN' THEN 'AN'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'CN' THEN 'AN'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'ABN' THEN 'AN'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'CAN' THEN 'AN'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'BCN' THEN 'AN'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'ABCN' THEN 'AN'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'AN' THEN 'BN'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'BN' THEN 'BN'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'CN' THEN 'BN'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'ABN' THEN 'BN'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'CAN' THEN 'BN'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'BCN' THEN 'BN'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'ABCN' THEN 'BN'
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'AN' THEN 'CN'
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'BN' THEN 'CN'
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'CN' THEN 'CN'
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'ABN' THEN 'CN'
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'CAN' THEN 'CN'
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'BCN' THEN 'CN'
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'ABCN' THEN 'CN'
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'AN' THEN 'AN'
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'BN' THEN 'BN'
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'CN' THEN 'AN'
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'ABN' THEN 'ABN'
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'CAN' THEN 'ABN'
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'BCN' THEN 'ABN'
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'ABCN' THEN 'ABN'
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'AN' THEN 'AN'
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'BN' THEN 'CN'
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'CN' THEN 'CN'
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'ABN' THEN 'CAN'
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'CAN' THEN 'CAN'
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'BCN' THEN 'CAN'
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'ABCN' THEN 'CAN'
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'AN' THEN 'BN'
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'BN' THEN 'BN'
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'CN' THEN 'CN'
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'ABN' THEN 'BCN'
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'CAN' THEN 'BCN'
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'BCN' THEN 'BCN'
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'ABCN' THEN 'BCN'
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'AN' THEN 'AN'
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'BN' THEN 'BN'
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'CN' THEN 'AN'
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'ABN' THEN 'ABN'
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'CAN' THEN 'CAN'
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'BCN' THEN 'BCN'
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'ABCN' THEN 'ABCN'
WHEN FAS_CON  = '-2' THEN FAS_CON_PAI
ELSE FAS_CON END FAS_CON_SUGERIDA,
CASE
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'AN' THEN NULL
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'BN' THEN NULL
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'CN' THEN NULL
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'ABN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'CAN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'BCN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'AN' AND FAS_CON = 'ABCN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'AN' THEN NULL
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'BN' THEN NULL
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'CN' THEN NULL
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'ABN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'CAN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'BCN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'ABCN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'AN' THEN NULL
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'BN' THEN NULL
WHEN FAS_CON_PAI = 'CN' AND FAS_CON = 'CN' THEN NULL
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'ABN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'CAN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'BCN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'BN' AND FAS_CON = 'ABCN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'AN' THEN NULL
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'BN' THEN NULL
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'CN' THEN NULL
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'ABN' THEN NULL
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'CAN' THEN NULL
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'BCN' THEN NULL
WHEN FAS_CON_PAI = 'ABN' AND FAS_CON = 'ABCN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'AN' THEN NULL
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'BN' THEN NULL
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'CN' THEN NULL
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'ABN' THEN NULL
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'CAN' THEN NULL
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'BCN' THEN NULL
WHEN FAS_CON_PAI = 'CAN' AND FAS_CON = 'ABCN' THEN '34172-2F'
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'AN' THEN NULL
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'BN' THEN NULL
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'CN' THEN NULL
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'ABN' THEN NULL
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'CAN' THEN NULL
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'BCN' THEN NULL
WHEN FAS_CON_PAI = 'BCN' AND FAS_CON = 'ABCN' THEN '34175-2F'
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'AN' THEN NULL
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'BN' THEN NULL
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'CN' THEN NULL
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'ABN' THEN NULL
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'CAN' THEN NULL
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'BCN' THEN NULL
WHEN FAS_CON_PAI = 'ABCN' AND FAS_CON = 'ABCN' THEN NULL
WHEN FAS_CON_PAI = '-2' AND FAS_CON = 'ABCN' THEN FAS_CON_PAI
ELSE NULL END TIP_CND_SUGERIDA
from (
select 
filho.*,
pai.fas_con as fas_con_pai,
pai.cod_id as cod_id_pai,
pai.entidade as entidade_pai,
pai.pac_2 as pac_2_pai,
CASE
WHEN (pai.fas_con = 'ABCN' OR pai.fas_con = 'ABC') and (filho.fas_con = 'ABCN' OR filho.fas_con = 'ABC' OR filho.fas_con = 'ABN' OR filho.fas_con = 'AB' OR filho.fas_con = 'BCN' OR filho.fas_con = 'BC' OR filho.fas_con = 'CAN' OR filho.fas_con = 'CA' OR filho.fas_con = 'AN' OR filho.fas_con = 'A' OR filho.fas_con = 'BN' OR filho.fas_con = 'B' OR filho.fas_con = 'CN' OR filho.fas_con = 'C')  THEN '0'
WHEN (pai.fas_con = 'ABN' OR pai.fas_con = 'AB') AND (filho.fas_con = 'ABN' OR filho.fas_con = 'AB' OR filho.fas_con = 'AN' OR filho.fas_con = 'A' OR filho.fas_con = 'BN' OR filho.fas_con = 'B') THEN '0'
WHEN (pai.fas_con = 'BCN' OR pai.fas_con = 'BC') AND (filho.fas_con = 'BCN' OR filho.fas_con = 'BC' OR filho.fas_con = 'BN' OR filho.fas_con = 'B' OR filho.fas_con = 'CN' OR filho.fas_con = 'C') THEN '0'
WHEN (pai.fas_con = 'CAN' OR pai.fas_con = 'CA') AND (filho.fas_con = 'CAN' OR filho.fas_con = 'CA' OR filho.fas_con = 'AN' OR filho.fas_con = 'A' OR filho.fas_con = 'CN' OR filho.fas_con = 'C') THEN '0'
WHEN (pai.fas_con = 'AN' OR pai.fas_con = 'A') AND (filho.fas_con = 'AN' OR filho.fas_con = 'A') THEN '0'
WHEN (pai.fas_con = 'BN' OR pai.fas_con = 'B') AND (filho.fas_con = 'BN' OR filho.fas_con = 'B') THEN '0'
WHEN (pai.fas_con = 'CN' OR pai.fas_con = 'C') AND (filho.fas_con = 'CN' OR filho.fas_con = 'C') THEN '0'
ELSE '1' END AS TESTE_FASE
from (
select distinct to_char(cod_id) as cod_id,to_char(pac) as pac_1,'PIP-'||cod_id as pac_2,fas_con,'PIP' as entidade from mv_bdgd_pip
union all
select to_char(cod_id) as cod_id,to_char(pac_1) as pac_1,to_char(pac_2) as pac_2,fas_con,'RAMLIG' as Entidade  from mv_bdgd_ramlig
union all
select to_char(cod_id) as cod_id,to_char(pac_1) as pac_1,to_char(pac_2) as pac_2,fas_con,'SSDBT' as Entidade from mv_bdgd_ssdbt_temp where fas_con != 'N'
union all
select to_char(cod_id)as cod_id,to_char(pac_1) as pac_1,to_char(pac_2) as pac_2,decode(fas_con_p,
'A','ABN',
'B','BCN',
'C','CAN',
'AB','ABN',
'CA','CAN',
'BC','BCN',
'ABC','ABCN',
FAS_CON_P) as fas_con,'UNTRD' as Entidade from mv_bdgd_untrd
) filho
left join 
(
select to_char(cod_id) as cod_id,to_char(pac_1) as pac_1,to_char(pac_2) as pac_2,fas_con,'RAMLIG' as entidade  from mv_bdgd_ramlig
union all
select to_char(cod_id) as cod_id,to_char(pac_1) as pac_1,to_char(pac_2) as pac_2,fas_con,'SSDBT' as entidade from mv_bdgd_ssdbt_temp where fas_con != 'N'
union all
select to_char(cod_id)as cod_id,to_char(pac_1) as pac_1,to_char(pac_2) as pac_2,decode(fas_con_p,'A','ABN','B','BCN','C','CAN','AB','ABN','CA','CAN','BC','BCN','ABC','ABCN',FAS_CON_P) as fas_con,'UNTRD' as entidade from mv_bdgd_untrd) 
pai on filho.pac_1 = pai.pac_2
) aux where TESTE_FASE = 1 and cod_id_pai is not null and FAS_CON is not null

)
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_PIP" ("COD_ID", "DIST", "MUN", "CONJ", "SUB", "UNI_TR_S", "CTMT", "UNI_TR_D", "PN_CON", "CLAS_SUB", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "ARE_LOC", "PAC", "TIP_CC", "CAR_INST", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "LIV", "SEMRED", "DAT_CON", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT GEN.COD_ID,
         GEN.DIST,
         GEN.MUN,
         GEN.CONJ,
         GEN.SUB,
         COALESCE(ALI.UN_TR_S, TAB.LOC_INST_SAP_TT,'0') AS UNI_TR_S,
         COALESCE(ALI.NOVO_ALIMENTADOR, GEN.CTMT,'0') AS CTMT,
         --COALESCE(ALI.UN_TR_S,DECODE(TAB.ID_TT,NULL,'0', '0', '0', 'SE-' || TAB.ID_TT)) UNI_TR_S,
         --COALESCE(ALI.NOVO_ALIMENTADOR, GEN.CTMT) CTMT,
         GEN.UNI_TR_D,
         NVL(GEN.PN_CON, '0') PN_CON,
         GEN.CLAS_SUB,
         GEN.FAS_CON,
         GEN.GRU_TEN,
         GEN.TEN_FORN,
         GEN.GRU_TAR,
         GEN.SIT_ATIV,
         GEN.ARE_LOC,
         --DECODE(GEN.PAC, NULL, '0', '0', '0', 'BT-' || GEN.PAC) PAC,
        DECODE(CON.NR_PTO_ELET_FIM, NULL, 'BT-' || NVL(CAST(GEN.PAC AS VARCHAR(50)) , 'DESC-'||GEN.COD_ID), ''||GEN.PAC) PAC,
        GEN.TIP_CC,
        GEN.CAR_INST,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('01/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('01/2019', 'MM/YYYY') THEN
    (GEN.ENE_01/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_01
    END ENE_01,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('02/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('02/2019', 'MM/YYYY') THEN
    (GEN.ENE_02/28)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_02
    END ENE_02,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('03/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('03/2019', 'MM/YYYY') THEN
    (GEN.ENE_03/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_03
    END ENE_03,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('04/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('04/2019', 'MM/YYYY') THEN
    (GEN.ENE_04/30)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_04
    END ENE_04,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('05/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('05/2019', 'MM/YYYY') THEN
    (GEN.ENE_05/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_05
    END ENE_05,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('06/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('06/2019', 'MM/YYYY') THEN
    (GEN.ENE_06/30)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_06
    END ENE_06,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('07/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('07/2019', 'MM/YYYY') THEN
    (GEN.ENE_07/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_07
    END ENE_07,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('08/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('08/2019', 'MM/YYYY') THEN
    (GEN.ENE_08/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_08
    END ENE_08,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('09/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('09/2019', 'MM/YYYY') THEN
    (GEN.ENE_09/30)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_09
    END ENE_09,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('10/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('10/2019', 'MM/YYYY') THEN
    (GEN.ENE_10/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_10
    END ENE_10,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('11/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('11/2019', 'MM/YYYY') THEN
    (GEN.ENE_11/30)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_11
    END ENE_11,
    CASE
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') > TO_DATE('12/2019', 'MM/YYYY') THEN
    0
    WHEN TO_DATE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), 'MM/YYYY') = TO_DATE('12/2019', 'MM/YYYY') THEN
    (GEN.ENE_12/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD')))
    ELSE GEN.ENE_12
    END ENE_12, --DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '01/2017', (GEN.ENE_01/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_01) ENE_01, /*DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '02/2017', (GEN.ENE_02/28)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_02) ENE_02, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '03/2017', (GEN.ENE_03/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_03) ENE_03, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '04/2017', (GEN.ENE_04/30)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_04) ENE_04, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '05/2017',
(GEN.ENE_05/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_05) ENE_05, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '06/2017', (GEN.ENE_06/30)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_06) ENE_06, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '07/2017', (GEN.ENE_07/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_07) ENE_07, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '08/2017', (GEN.ENE_08/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_08) ENE_08, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '09/2017', (GEN.ENE_09/30)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_09) ENE_09, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'),
TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '10/2017', (GEN.ENE_10/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_10) ENE_10, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '11/2017', (GEN.ENE_11/30)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_11) ENE_11, DECODE(TO_CHAR(DECODE(DT.DATA_CONEXAO, NULL, TO_DATE('20/10/2010', 'DD/MM/YYYY'), TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY')), 'MM/YYYY'), '12/2017', (GEN.ENE_12/31)*(TO_NUMBER(TO_CHAR(TO_DATE(DT.DATA_CONEXAO, 'DD/MM/YYYY'), 'DD'))) , GEN.ENE_12) ENE_12, */
    GEN.DIC, GEN.FIC, GEN.LIV, GEN.SEMRED, DECODE(DT.DATA_CONEXAO, NULL, '20/10/2010', DT.DATA_CONEXAO) DAT_CON, GEN.DESCR
FROM BDGD_STAGING.MV_GENESIS_PIP GEN
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB
    ON GEN.CTMT = TAB.CD_ALIMENTADOR
LEFT JOIN
    (SELECT MIN(A.DATA_CONEXAO) DATA_CONEXAO,
         A.LOCAL_INSTALACAO
    FROM MV_BDGD_SAP A
    WHERE A.BDGD = 'EQSE'
            AND A.LOCAL_INSTALACAO IS NOT NULL
            AND A.DATA_CONEXAO NOT LIKE '%00%'
    GROUP BY  A.LOCAL_INSTALACAO) DT
    ON DT.LOCAL_INSTALACAO = SUBSTR(GEN.DESCR, 1, (INSTR(GEN.DESCR, ',')-1))
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ALI
    ON ALI.ALIMENTADOR = GEN.CTMT
LEFT JOIN BDGD_STAGING.MV_GENESIS_VA_CONEC_MT_BT CON
    ON CON.NR_PTO_ELET_FIM = GEN.PAC
WHERE GEN.CONJ <> '0'
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNSEMT_TEMP" ("COD_ID", "DIST", "PAC_1", "PAC_2", "FAS_CON", "SIT_ATIV", "TIP_UNID", "P_N_OPE", "CAP_ELO", "COR_NOM", "TLCD", "DAT_CON", "POS", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "ARE_LOC", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
unsemt.cod_id,
unsemt.dist,
decode(coalesce(ajust.pac_1,unsemt.pac_1),'1547154','SE-SE-6029071',coalesce(ajust.pac_1,unsemt.pac_1)) as pac_1,
coalesce(ajust.pac_2,unsemt.pac_2) as pac_2,
unsemt.fas_con,
unsemt.sit_ativ,
unsemt.tip_unid,
unsemt.p_n_ope,
unsemt.cap_elo,
unsemt.cor_nom,
unsemt.tlcd,
unsemt.dat_con,
unsemt.pos,
coalesce(ajust.ctmt,unsemt.ctmt) as ctmt,
coalesce(ajust.uni_tr_s,unsemt.uni_tr_s) as uni_tr_s,
coalesce(ajust.sub,unsemt.sub) as sub,
unsemt.conj,
unsemt.mun,
unsemt.are_loc,
unsemt.descr,
unsemt.geometry
from mv_bdgd_unsemt unsemt left join 
(
select distinct local_inst,eqpto.sub,ctmt,ten_nom,uni_tr_s,'D-'||LOCAL_INST as pac_1,pac as pac_2 from
(select * from 
GEN_BDGD2019.mv_eqptos_subestacao 
where id_tipo_modulo != 1 and 
id_tn_nominal_eqpto < 6 and 
tipo_eqpto in ('CD', 'CF', 'CT', 'CQ', 'CN', 'CU', 'CO', 'CUA', 'DJ', 'FU' , 'RL', 'CBY')) EQPTO 
inner join (select sub,
cod_id as ctmt,
pac,
uni_tr_s,
decode(ten_nom,72,5,62,4,49,3,NULL) as ten_nom
from mv_bdgd_ctmt where cod_id in (
select min(COD_ID) from mv_bdgd_ctmt group by sub,ten_nom)) ctmt on ctmt.sub = eqpto.sub and ctmt.ten_nom = eqpto.id_tn_nominal_eqpto ) ajust 
on ajust.local_inst = unsemt.descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_SEGCON" ("COD_ID", "DIST", "GEOM_CAB", "FORM_CAB", "BIT_FAS_1", "BIT_FAS_2", "BIT_FAS_3", "BIT_NEU", "MAT_FAS_1", "MAT_FAS_2", "MAT_FAS_3", "MAT_NEU", "ISO_FAS_1", "ISO_FAS_2", "ISO_FAS_3", "ISO_NEU", "CND_FAS", "R1", "X1", "FTRCNV", "CNOM", "CMAX", "CM_FAS", "TUC_FAS", "A1_FAS", "A2_FAS", "A3_FAS", "A4_FAS", "A5_FAS", "A6_FAS", "CM_NEU", "TUC_NEU", "A1_NEU", "A2_NEU", "A3_NEU", "A4_NEU", "A5_NEU", "A6_NEU", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT A.COD_ID,
A.DIST,
TO_CHAR(A.GEOM_CAB) GEOM_CAB,
A.FORM_CAB,
A.BIT_FAS_1,
A.BIT_FAS_2,
A.BIT_FAS_3,
A.BIT_NEU,
A.MAT_FAS_1,
A.MAT_FAS_2,
A.MAT_FAS_3,
A.MAT_NEU,
A.ISO_FAS_1,
A.ISO_FAS_2,
A.ISO_FAS_3,
A.ISO_NEU,
A.CND_FAS,
A.R1,
A.X1,
A.FTRCNV,
A.CNOM,
A.CMAX,
A.CM_FAS,
A.TUC_FAS,
A.A1_FAS,
A.A2_FAS,
A.A3_FAS,
A.A4_FAS,
A.A5_FAS,
A.A6_FAS,
A.CM_NEU,
A.TUC_NEU,
A.A1_NEU,
A.A2_NEU,
A.A3_NEU,
A.A4_NEU,
A.A5_NEU,
A.A6_NEU,
A.DESCR
FROM BDGD_STAGING.MV_GENESIS_SEGCON A
WHERE A.COD_ID NOT IN ('MT-0--3', 'BT-0-0-0-0-7')
UNION
ALL
SELECT *
FROM BDGD_STAGING.TAB_BDGD_SEGCON_RAMAL
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UCMT" ("COD_ID", "PN_CON", "DIST", "PAC", "CEG", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CLAS_SUB", "CNAE", "TIP_CC", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "DAT_CON", "CAR_INST", "LIV", "ARE_LOC", "DEM_01", "DEM_02", "DEM_03", "DEM_04", "DEM_05", "DEM_06", "DEM_07", "DEM_08", "DEM_09", "DEM_10", "DEM_11", "DEM_12", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "SEMRED", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT SIGA.COD_ID,
GEN.PN_CON,
SIGA.DIST,
DECODE(GEN.PAC,'1639739','SE-SE-6037480','5914999','SE-SE-6000495',GEN.PAC) as PAC,
SIGA.CEG,
COALESCE(ALI.NOVO_ALIMENTADOR, GEN.CTMT,'0') AS CTMT,
COALESCE(ALI.UN_TR_S, TAB.LOC_INST_SAP_TT,'0') AS UNI_TR_S,
GEN.SUB,
GEN.CONJ,
SIGA.MUN,
SIGA.LGRD,
SIGA.BRR,
SIGA.CEP,
CASE WHEN SIGA.COD_ID IN (12313764, 12313934, 12314191, 12314418, 12314574, 12314582, 12314590, 12314620, 12314884, 12315015,
12315104, 12315120, 12315147, 12315155, 12337442, 12337450, 12337884, 12337892, 12339658, 12339780,
12340168, 12340303, 12340370, 12340419, 12340435, 12342870, 12350694, 12350783, 22837338, 25510364,
25514467, 25605101, 27160840, 29160686, 32133495, 32133681, 32133800, 42610216, 46245865, 46543645,
49552238, 49554630, 51082885,12340427, 18366177) THEN 'CSPS' ELSE SIGA.CLAS_SUB END CLAS_SUB,
NVL(SIGA.CNAE, '0') CNAE,
TIP.TIP_CC,
SIGA.FAS_CON,
SIGA.GRU_TEN,
SIGA.TEN_FORN,
SIGA.GRU_TAR,
SIGA.SIT_ATIV,
SIGA.DAT_CON,
CASE WHEN TO_NUMBER(SIGA.CAR_INST) < 8 THEN '8'
WHEN SIGA.CAR_INST IS NULL THEN '8'
ELSE SIGA.CAR_INST
END CAR_INST,
SIGA.LIV,
GEN.ARE_LOC,
COALESCE(SIGA.DEM_01,'0') as DEM_01,
COALESCE(SIGA.DEM_02,'0') as DEM_02,
COALESCE(SIGA.DEM_03,'0') as DEM_03,
COALESCE(SIGA.DEM_04,'0') as DEM_04,
COALESCE(SIGA.DEM_05,'0') as DEM_05,
COALESCE(SIGA.DEM_06,'0') as DEM_06,
COALESCE(SIGA.DEM_07,'0') as DEM_07,
COALESCE(SIGA.DEM_08,'0') as DEM_08,
COALESCE(SIGA.DEM_09,'0') as DEM_09,
COALESCE(SIGA.DEM_10,'0') as DEM_10,
COALESCE(SIGA.DEM_11,'0') as DEM_11,
COALESCE(SIGA.DEM_12,'0') as DEM_12,
COALESCE(SIGA.ENE_01,'0')as ENE_01,
COALESCE(SIGA.ENE_02,'0')as ENE_02,
COALESCE(SIGA.ENE_03,'0')as ENE_03,
COALESCE(SIGA.ENE_04,'0')as ENE_04,
COALESCE(SIGA.ENE_05,'0')as ENE_05,
COALESCE(SIGA.ENE_06,'0')as ENE_06,
COALESCE(SIGA.ENE_07,'0')as ENE_07,
COALESCE(SIGA.ENE_08,'0')as ENE_08,
COALESCE(SIGA.ENE_09,'0')as ENE_09,
COALESCE(SIGA.ENE_10,'0')as ENE_10,
COALESCE(SIGA.ENE_11,'0')as ENE_11,
COALESCE(SIGA.ENE_12,'0')as ENE_12,
COALESCE(TO_NUMBER(REPLACE(DIC_FIC.DIC, ',', '.')),0) DIC,
COALESCE(TO_NUMBER(DIC_FIC.FIC),0) FIC,
GEN.SEMRED,
'NR_CONTA_CNS_GENESIS: ' || GEN.COD_ID || ',' || GEN.DESCR DESCR,
GEN.GEOMETRY
FROM BDGD_STAGING.MV_GENESIS_UCMT GEN
INNER JOIN MV_SIGA_UC_MT SIGA                       ON GEN.COD_ID = SIGA.COD_ID
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB           ON GEN.CTMT = TAB.CD_ALIMENTADOR
LEFT JOIN MV_TAB_UC_DIC_FIC DIC_FIC                 ON DIC_FIC.NR_CONTA_CNS = GEN.COD_ID
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ALI     ON ALI.ALIMENTADOR = GEN.CTMT
LEFT JOIN BDGD.TAB_UCMT_TIP_CC TIP                  ON SIGA.COD_ID = TIP.COD_ID
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNCRMT_TEMP" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "POT_NOM", "PAC_1", "PAC_2", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "ARE_LOC", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY", "R")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select * from (select mv.*, rank() over (PARTITION BY cod_id order by ROWID) r from mv_bdgd_uncrmt_temp_1 mv) where r = 1
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNREAT_BKP" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "PAC_1", "PAC_2", "SUB", "CONJ", "MUN", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
'SE-'||NS.OBJECTID as COD_ID,
5697 as DIST,
'ABCN'  as FAS_CON,
'AT'  as SIT_ATIV,
14 as TIP_UNID,
'SE-'||SUB.PAC1_FID as PAC_1,
'SE-'||SUB.PAC2_FID as PAC_2,
GEN.SUB,
GEN.CONJ,
GEN.MUNICIPIO as MUN,
CASE 
  WHEN SAP.DATA_CONEXAO='00/00/0000' THEN '01/01/0001' 
  ELSE SAP.DATA_CONEXAO
  END AS  DAT_CON,
0 as BANC,
'PD' as POS,
SAP.LOCAL_INSTALACAO as DESCR,
NS.SHAPE as GEOMETRY
FROM
MV_BDGD_SAP SAP 
INNER JOIN BDGD_STAGING.TAB_BDGD_SUB_CONN SUB         ON SAP.LOCAL_INSTALACAO=SUB.PAC2
INNER JOIN BDGD_STAGING.TAB_NODE_SUB NS          ON NS.LOC_INST_SAP=SUB.PAC2
INNER JOIN BDGD_STAGING.MV_GENESIS_EQPTO_SUB GEN ON  'SE-'||GEN.NR_SUB = SAP.SUBESTACAO
WHERE
--SAP.SUBESTACAO is not null AND
--SAP.bdgd = 'EQRE'          AND
OBJ_TECNICO in ('TRT') AND
SAP.A2>=34
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNTRD_TEMP" ("COD_ID", "DIST", "PAC_1", "PAC_2", "PAC_3", "FAS_CON_P", "FAS_CON_S", "FAS_CON_T", "SIT_ATIV", "TIP_UNID", "POS", "ATRB_PER", "TEN_LIN_SE", "CAP_ELO", "CAP_CHA", "TAP", "ARE_LOC", "CONF", "POSTO", "POT_NOM", "PER_FER", "PER_TOT", "DAT_CON", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "BANC", "TIP_TRAFO", "MRT", "DESCR", "GEOMETRY", "R1")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select * from (select mv.*, rank() over (PARTITION BY cod_id order by pac_3, fas_con_p) r1 from 
( 
select
untrd.cod_id,
untrd.dist,
coalesce(ajust.pac_1,untrd.pac_1) as pac_1,
coalesce(ajust.pac_2,untrd.pac_2) as pac_2,
untrd.pac_3,
untrd.fas_con_p,
untrd.fas_con_s,
untrd.fas_con_t,
untrd.sit_ativ,
untrd.tip_unid,
untrd.pos,
untrd.atrb_per,
untrd.ten_lin_se,
untrd.cap_elo,
untrd.cap_cha,
untrd.tap,
untrd.are_loc,
untrd.conf,
untrd.posto,
untrd.pot_nom,
untrd.per_fer,
untrd.per_tot,
untrd.dat_con,
coalesce(ajust.ctmt,untrd.ctmt) as ctmt,
coalesce(ajust.uni_tr_s,untrd.uni_tr_s) as uni_tr_s,
coalesce(ajust.sub,untrd.sub) as sub,
untrd.conj,
untrd.mun,
untrd.banc,
untrd.tip_trafo,
untrd.mrt,
untrd.descr,
untrd.geometry
from mv_bdgd_untrd untrd left join 
(
select distinct local_inst,eqpto.sub,ctmt,ten_nom,uni_tr_s,'D-'||LOCAL_INST as pac_1,pac as pac_2 from
(select * from 
GEN_BDGD2019.mv_eqptos_subestacao 
where id_tipo_modulo != 1 and 
id_tn_nominal_eqpto < 6 and 
tipo_eqpto in ('TT')) EQPTO 
inner join (select sub,
cod_id as ctmt,
pac,
uni_tr_s,
decode(ten_nom,72,5,62,4,49,3,NULL) as ten_nom
from mv_bdgd_ctmt where cod_id in (
select min(COD_ID) from mv_bdgd_ctmt group by sub,ten_nom)) ctmt on ctmt.sub = eqpto.sub and ctmt.ten_nom = eqpto.id_tn_nominal_eqpto ) ajust 
on ajust.local_inst = substr(untrd.descr,1,10)
)
mv
) where r1 = 1
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQSIAT" ("COD_ID", "DIST", "SUB", "TIP_UNID", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "COMP", "DAT_IMO", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT 
REPLACE(REPLACE(S.LOCAL_INSTALACAO, '-', ''), '1SAT2', 3) COD_ID,
'5697' DIST,
SUB.SG_SUBESTACAO SUB,
8 TIP_UNID,
S.ODI,
DECODE(SUB.ID_AREA_OBJETO, 'U', DECODE(REGEXP_SUBSTR(SAP_SUB.DS_NIVEL_TN_NOMINAL, '[0-9]{2,3}'), '138', 33,
'69', 32,
'34', 31,
'23', 31),
'R', DECODE(REGEXP_SUBSTR(SAP_SUB.DS_NIVEL_TN_NOMINAL, '[0-9]{2,3}'),
'138', 53,
'69', 52,
'34', 51,
'23', 51), -1) TI,
S.CENTRO_MODULAR CM,
S.TUC,
NVL(S.A1, '00') A1,
NVL(S.A2, '00') A2,
NVL(S.A3, '00') A3,
NVL(S.A4, '00') A4,
NVL(S.A5, '00') A5,
NVL(S.A6, '00') A6,
S.IDUC,
DECODE(S.DATA_IMOB, NULL, 'NIM', 'AT1') AS SITCONT,
CAST(REPLACE(REPLACE (S.COMP,' m',''),',','.') AS NUMBER) COMP,
--CAST(coalesce(REPLACE (S.COMP,' m',''),'0')AS NUMBER) COMP,
S.DATA_IMOB AS DAT_IMO,
S.LOCAL_INSTALACAO AS "DESCR"
FROM MV_BDGD_SAP S
LEFT JOIN  BDGD_STAGING.MV_GENESIS_VS_PRI_SUBESTACAO SUB ON SUB.NR_SUBESTACAO = TO_NUMBER(SUBSTR(S.SUBESTACAO, 4))
LEFT JOIN BDGD_STAGING.MV_GENESIS_TAB_SAP_SUBESTACAO SAP_SUB ON SAP_SUB.NR_SUBESTACAO = SUB.NR_SUBESTACAO 
WHERE BDGD = 'EQSIAT' AND SUB.SG_SUBESTACAO IS NOT NULL AND S.COMP is not null
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UCBT_TESTE" ("COD_ID", "DIST", "PAC", "CEG", "PN_CON", "UNI_TR_D", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CLAS_SUB", "CNAE", "TIP_CC", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "DAT_CON", "CAR_INST", "LIV", "ARE_LOC", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "SEMRED", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
ucbt.cod_id,
ucbt.dist,
ucbt.pac,
ucbt.ceg,
ucbt.pn_con,
ucbt.uni_tr_d,
ucbt.ctmt,
ucbt.uni_tr_s,
ucbt.sub,
ucbt.conj,
ucbt.mun,
ucbt.lgrd,
ucbt.brr,
ucbt.cep,
COALESCE(ajust_clas_sub.cod_ref_clas_sub_clas_rcl,ucbt.clas_sub,'0') as clas_sub,
ucbt.cnae,
ucbt.tip_cc,
ucbt.fas_con,
ucbt.gru_ten,
ucbt.ten_forn,
ucbt.gru_tar,
ucbt.sit_ativ,
ucbt.dat_con,
ucbt.car_inst,
ucbt.liv,
ucbt.are_loc,
ucbt.ene_01,
ucbt.ene_02,
ucbt.ene_03,
ucbt.ene_04,
ucbt.ene_05,
ucbt.ene_06,
ucbt.ene_07,
ucbt.ene_08,
ucbt.ene_09,
ucbt.ene_10,
ucbt.ene_11,
ucbt.ene_12,
ucbt.dic,
ucbt.fic,
ucbt.semred,
ucbt.descr,
ucbt.geometry
from mv_bdgd_ucbt_temp ucbt
left join
(
select * from (select mv.*, rank() over (PARTITION BY cod_id order by ROWID) r from dif_clas_sub_bdgd_siga mv) 
where r = 1 and clas_sub = 'CO1' and cod_ref_clas_sub_clas_rcl like 'RU%' and entidade = 'UCBT'
) ajust_clas_sub  
on ajust_clas_sub.cod_id = ucbt.cod_id
where not exists (
select NULL from tab_uc_ip_temp ip where to_number(ip.cod_un_cons_uee) = to_number(ucbt.cod_id))
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_CTAT" ("COD_ID", "DIST", "NOM", "TEN_NOM", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND NEXT null
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT
  CTAT.COD_ID,
  CTAT.DIST,
  CTAT.NOM,
  CTAT.TEN_NOM,
  CAST(NULL AS VARCHAR2(255)) AS "DESCR"
  FROM BDGD_STAGING.MV_GENESIS_CTAT CTAT
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQTRD_TEMP" ("COD_ID", "DIST", "PAC_1", "PAC_2", "PAC_3", "CLAS_TEN", "POT_NOM", "LIG", "FAS_CON", "TEN_PRI", "TEN_SEC", "TEN_TER", "LIG_FAS_P", "LIG_FAS_S", "LIG_FAS_T", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "SITCONT", "DAT_IMO", "PER_FER", "PER_TOT", "R", "XHL", "XHT", "XLT", "DESCR", "R1")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select * from (select mv.*, rank() over (PARTITION BY cod_id order by fas_con) r1 from
(
select 
eqtrd.cod_id,
eqtrd.dist,
coalesce(ajust.pac_1,eqtrd.pac_1) as pac_1,
coalesce(ajust.pac_2,eqtrd.pac_2) as pac_2,
eqtrd.pac_3,
eqtrd.clas_ten,
eqtrd.pot_nom,
eqtrd.lig,
eqtrd.fas_con,
eqtrd.ten_pri,
eqtrd.ten_sec,
eqtrd.ten_ter,
eqtrd.lig_fas_p,
eqtrd.lig_fas_s,
eqtrd.lig_fas_t,
eqtrd.odi,
eqtrd.ti,
eqtrd.cm,
eqtrd.tuc,
eqtrd.a1,
eqtrd.a2,
eqtrd.a3,
eqtrd.a4,
eqtrd.a5,
eqtrd.a6,
eqtrd.sitcont,
eqtrd.dat_imo,
eqtrd.per_fer,
eqtrd.per_tot,
eqtrd.r,
eqtrd.xhl,
eqtrd.xht,
eqtrd.xlt,
eqtrd.descr
from mv_bdgd_eqtrd eqtrd left join 
(
select distinct local_inst,eqpto.sub,ctmt,ten_nom,uni_tr_s,'D-'||LOCAL_INST as pac_1,pac as pac_2 from
(select * from 
GEN_BDGD2019.mv_eqptos_subestacao 
where id_tipo_modulo != 1 and 
id_tn_nominal_eqpto < 6 and 
tipo_eqpto in ('TT')) EQPTO 
inner join (select sub,
cod_id as ctmt,
pac,
uni_tr_s,
decode(ten_nom,72,5,62,4,49,3,NULL) as ten_nom
from mv_bdgd_ctmt where cod_id in (
select min(COD_ID) from mv_bdgd_ctmt group by sub,ten_nom)) ctmt on ctmt.sub = eqpto.sub and ctmt.ten_nom = eqpto.id_tn_nominal_eqpto ) ajust 
on ajust.local_inst = substr(eqtrd.descr,1,10)
) mv
)
where r1 = 1
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_EQRE" ("COD_ID", "DIST", "PAC_1", "PAC_2", "POT_NOM", "TIP_REGU", "TEN_REG", "LIG_FAS_P", "LIG_FAS_S", "COR_NOM", "REL_TP", "REL_TC", "ODI", "TI", "CM", "TUC", "A1", "A2", "A3", "A4", "A5", "A6", "IDUC", "SITCONT", "DAT_IMO", "PER_FER", "PER_TOT", "R", "XHL", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
  SELECT SA.EQUIPAMENTO AS COD_ID,
         5697 AS DIST,
        
        CASE
        WHEN GEN.PAC_1 IS NULL THEN
        'D1'||SA.EQUIPAMENTO
        ELSE CAST (GEN.PAC_1 AS VARCHAR(20))END AS PAC_1,
        CASE
        WHEN GEN.PAC_2 IS NULL THEN
        'D2-'||SA.EQUIPAMENTO
        ELSE CAST (GEN.PAC_2 AS VARCHAR(20))END AS PAC_2,
        CASE
        WHEN A3 =29 THEN
        16
        WHEN A3 =33 THEN
        19
        WHEN A3 =40 THEN
        23
        WHEN A3 =41 THEN
        24
        WHEN A3 =49 THEN
        29
        WHEN A3 =53 THEN
        31
        WHEN A3 =54 THEN
        32
        WHEN A3 =55 THEN
        34
        WHEN A3 =59 THEN
        37
        WHEN A3 =60 THEN
        38
        WHEN A3 =61 THEN
        41
        WHEN A3 =62 THEN
        42
        WHEN A3 =63 THEN
        43
        WHEN A3 =92 THEN
        91
        ELSE 0
        END AS POT_NOM, 
        TIPO_REGULADOR(SA.LOCAL_INSTALACAO) AS TIP_REGU, 
        1 AS TEN_REG, 
        COALESCE(LIGF.LIG_FAS,'0')as LIG_FAS_P, 
        COALESCE(LIGF.LIG_FAS,'0')as LIG_FAS_S,
        CASE
        WHEN A4=05 THEN
        3
        WHEN A4=07 THEN
        5
        WHEN A4=10 THEN
        12
        WHEN A4=13 THEN
        18
        WHEN A4=16 THEN
        22
        WHEN A4=19 THEN
        23
        WHEN A4=22 THEN
        24
        ELSE 0
        END AS COR_NOM,
                
       CASE
           WHEN A2=15 THEN 13
           WHEN A2=13 THEN 13
           WHEN A2=25 THEN 6
           WHEN A2=34 THEN 3
           WHEN A2=69 THEN 2
         ELSE 0 END AS REL_TP,  
         
        CASE
        WHEN A4=05 THEN
        44
        WHEN A4=13 THEN
        55
        WHEN A4=22 THEN
        58
        WHEN A4=10 THEN
        53
        WHEN A4=16 THEN
        57
        ELSE 0
        END AS REL_TC,
        CASE
        WHEN SA.GRUPO_DE_PLANEJAMENTO='GRD' THEN
        '190400'
        ELSE SA.ODI
        END AS ODI,
        CASE
        WHEN GEN.ID_AREA_OBJETO='U' THEN
        '40'
        ELSE '41'
        END AS TI, '999' AS CM,
        SA.TUC, 
        SA.A1, 
        SA.A2, 
        SA.A3, 
        SA.A4, 
        SA.A5, 
        COALESCE (SA.A6,'00') AS A6, 
        SA.IDUC, 
        DECODE(DATA_IMOB, NULL, 'NIM', 'AT1') AS SITCONT, 
        data_imob as DAT_IMO,
        SA.PER_FER, 
        SA.PER_TOT, 
        SA.R, 
        SA.XHL,
        NVL(ENERGIA.ENE_01, 0) AS ENE_01,
        NVL(ENERGIA.ENE_02, 0) AS ENE_02,
        NVL(ENERGIA.ENE_03, 0) AS ENE_03,
        NVL(ENERGIA.ENE_04, 0) AS ENE_04,
        NVL(ENERGIA.ENE_05, 0) AS ENE_05,
        NVL(ENERGIA.ENE_06, 0) AS ENE_06,
        NVL(ENERGIA.ENE_07, 0) AS ENE_07,
        NVL(ENERGIA.ENE_08, 0) AS ENE_08,
        NVL(ENERGIA.ENE_09, 0) AS ENE_09,
        NVL(ENERGIA.ENE_10, 0) AS ENE_10,
        NVL(ENERGIA.ENE_11, 0) AS ENE_11,
        NVL(ENERGIA.ENE_12, 0) AS ENE_12,
        SA.LOCAL_INSTALACAO AS DESCR
    FROM MV_BDGD_SAP SA
    LEFT JOIN BDGD_STAGING.MV_GENESIS_EQRE GEN
        ON SA.LOCAL_INSTALACAO = GEN.COD_ID
    LEFT JOIN BDGD_STAGING.TAB_ENERGIA_EQRE ENERGIA
        ON ENERGIA.DESCR = SA.LOCAL_INSTALACAO
    INNER JOIN -- Completa o campo LIG_FAS de acordo com q ordem do registro 
        (SELECT SA.LOCAL_INSTALACAO,
         SA.EQUIPAMENTO,
            CASE
            WHEN TIPO_REGULADOR(SA.LOCAL_INSTALACAO)='T' THEN
            'ABCN'
            WHEN RA.RANK= 1 THEN
            'AB'
            WHEN RA.RANK= 2 THEN
            'BC'
            WHEN RA.RANK= 3 THEN
            'CA'
            ELSE '0'
            END AS LIG_FAS
        FROM MV_BDGD_SAP SA
        LEFT JOIN BDGD_STAGING.MV_GENESIS_EQRE GEN
            ON SA.EQUIPAMENTO = GEN.COD_ID
        INNER JOIN 
            (SELECT SA.LOCAL_INSTALACAO,
         SA.EQUIPAMENTO,
         RANK()
                OVER ( PARTITION BY SA.LOCAL_INSTALACAO
            ORDER BY  SA.EQUIPAMENTO DESC )as RANK
            FROM MV_BDGD_SAP SA
            WHERE SA.bdgd = 'EQRE'
                    AND SA.STATUS_SE NOT IN ('LIDI','MREL')
                    AND SA.LOCAL_INSTALACAO NOT LIKE 'AX-%' 
            GROUP BY  SA.LOCAL_INSTALACAO,EQUIPAMENTO ) RA
                ON RA.LOCAL_INSTALACAO = SA.LOCAL_INSTALACAO
                    AND RA.EQUIPAMENTO =SA.EQUIPAMENTO
            WHERE SA.bdgd = 'EQRE'
                    AND SA.STATUS_SE NOT IN ('LIDI','MREL')
                    AND SA.LOCAL_INSTALACAO NOT LIKE 'AX-%') LIGF
                ON LIGF.LOCAL_INSTALACAO = SA.LOCAL_INSTALACAO
                    AND LIGF.EQUIPAMENTO =SA.EQUIPAMENTO
            WHERE SA.bdgd = 'EQRE'
                    AND SA.STATUS_SE NOT IN ('LIDI','MREL')
                    AND SA.LOCAL_INSTALACAO LIKE 'RD-%'
                    
        -------------
        UNION
    (SELECT SA.EQUIPAMENTO AS COD_ID,
         5697 AS DIST,
        coalesce( p.pac_1,
        CASE
        WHEN SUB.NR_PTO_ELET_INICIO IS NULL THEN
        'D1-'||SA.EQUIPAMENTO
        ELSE CAST ('SE-'||SUB.NR_PTO_ELET_INICIO AS VARCHAR(20))END) AS PAC_1,
        coalesce( p.pac_2,
        CASE
        WHEN SUB.NR_PTO_ELET_FIM IS NULL THEN
        'D2-'||SA.EQUIPAMENTO
        ELSE CAST ('SE-'||SUB.NR_PTO_ELET_FIM AS VARCHAR(20))END) AS PAC_2,
        CASE
        WHEN A3 =29 THEN
        16
        WHEN A3 =33 THEN
        19
        WHEN A3 =40 THEN
        23
        WHEN A3 =41 THEN
        24
        WHEN A3 =49 THEN
        29
        WHEN A3 =53 THEN
        31
        WHEN A3 =54 THEN
        32
        WHEN A3 =55 THEN
        34
        WHEN A3 =59 THEN
        37
        WHEN A3 =60 THEN
        38
        WHEN A3 =61 THEN
        41
        WHEN A3 =62 THEN
        42
        WHEN A3 =63 THEN
        43
        WHEN A3 =92 THEN
        91
        ELSE 0
        END AS POT_NOM, 
        TIPO_REGULADOR(SA.LOCAL_INSTALACAO) AS TIP_REGU, 
        1 AS TEN_REG, COALESCE(LIGF.LIG_FAS,'0')as LIG_FAS_P, 
        COALESCE(LIGF.LIG_FAS,'0')as LIG_FAS_S,
        CASE
        WHEN A4=05 THEN
        3
        WHEN A4=07 THEN
        5
        WHEN A4=10 THEN
        12
        WHEN A4=13 THEN
        18
        WHEN A4=16 THEN
        22
        WHEN A4=19 THEN
        23
        WHEN A4=22 THEN
        24
        ELSE 0
        END AS COR_NOM,
        
        CASE
           WHEN A2=15 THEN 13
           WHEN A2=13 THEN 13
           WHEN A2=25 THEN 6
           WHEN A2=34 THEN 3
           WHEN A2=69 THEN 2
         ELSE 0 END AS REL_TP,        
        
         
        CASE
        WHEN A4 =05 THEN
        44
        WHEN A4=13 THEN
        55
        WHEN A4=22 THEN
        58
        WHEN A4=10 THEN
        53
        WHEN A4=16 THEN
        57
        WHEN A4=19 THEN
        58
        ELSE 0
        END AS REL_TC,
        CASE
        WHEN SA.GRUPO_DE_PLANEJAMENTO='GRD' THEN
        '190400'
        ELSE SA.ODI
        END AS ODI, SA.TI, SA.CENTRO_MODULAR AS CM, 
        SA.TUC, 
        SA.A1, 
        SA.A2, 
        SA.A3, 
        SA.A4, 
        SA.A5, 
        COALESCE (SA.A6,'00') AS A6, 
        SA.IDUC, 
        DECODE(DATA_IMOB, NULL,'NIM', 'AT1') AS SITCONT, 
        data_imob as DAT_IMO,
        SA.PER_FER, 
        SA.PER_TOT, 
        SA.R, 
        SA.XHL, 
        NVL(ENERGIA.ENE_01, 0) AS ENE_01,
        NVL(ENERGIA.ENE_02, 0) AS ENE_02,
        NVL(ENERGIA.ENE_03, 0) AS ENE_03,
        NVL(ENERGIA.ENE_04, 0) AS ENE_04,
        NVL(ENERGIA.ENE_05, 0) AS ENE_05,
        NVL(ENERGIA.ENE_06, 0) AS ENE_06,
        NVL(ENERGIA.ENE_07, 0) AS ENE_07,
        NVL(ENERGIA.ENE_08, 0) AS ENE_08,
        NVL(ENERGIA.ENE_09, 0) AS ENE_09,
        NVL(ENERGIA.ENE_10, 0) AS ENE_10,
        NVL(ENERGIA.ENE_11, 0) AS ENE_11,
        NVL(ENERGIA.ENE_12, 0) AS ENE_12,
        SA.LOCAL_INSTALACAO AS DESCR
    FROM MV_BDGD_SAP SA
    INNER JOIN GEN_BDGD2019.V_EQPTOS_SUBESTACAO SUB
        ON SA.LOCAL_INSTALACAO=SUB.local_inst
    --INNER JOIN BDGD_STAGING.TAB_NODE_SUB NS
       -- ON NS.LOC_INST_SAP=SUB.PAC2
    LEFT JOIN BDGD_STAGING.MV_GENESIS_EQRE GEN
        ON SA.EQUIPAMENTO = GEN.COD_ID
    LEFT JOIN BDGD_STAGING.TAB_ENERGIA_EQRE ENERGIA
        ON ENERGIA.DESCR = SA.LOCAL_INSTALACAO
    left join tab_aux_pac_se_re p --GAMBIARRA
            on p.loc_inst = SA.LOCAL_INSTALACAO
    INNER JOIN -- Completa o campo LIG_FAS de acordo com q ordem do registro 
        (SELECT SA.LOCAL_INSTALACAO,
         SA.EQUIPAMENTO,

        CASE
            WHEN TIPO_REGULADOR(SA.LOCAL_INSTALACAO)='T' THEN
            'ABCN'
            WHEN RA.RANK= 1 THEN
            'AB'
            WHEN RA.RANK= 2 THEN
            'BC'
            WHEN RA.RANK= 3 THEN
            'CA'
            ELSE '0'
            END AS LIG_FAS
        FROM MV_BDGD_SAP SA
        LEFT JOIN BDGD_STAGING.MV_GENESIS_EQRE GEN
            ON SA.EQUIPAMENTO = GEN.COD_ID
        INNER JOIN 
            (SELECT SA.LOCAL_INSTALACAO,
         SA.EQUIPAMENTO,
         RANK()
                OVER ( PARTITION BY SA.LOCAL_INSTALACAO
            ORDER BY  SA.EQUIPAMENTO DESC )as RANK
            FROM MV_BDGD_SAP SA
            WHERE SA.bdgd = 'EQRE'
                    AND SA.STATUS_SE NOT IN ('LIDI','MREL')
                    AND SA.LOCAL_INSTALACAO NOT LIKE 'AX-%'
            GROUP BY  SA.LOCAL_INSTALACAO,EQUIPAMENTO ) RA
                ON RA.LOCAL_INSTALACAO = SA.LOCAL_INSTALACAO
                    AND RA.EQUIPAMENTO =SA.EQUIPAMENTO
            WHERE SA.bdgd = 'EQRE'
                    AND SA.STATUS_SE NOT IN ('LIDI','MREL')
                    AND SA.LOCAL_INSTALACAO NOT LIKE 'AX-%') LIGF
                ON LIGF.LOCAL_INSTALACAO = SA.LOCAL_INSTALACAO
                    AND LIGF.EQUIPAMENTO =SA.EQUIPAMENTO
            WHERE SA.bdgd = 'EQRE'
                    AND SA.STATUS_SE NOT IN ('LIDI','MREL')
                    AND SA.LOCAL_INSTALACAO LIKE 'SE%' ))
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_SSDAT" ("COD_ID", "PN_CON_1", "PN_CON_2", "CONJ", "ARE_LOC", "DIST", "PAC_1", "PAC_2", "FAS_CON", "TIP_CND", "POS", "ODI_FAS", "TI_FAS", "SITCONTFAS", "ODI_NEU", "TI_NEU", "SITCONTNEU", "COMP", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT GEN.COD_ID,
       GEN.PN_CON_1,
       GEN.PN_CON_2,
       GEN.CONJ,
       GEN.ARE_LOC,
       GEN.DIST,
       COALESCE(C_AT_1.PAC, 'AT-'||GEN.PAC_1,'D1-'||GEN.COD_ID) PAC_1,
       COALESCE(C_AT_2.PAC, 'AT-'||GEN.PAC_2,'D2-'||GEN.COD_ID) PAC_2,
       GEN.FAS_CON,
       GEN.TIP_CND,
       GEN.POS,
       GEN.ODI_FAS,
       GEN.TI_FAS,
       GEN.SITCONTFAS,
       GEN.ODI_NEU,
       GEN.TI_NEU,
       GEN.SITCONTNEU,
       GEN.COMP,
       GEN.DESCR,
       GEN.GEOMETRY
FROM BDGD_STAGING.MV_GENESIS_SSDAT GEN 
     /*LEFT JOIN BDGD_STAGING.MV_GENESIS_CONECTIVIDADE_SE_AT C_AT_1
          ON GEN.PN_CON_1 = C_AT_1.ID
     LEFT JOIN BDGD_STAGING.MV_GENESIS_CONECTIVIDADE_SE_AT C_AT_2
          ON GEN.PN_CON_2 = C_AT_2.ID*/
    left join gen_bdgd2019.v_pac_ltr_1 C_AT_1
        on c_at_1.FID_SEGMENTO = gen.cod_id
    left join gen_bdgd2019.v_pac_ltr_2 C_AT_2
        on c_at_2.FID_SEGMENTO = gen.cod_id
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_PIP_TESTE" ("COD_ID", "DIST", "MUN", "CONJ", "SUB", "UNI_TR_S", "CTMT", "UNI_TR_D", "PN_CON", "CLAS_SUB", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR", "SIT_ATIV", "ARE_LOC", "PAC", "TIP_CC", "CAR_INST", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "LIV", "SEMRED", "DAT_CON", "DESCR")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select
cod_id,
dist,
mun,
conj,
sub,
uni_tr_s,
ctmt,
uni_tr_d,
pn_con,
clas_sub,
fas_con,
gru_ten,
ten_forn,
gru_tar,
sit_ativ,
are_loc,
pac,
tip_cc,
car_inst,
ene_01*1.03 as ene_01,
ene_02*1.03 as ene_02,
ene_03*1.03 as ene_03,
ene_04*1.03 as ene_04,
ene_05*1.03 as ene_05,
ene_06*1.03 as ene_06,
ene_07*1.03 as ene_07,
ene_08*1.03 as ene_08,
ene_09*1.03 as ene_09,
ene_10*1.03 as ene_10,
ene_11*1.03 as ene_11,
ene_12*1.03 as ene_12,
dic,
fic,
liv,
semred,
dat_con,
descr
from mv_bdgd_pip_temp
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNCRMT" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "POT_NOM", "PAC_1", "PAC_2", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "ARE_LOC", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS (
    SELECT 
    CAST (GEN.COD_ID as VARCHAR(20)) as COD_ID,
    GEN.DIST,
    GEN.FAS_CON,
    GEN.SIT_ATIV,
    GEN.TIP_UNID,
    CAST(SAP.POT_NOM as VARCHAR(3))as POT_NOM,
    CAST(GEN.PAC_1 as VARCHAR(20)) as PAC_1,
    CAST(GEN.PAC_2 as VARCHAR(20)) as PAC_2,
    --DECODE(NV.NOVO_ALIMENTADOR, NULL, GEN.CD_ALIMENTADOR, NV.NOVO_ALIMENTADOR,'0') AS "CTMT",
    COALESCE(NV.NOVO_ALIMENTADOR,GEN.CTMT,'0') AS "CTMT",
    --DECODE(NV.UN_TR_S, NULL, 'SE-'||BAR.ID_TT, NV.UN_TR_S) as UNI_TR_S,
    COALESCE(NV.UN_TR_S,BAR.LOC_INST_SAP_TT,'0') as "UNI_TR_S",
    COALESCE(GEN.SUB,'0') as SUB,
    GEN.CONJ,
    GEN.MUN,
    GEN.ARE_LOC,
    LOCINS.DAT_CON,
    GEN.BANC,
    GEN.POS,
    GEN.DESCR,
    GEN.GEOMETRY
    FROM
    (
    SELECT
    LOCAL_INSTALACAO,
    MAX(POT.COD_ID) as POT_NOM
    FROM MV_BDGD_SAP SAP
    LEFT JOIN TPOTRTV POT ON POT.POT=SAP.A4_TEC
    WHERE SAP.bdgd = 'EQCR' AND 
    SAP.LOCAL_INSTALACAO is not null AND
    LOCAL_INSTALACAO not like 'AX-%'
    GROUP BY LOCAL_INSTALACAO
    )   SAP
    INNER JOIN 
    (
    SELECT LOCAL_INSTALACAO, 
    MAX (
    CASE 
    WHEN SAP.DATA_CONEXAO='00/00/0000' THEN '01/01/0001' 
    ELSE SAP.DATA_CONEXAO END 
    )as  DAT_CON
    FROM MV_BDGD_SAP SAP
    WHERE SAP.bdgd = 'EQCR' AND 
    SAP.LOCAL_INSTALACAO is not null AND
    LOCAL_INSTALACAO NOT LIKE 'AX-%'
    GROUP BY LOCAL_INSTALACAO
    ) LOCINS ON LOCINS.LOCAL_INSTALACAO = SAP.LOCAL_INSTALACAO
    INNER JOIN
    (SELECT * FROM BDGD_STAGING.MV_GENESIS_UNCRMT)  GEN   ON  GEN.DESCR          = SAP.LOCAL_INSTALACAO
    LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT                   BAR   ON  BAR.CD_ALIMENTADOR = GEN.CD_ALIMENTADOR
    LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS             NV    ON  NV.ALIMENTADOR     = GEN.CD_ALIMENTADOR
    )
    UNION ALL
    (
    SELECT Q.*,
        GEO.SUB_GEOMETRY as GEOMETRY
    FROM
    (SELECT DISTINCT
    ''||SUB.ID as COD_ID,
    5697 as DIST,
    'ABC'  as FAS_CON,
    'AT'  as SIT_ATIV,
    10 as TIP_UNID,
    POT.COD_ID as POT_NOM,
    'SE-'||SUB.NR_PTO_ELET_INICIO as PAC_1,
    'SE-'||SUB.NR_PTO_ELET_FIM as PAC_2,
    --COALESCE(N_ALIM.novo_alimentador,'0') AS "CTMT"
    COALESCE(N_ALIM.NOVO_ALIMENTADOR,DECODE(SUB.ID_TIPO_MODULO,'1',SUB.CD_ALIMENTADOR,'0'),'0') AS CTMT,
    --COALESCE(N_ALIM.novo_alimentador,'0') AS "CTMT",
    COALESCE(N_ALIM.UN_TR_S,SUB.LOCAL_INST_TT,'0') AS "UNI_TR_S",
    --DECODE(SUB.LOCAL_INST_TT,NULL, '0', '' || SUB.LOCAL_INST_TT) AS UNI_TR_S, 
    COALESCE(SUB.SUB,'0') as SUB,
    SUB.CONJ,
    SUB.MUNICIPIO as MUN,
    TARE.COD_ID as ARE_LOC,
    CASE 
    WHEN SAP.DATA_CONEXAO='00/00/0000' THEN '01/01/0001' 
    ELSE SAP.DATA_CONEXAO END as  DAT_CON,
    0 as BANC,
    'PD' as POS,
    SAP.LOCAL_INSTALACAO as DESCR--,
    --SUB.SUB_GEOMETRY as GEOMETRY
    FROM
    (SELECT * FROM GEN_BDGD2019.MV_EQPTOS_SUBESTACAO WHERE tipo_eqpto = 'BC' AND id_tn_nominal_eqpto < '6') SUB
            LEFT JOIN (SELECT * FROM MV_BDGD_SAP WHERE OBJ_TECNICO = 'BC') SAP ON SUB.LOCAL_INST = SAP.LOCAL_INSTALACAO
            LEFT JOIN TARE TARE ON TARE.ARE_LOC=SUB.ARE_LOC
            LEFT JOIN TPOTRTV POT ON POT.POT=SAP.A4_TEC   
            LEFT JOIN
            (SELECT distinct 
            substr(alimentador,1,3) as SUB,
                novo_alimentador,un_tr_s 
            FROM BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ) N_ALIM  ON N_ALIM.SUB = SUB.SUB
    ) Q 
    LEFT JOIN GEN_BDGD2019.MV_EQPTOS_SUBESTACAO GEO ON GEO.ID = Q.COD_ID
    )
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UNCRMT_TEMP_1" ("COD_ID", "DIST", "FAS_CON", "SIT_ATIV", "TIP_UNID", "POT_NOM", "PAC_1", "PAC_2", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "ARE_LOC", "DAT_CON", "BANC", "POS", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS select 
uncrmt.cod_id,
uncrmt.dist,
uncrmt.fas_con,
uncrmt.sit_ativ,
uncrmt.tip_unid,
uncrmt.pot_nom,
coalesce(ajust.pac_1,uncrmt.pac_1) as pac_1,
coalesce(ajust.pac_2,uncrmt.pac_2) as pac_2,
coalesce(ajust.ctmt,uncrmt.ctmt) as ctmt,
coalesce(ajust.uni_tr_s,uncrmt.uni_tr_s) as uni_tr_s,
coalesce(ajust.sub,uncrmt.sub) as sub,
uncrmt.conj,
uncrmt.mun,
uncrmt.are_loc,
uncrmt.dat_con,
uncrmt.banc,
uncrmt.pos,
uncrmt.descr,
uncrmt.geometry
from mv_bdgd_uncrmt uncrmt left join 
(
select distinct local_inst,eqpto.sub,ctmt,ten_nom,uni_tr_s,'D-'||LOCAL_INST as pac_1,pac as pac_2 from
(select * from 
GEN_BDGD2019.mv_eqptos_subestacao 
where id_tipo_modulo != 1 and 
id_tn_nominal_eqpto < 6 and 
tipo_eqpto in ('BC')) EQPTO 
inner join (select sub,
cod_id as ctmt,
pac,
uni_tr_s,
decode(ten_nom,72,5,62,4,49,3,NULL) as ten_nom
from mv_bdgd_ctmt where cod_id in (
select min(COD_ID) from mv_bdgd_ctmt group by sub,ten_nom)) ctmt on ctmt.sub = eqpto.sub and ctmt.ten_nom = eqpto.id_tn_nominal_eqpto ) ajust 
on ajust.local_inst = uncrmt.descr
----------------------------------------------

  CREATE MATERIALIZED VIEW "BDGD"."MV_BDGD_UGMT" ("COD_ID", "PN_CON", "DIST", "PAC", "CEG", "CTMT", "UNI_TR_S", "SUB", "CONJ", "MUN", "LGRD", "BRR", "CEP", "CNAE", "FAS_CON", "GRU_TEN", "TEN_FORN", "SIT_ATIV", "DAT_CON", "POT_INST", "POT_CONT", "DEM_01", "DEM_02", "DEM_03", "DEM_04", "DEM_05", "DEM_06", "DEM_07", "DEM_08", "DEM_09", "DEM_10", "DEM_11", "DEM_12", "ENE_01", "ENE_02", "ENE_03", "ENE_04", "ENE_05", "ENE_06", "ENE_07", "ENE_08", "ENE_09", "ENE_10", "ENE_11", "ENE_12", "DIC", "FIC", "DESCR", "GEOMETRY")
  ORGANIZATION HEAP PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TS_BDGD" 
 VARRAY "GEOMETRY"."SDO_ELEM_INFO" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
 VARRAY "GEOMETRY"."SDO_ORDINATES" STORE AS SECUREFILE LOB 
  ( TABLESPACE "TS_BDGD" ENABLE STORAGE IN ROW CHUNK 8192
  CACHE  NOCOMPRESS  KEEP_DUPLICATES 
  STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
  BUILD IMMEDIATE
  USING INDEX 
  REFRESH FORCE ON DEMAND
  USING DEFAULT LOCAL ROLLBACK SEGMENT
  USING ENFORCED CONSTRAINTS DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE
  AS SELECT SIGA.COD_ID,
GEN.PN_CON,
SIGA.DIST,
DECODE(GEN.PAC,'5884785','SE-SE-6031336', '6063620','SE-SE-6017797','6063020','SE-SE-6031411','2677488', 'SE-SE-6024341','5780659', 'SE-SE-6010114', '5781352', 'SE-SE-6014647', GEN.PAC) as PAC,
SIGA.CEG,
COALESCE(ALI.NOVO_ALIMENTADOR, GEN.CTMT,'0') CTMT,
COALESCE(ALI.UN_TR_S, TAB.LOC_INST_SAP_TT,'0') UNI_TR_S,
GEN.SUB,
GEN.CONJ,
SIGA.MUN,
SIGA.LGRD,
SIGA.BRR,
SIGA.CEP,
NVL(SIGA.CNAE, 0) CNAE,
SIGA.FAS_CON,
SIGA.GRU_TEN,
SIGA.TEN_FORN,
SIGA.SIT_ATIV,
SIGA.DAT_CON,
TO_NUMBER(SIGA.POT_INST) POT_INST,
TO_NUMBER(SIGA.POT_CONT) POT_CONT,
NVL(SIGA.DEM_01,0) as DEM_01,
NVL(SIGA.DEM_02,0) as DEM_02,
NVL(SIGA.DEM_03,0) as DEM_03,
NVL(SIGA.DEM_04,0) as DEM_04,
NVL(SIGA.DEM_05,0) as DEM_05,
NVL(SIGA.DEM_06,0) as DEM_06,
NVL(SIGA.DEM_07,0) as DEM_07,
NVL(SIGA.DEM_08,0) as DEM_08,
NVL(SIGA.DEM_09,0) as DEM_09,
NVL(SIGA.DEM_10,0) as DEM_10,
NVL(SIGA.DEM_11,0) as DEM_11,
NVL(SIGA.DEM_12,0) as DEM_12,
NVL(SIGA.ENE_01,0) as ENE_01,
NVL(SIGA.ENE_02,0) as ENE_02,
NVL(SIGA.ENE_03,0) as ENE_03,
NVL(SIGA.ENE_04,0) as ENE_04,
NVL(SIGA.ENE_05,0) as ENE_05,
NVL(SIGA.ENE_06,0) as ENE_06,
NVL(SIGA.ENE_07,0) as ENE_07,
NVL(SIGA.ENE_08,0) as ENE_08,
NVL(SIGA.ENE_09,0) as ENE_09,
NVL(SIGA.ENE_10,0) as ENE_10,
NVL(SIGA.ENE_11,0) as ENE_11,
NVL(SIGA.ENE_12,0) as ENE_12,
TO_NUMBER(REPLACE(SIGA.DIC, ',', '.')) DIC,
TO_NUMBER(SIGA.FIC) FIC,
GEN.DESCR,
GEN.GEOMETRY
FROM MV_SIGA_UG_MT SIGA 
INNER JOIN BDGD_STAGING.MV_GENESIS_UCMT GEN     ON SIGA.COD_ID = GEN.COD_ID
LEFT JOIN BDGD_STAGING.TAB_ALI_BAR_TT TAB       ON GEN.CTMT = TAB.CD_ALIMENTADOR
LEFT JOIN BDGD_STAGING.TAB_BDGD_NOVOS_ALIMS ALI ON ALI.ALIMENTADOR = GEN.CTMT
----------------------------------------------

