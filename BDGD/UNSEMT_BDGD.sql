SELECT   TO_CHAR(GEN.COD_ID) COD_ID,
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
            G.SHAPE GEOMETRY
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