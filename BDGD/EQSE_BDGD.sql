SELECT CAST(S.EQUIPAMENTO AS VARCHAR2(20)) AS COD_ID,
         '5697' AS DIST,
          COALESCE(to_char(gen.PAC_1), 'D1-'||gen.cod_id) PAC_1,
           COALESCE(to_char(gen.PAC_2), 'D2-'||gen.cod_id) PAC_2, 
           TCLATEN.COD_ID AS CLAS_TEN, 
           nvl(TCAPELFU.COD_ID, '0') AS ELO_FSV, 
         TMEIISO.COD_ID AS MEI_ISO, 
         nvl(GEN.fas_con, '0') AS FAS_CON,
         tcor.cod_id AS COR_NOM,
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
         TCLATEN.COD_ID AS CLAS_TEN,
         nvl(TCAPELFU.COD_ID, '0') AS ELO_FSV, --s.mei_iso, 
         TMEIISO.COD_ID AS MEI_ISO,
         nvl(coalesce(f.fas_con, s.fas_con), '0') AS FAS_CON,
         tcor.cod_id AS COR_NOM,
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
         TCLATEN.COD_ID AS CLAS_TEN, 
         nvl(TCAPELFU.COD_ID, '0') AS ELO_FSV, 
         TMEIISO.COD_ID AS MEI_ISO,
         nvl(gen.fas_con, '0') AS FAS_CON, 
         nvl(tcor.cod_id, '0') AS COR_NOM, 
         s.odi AS ODI, 
         TARE.BDGD AS TI, 
         nvl(s.centro_modular, '999') AS CM,
    CASE
    WHEN s.a2 < 34 THEN
    '00'
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
select s.equipamento COD_ID,
       '5697' DIST,
       SUB_GEN.NR_PTO_ELET_INICIO || '' PAC_1,
       SUB_GEN.NR_PTO_ELET_FIM || '' PAC_2,
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
INNER JOIN GEN_BDGD2019.V_EQPTOS_SUBESTACAO SUB_GEN
        ON SUB_GEN.LOCAL_INST = LOCAL_INSTALACAO
LEFT JOIN tcapelfu
    ON tcapelfu.cap = s.elo_fsv
LEFT JOIN tcor
    ON tcor.corr = s.cor_nom
LEFT JOIN TCLATEN 
    ON TCLATEN.TEN = S.CLAS_TEN
LEFT JOIN TMEIISO
    ON TMEIISO.DESCR = s.mei_iso
where bdgd = 'EQSE'