WITH params AS (

  SELECT

    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR) AS start_utc,

    CURRENT_TIMESTAMP() AS end_utc

),



-- 1) Sesiones de las últimas {window_hours} horas

sessions_12h AS (

  SELECT DISTINCT m.session_id

  FROM `botmaker-bigdata.ext_metric_zenziya.message_metrics` AS m,

       params p

  WHERE m.session_creation_time >= p.start_utc

    AND m.session_creation_time <  p.end_utc

),



-- 2) Atributos por sesión

session_attrs AS (

  SELECT

    u.session_id,

    ANY_VALUE(u.session_creation_time) AS session_creation_time,

    MAX(

      CASE

        WHEN u.var_name IN ('cedula','numcedula')

        -- CORRECCIÓN AQUÍ: Usamos doble llave {{11}} para que Python no falle

        THEN REGEXP_EXTRACT(u.var_value, r'(\d{{11}})')

        ELSE NULL

      END

    ) AS cedula,

    MAX(CASE WHEN u.var_name = 'colaAtencion'   THEN u.var_value END) AS cola_atencion,

    MAX(CASE WHEN u.var_name = 'mail'           THEN u.var_value END) AS email,

    MAX(CASE WHEN u.var_name = 'nombrecliente'  THEN u.var_value END) AS nombre_cliente,

    MAX(CASE WHEN u.var_name = 'nombrecompleto' THEN u.var_value END) AS nombre_completo,

    MAX(CASE WHEN u.var_name = 'realwhatsappid' THEN u.var_value END) AS telefono,

    MAX(CASE WHEN u.var_name = 'fromname'       THEN u.var_value END) AS nombre_agente_bm

  FROM `botmaker-bigdata.ext_metric_zenziya.user_vars_metrics` AS u

  JOIN sessions_12h s

    USING (session_id)

  GROUP BY u.session_id

),



-- 3) Mensajes base

msgs AS (

  SELECT

    m.session_id,

    m.session_creation_time,

    m.creation_time              AS message_time,

    m.msg_from                   AS us_origen,

    m.message                    AS mensaje,

    m.audios_urls                AS audios,

    m.operator_name              AS operador_nombre,

    m.operator_email             AS operador_email,

    m.operator_role              AS operador_rol,

    m.queue                      AS departamento

  FROM `botmaker-bigdata.ext_metric_zenziya.message_metrics` AS m

  JOIN sessions_12h s

    USING (session_id)

  WHERE

    (m.message IS NOT NULL AND LENGTH(TRIM(m.message)) > 0)

    OR (m.audios_urls IS NOT NULL AND m.audios_urls != '')

),



-- 4) Agregados por sesión

msgs_agg_per_session AS (

  SELECT

    session_id,

    COUNT(*)                                      AS mensajes_count,

    COUNTIF(us_origen = 'user')                   AS mensajes_usuario,

    COUNTIF(us_origen != 'user')                  AS mensajes_sistema,

    MIN(message_time)                             AS first_msg_ts,

    MAX(message_time)                             AS last_msg_ts,

    COUNTIF(audios IS NOT NULL AND audios != '')  AS audios_count,

    COUNTIF(audios IS NOT NULL AND audios != '') > 0 AS tiene_audio,



    ARRAY_AGG(

      STRUCT(

        message_time,

        us_origen,

        mensaje,

        audios,

        operador_nombre,

        operador_email,

        operador_rol,

        departamento

      )

      ORDER BY message_time ASC

    ) AS mensajes,



    ARRAY_AGG(

      IF(

        audios IS NOT NULL AND audios != '',

        STRUCT(

          message_time,

          us_origen,

          audios,

          operador_nombre,

          operador_email,

          operador_rol,

          departamento

        ),

        NULL

      )

      IGNORE NULLS

      ORDER BY message_time ASC

    ) AS audios_detalle



  FROM msgs

  GROUP BY session_id

),



-- 5) Arrays de valores únicos

deps_per_session AS (

  SELECT session_id, ARRAY_AGG(DISTINCT departamento IGNORE NULLS) AS departamentos_distintos FROM msgs GROUP BY session_id

),

operators_per_session AS (

  SELECT session_id, ARRAY_AGG(DISTINCT operador_nombre IGNORE NULLS) AS operadores_distintos FROM msgs GROUP BY session_id

),

emails_per_session AS (

  SELECT session_id, ARRAY_AGG(DISTINCT operador_email IGNORE NULLS) AS operadores_emails_distintos FROM msgs GROUP BY session_id

),

roles_per_session AS (

  SELECT session_id, ARRAY_AGG(DISTINCT operador_rol IGNORE NULLS) AS operadores_roles_distintos FROM msgs GROUP BY session_id

),



-- 6) Conversaciones por agente

agent_convs AS (

  SELECT

    session_id,

    TRIM(operador_nombre) AS operador_nombre,

    TRIM(operador_email)  AS operador_email,

    TRIM(operador_rol)    AS operador_rol,

    TRIM(departamento)    AS departamento,

    ARRAY_AGG(STRUCT(

      message_time,

      us_origen,

      mensaje,

      audios

    ) ORDER BY message_time) AS mensajes

  FROM msgs

  WHERE operador_nombre IS NOT NULL OR operador_email IS NOT NULL

  GROUP BY

    session_id, operador_nombre, operador_email, operador_rol, departamento

),

agent_convs_agg AS (

  SELECT

    session_id,

    ARRAY_AGG(STRUCT(

      operador_nombre,

      operador_email,

      operador_rol,

      departamento,

      mensajes

    )) AS conversaciones_por_agente

  FROM agent_convs

  GROUP BY session_id

)



-- 7) SELECT FINAL

SELECT

  s.cedula,

  s.session_id,

  s.session_creation_time,

  s.cola_atencion,

  COALESCE(s.nombre_cliente, s.nombre_completo) AS nombre_cliente,

  s.nombre_completo,

  s.email,

  s.telefono,

  s.nombre_agente_bm,



  mAgg.mensajes_count,

  mAgg.mensajes_usuario,

  mAgg.mensajes_sistema,

  mAgg.first_msg_ts,

  mAgg.last_msg_ts,



  deps.departamentos_distintos,

  ops.operadores_distintos,

  ems.operadores_emails_distintos,

  roles.operadores_roles_distintos,



  mAgg.tiene_audio,

  mAgg.audios_count,



  mAgg.mensajes,

  mAgg.audios_detalle,

  

  ac.conversaciones_por_agente



FROM session_attrs s

JOIN msgs_agg_per_session mAgg USING (session_id)

LEFT JOIN deps_per_session     deps  USING (session_id)

LEFT JOIN operators_per_session ops   USING (session_id)

LEFT JOIN emails_per_session    ems   USING (session_id)

LEFT JOIN roles_per_session     roles USING (session_id)

LEFT JOIN agent_convs_agg       ac    USING (session_id)

WHERE mAgg.mensajes_count > 4

ORDER BY mAgg.last_msg_ts;