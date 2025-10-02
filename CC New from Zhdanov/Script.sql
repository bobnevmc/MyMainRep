with pre_square_talk_campaign AS (
    SELECT
        campaign_id,
        list_id,
        name AS campaign_name
    FROM BI_SYSTEM_PROD.stg_plumber.square_talk_campaign
    QUALIFY 1 = ROW_NUMBER() OVER (
            PARTITION BY campaign_id, list_id
            ORDER BY created_at DESC
        )
),
    base as (select s.PLAYER_ID,
                     s.BRAND_NAME,
                     s.BRAND_NAME || ':' || s.PLAYER_ID uid_in_casino,
                     pl.COUNTRY_ALIAS,
                     SEGMENT_NAME,
                     ENLISTED_FROM as                   START_AT,
                     DELISTED_AT,
                     CASE
                         WHEN DELISTED_AT > CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP
                         ELSE DATEADD(minute, 30, DELISTED_AT) //чтобы дать время звонкам еще позвонить
                         END       AS                   END_AT
              // from bi_system_prod.data_sandbox.leads_segments_dm s //
              from bi_system_prod.data_mart.leads_segments_dm s
                       left join BI_SYSTEM_PROD.DATA_MODEL.PLAYER_DIM pl
                                 ON s.BRAND_NAME = pl.BRAND_NAME and s.PLAYER_ID = pl.PLAYER_ID
),
     CALLS_STAT as (SELECT talk_calls_history.uid_in_casino                         AS uid_in_casino,
                           1 AS player_call_c,
                           max(case when talk_calls_history.agent_name is  not null
                                then 1
                                else 0 end)                                          as player_call_c_pickup,
                           max(case when (status_label = 'Sale' or LEAD_STATUS_LABEL = 'Sale')
                                        and AGENT_NAME is not null then 1 else 0 end)
                                                                                    as player_call_c_sale //длительность 10 сек убрали, согласовано с КЦ
                    FROM BI_SYSTEM_PROD.stg_plumber.square_talk_calls_history talk_calls_history
                    left join pre_square_talk_campaign
                        on pre_square_talk_campaign.CAMPAIGN_ID = talk_calls_history.CAMPAIGN_ID
                        and pre_square_talk_campaign.LIST_ID = talk_calls_history.LIST_ID
                    inner join base on base.uid_in_casino = talk_calls_history.uid_in_casino
                   and (talk_calls_history.called_at between base.START_AT and base.END_at)
                    and pre_square_talk_campaign.campaign_name ilike '%cc_medium%' -- тут в будущем надо проверять чтобы и кампании и сегменты совпадали по названию
                group by 1,2
        ),
     pre_new_lead AS (SELECT distinct talk_lead.UID_IN_CASINO,
                                      1 as IS_ADDED
                      FROM BI_SYSTEM_PROD.stg_plumber.square_talk_lead talk_lead
                               inner join base using (uid_in_casino)
                      WHERE LENGTH(talk_lead.uid_in_casino) - LENGTH(REPLACE(talk_lead.uid_in_casino, ':', '')) = 1
                        AND TRY_TO_NUMBER(TO_VARCHAR(SPLIT(talk_lead.uid_in_casino, ':')[1])) IS NOT NULL
                        and talk_lead.CREATED_AT between base.START_AT and base.END_at),
     bonuses as (select BRAND,
                        b.PLAYER_ID,
                        count(BONUS)                as BONUS_ISSUED_C,
                        count(distinct b.PLAYER_ID) as PLAYER_BONUS_ISSUED_C,
                 from BI_SYSTEM_PROD.DATA_MART.CRM_BONUS_DM b
                          inner join base ON b.BRAND = base.BRAND_NAME and b.PLAYER_ID = base.PLAYER_ID
                     and BONUS_ISSUED_TS > START_AT //бонус выдан позже начала сегмента
                 where BONUS_NAME ilike '%call%'
                   and BONUS_NAME ilike '%risk%'
                   and BONUS != 'freeSpinPlayed'
                 group by BRAND, b.PLAYER_ID),
     deposit_bonus as (SELECT ab.PLAYER_ID,
                              sum(tr.AMOUNT_EUR) / 100000 as BONUS_DEPOSIT_S
                       FROM BI_SYSTEM_PROD.STG_ARAMUZ_BE.ACCOUNT_BONUS ab
                                inner join base ON ab.PLAYER_ID = base.PLAYER_ID and ab.CREATED_AT > base.START_AT
                                INNER JOIN BI_SYSTEM_PROD.STG_ARAMUZ_BE.BP_WL_TRANSACTION tr
                                           ON COALESCE(ab.BP_WL_TRANSACTION_ID, ab.DEPOSIT_ACCOUNT_TRANSACTION_ID) =
                                              tr.ID //согласовали с Мишей Шевцовым, что ИД депозита в 2 полях может быть
                       WHERE OPERATION = 'deposit'
                         and tr.STATUS = 'done'
                         and ab.TYPE <> 'freeSpinPlayed'
                         and (
                           AB.NAME ILIKE 'call%'
                           )
                       group by ab.PLAYER_ID),
     result as (select base.PLAYER_ID,
                       BRAND_NAME,
                       COUNTRY_ALIAS,
                       uid_in_casino,
                       SEGMENT_NAME,
                       START_AT,
                       DELISTED_AT,
                       coalesce(IS_ADDED, 0)              as IS_ADDED,
                       coalesce(player_call_c, 0)         as player_call_c,
                       coalesce(player_call_c_pickup, 0)  as player_call_c_pickup,
                       coalesce(player_call_c_sale, 0)    as player_call_c_sale,
                       coalesce(BONUS_ISSUED_C, 0)        as BONUS_ISSUED_C,
                       coalesce(PLAYER_BONUS_ISSUED_C, 0) as PLAYER_BONUS_ISSUED_C,
                       coalesce(BONUS_DEPOSIT_S, 0)       as BONUS_DEPOSIT_S
                from base
                         left join pre_new_lead using (uid_in_casino)
                         left join CALLS_STAT using (uid_in_casino)
                         left join bonuses ON bonuses.BRAND = base.BRAND_NAME and bonuses.PLAYER_ID = base.PLAYER_ID
                         left join deposit_bonus ON deposit_bonus.PLAYER_ID = base.PLAYER_ID)
select * from result