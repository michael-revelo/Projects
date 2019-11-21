-- Date & Marketing Attribute Matrix

with population as (SELECT distinct createddate::timestamp_ltz::date as date, 
                                    marketing_channel
                    FROM marketing
                                CROSS JOIN
                   (SELECT distinct lower(channel) as marketing_channel
                    FROM marketing))
                   
-- Existing Marketing Attributes in SFDC

    ,touches as (SELECT * FROM
                (SELECT createddate::timestamp_ltz as date,
                        lower(channel) as marketing_channel,
                        lower(subchannel) as marketing_subchannel,
                        CASE WHEN lead__c is not null and account__c is null then lead__c
                             WHEN account__c is not null then account__c
                             END as id,
                        timestamp_from_parts(to_date(createddate::timestamp_ltz),to_time(hour(createddate::timestamp_ltz)||':'||floor(minute(createddate::timestamp_ltz), -.5)||':00')) as mod_createdate,
                        CASE WHEN lead__c is not null and account__c is null then row_number() over (partition by lead__c, marketing_channel, mod_createdate order by createddate desc)
                             WHEN account__c is not null then row_number() over (partition by account__c, marketing_channel, mod_createdate order by createddate desc)
                             END as rank
                 FROM marketing
                 WHERE isdeleted = 'False' and (account__c is not null or lead__c is not null))
                 WHERE rank = 1)
                 
-- Existing demo records 

    ,demo as (SELECT * FROM
             (SELECT CASE WHEN lead__c is not null and account__c is null then lead__c
                          WHEN account__c is not null then account__c
                          END as id,
                     CASE WHEN lead__c is not null and account__c is null then row_number() over (partition by lead__c order by createddate desc)
                          WHEN account__c is not null then row_number() over (partition by account__c order by createddate desc)
                          END as rank,
                     createddate::timestamp_ltz::date as booked_date,
                     attended_date_time__c::timestamp_ltz::date as attended_date
              FROM demo
              WHERE recordtypeid = 'xyz' and isdeleted = 'False')
              WHERE rank = 1)
          
-- Daily marketing spend by channel

    ,spend as (SELECT date,
                      lower(marketing_channel) as marketing_channel,
                      sum(spend) as spend,
                      sum(clicks) as clicks,
                      sum(impressions) as impressions
               FROM spend
               GROUP BY date,
                        marketing_channel)

-- Daily spend used to get prior spend (get prior spend as MoM inside of left join)

    ,prior_spend as (SELECT date,
                            lower(marketing_channel) as marketing_channel,
                            sum(spend) as spend,
                            sum(clicks) as clicks,
                            sum(impressions) as impressions
                     FROM spend
                     GROUP BY date,
                              marketing_channel)

-- Most recent enrollment, from the opportunity object in SFDC, to provide a cutoff date for marketing attributes

    ,most_recent_enrollment as (SELECT * FROM 
                               (SELECT o.account, 
                                       o.id, 
                                       to_date(o.closedate) as most_recent_enrollment_date,
                                       o.amount,
                                       p.family as product_family,
                                       o.pod__c as pod,
                                       o.isclosed,
                                       o.loss_type__c as loss_type,
                                       row_number() over (partition by accountid order by o.closedate desc) as rank
                                FROM opportunity o  
                                     LEFT JOIN line_item li ON li.opportunityid = o.id AND li.isdeleted = 'False'
                                     LEFT JOIN product p ON p.id = li.product2id AND p.isdeleted = 'False'
                                     LEFT JOIN account a ON o.accountid=a.id AND a.isdeleted = 'False'
                                WHERE o.isclosed = true
                                      AND o.isdeleted = 'False'
                                      AND o.recordtypeid = 'xyz'  --Enrolled Opportunities
                                      AND o.closedate IS NOT null
                                      AND lower(P.FAMILY) NOT IN ('x', 'y', 'z'))
                                WHERE rank = 1)

-- Marketing channel arrays and weights from both the lead and account objects

    ,all_arrays as ((SELECT id,
                            org,
                            industry,
                            array_agg(marketing_channel) within group (order by createddate asc) as "Marketing Channel Array",
                            array_agg(marketing_channel || ' : ' || createddate) within group (order by createddate asc) as "Marketing Channel Array with Touch Date",
                            array_size("Marketing Channel Array") as "MA Count",
                            1 / "MA Count" as "Weight per MA"
                     FROM (SELECT lead.id,
                                  lead.org,
                                  lead.vertical,
                                  lower(ma.channel) as marketing_channel,
                                  lower(ma.subchannel) as marketing_subchannel,
                                  ma.createddate::timestamp_ltz::date as createddate,
                                  timestamp_from_parts(to_date(ma.createddate::timestamp_ltz),to_time(hour(ma.createddate::timestamp_ltz)||':'||floor(minute(ma.createddate::timestamp_ltz), -.5)||':00')) as mod_createdate,
                                  ROW_NUMBER () OVER (PARTITION BY lead.id, marketing_channel, mod_createdate ORDER BY ma.createddate::timestamp_ltz) as rank
                           FROM lead
                                LEFT JOIN marketing ma on ma.lead__c = lead.id and ma.isdeleted = 'False'
                          WHERE ma.marketing_channel__c is not null)
                    WHERE rank = 1
                    GROUP BY id, org, vertical)

                    UNION ALL

                    (SELECT id,
                            org,
                            vertical,
                            array_agg(marketing_channel) within group (order by createddate asc) as "Marketing Channel Array",
                            array_agg(marketing_channel || ' : ' || createddate) within group (order by createddate asc) as "Marketing Channel Array with Touch Date",
                            array_size("Marketing Channel Array") as "MA Count",
                            1 / "MA Count" as "Weight per MA"
                     FROM (SELECT account.id,
                                  account.org,
                                  account.vertical,
                                  lower(ma.channel) as marketing_channel,
                                  lower(ma.subchannel) as marketing_subchannel,
                                  ma.createddate::timestamp_ltz::date as createddate,
                                  most_recent_enrollment.most_recent_enrollment_date,
                                  case when most_recent_enrollment.most_recent_enrollment_date is null or most_recent_enrollment.most_recent_enrollment_date >= ma.createddate::timestamp_ltz::date then 'before'
                                       else 'after' end as "MA Before Enrollment",
                                  timestamp_from_parts(to_date(ma.createddate::timestamp_ltz),to_time(hour(ma.createddate::timestamp_ltz)||':'||floor(minute(ma.createddate::timestamp_ltz), -.5)||':00')) as mod_createdate,
                                  ROW_NUMBER () OVER (PARTITION BY account.id, ma.channel, ma.subchannel, mod_createdate ORDER BY ma.createddate::timestamp_ltz::date) as rank
                           FROM account
                                LEFT JOIN marketing ma on ma.account__c = account.id and ma.isdeleted = 'False'
                                LEFT JOIN most_recent_enrollment on most_recent_enrollment.accountid = account.id
                           WHERE marketing_channel is not null and "MA Before Enrollment" = 'before')
                     WHERE rank = 1
                     GROUP BY id, org, vertical)) 

-- Counts of opportunities, enrollments, LITs, and churns

    ,counts as (SELECT distinct all_arrays.*,
                                most_recent_enrollment.pod,
                                booked_date,
                                attended_date,
                                most_recent_enrollment.amount,
                                most_recent_enrollment.most_recent_enrollment_date,
                                case when most_recent_enrollment.isclosed = 'True' and lower(most_recent_enrollment.loss_type) = 'loss_1' then 1 else 0 end as "Loss 1 Count",
                                case when most_recent_enrollment.isclosed = 'True' and lower(most_recent_enrollment.loss_type) = 'loss_2' then 1 else 0 end as "Loss 2 Count"
                FROM all_arrays LEFT JOIN most_recent_enrollment on most_recent_enrollment.accountid = all_arrays.id
                                LEFT JOIN demo on demo.id = all_arrays.id)

-- Prospect eLTV from eLTV by Org Day

    ,prospect as (SELECT date,
                         marketing_channel, 
                         avg(eltv_1) as prospect_eLTV
                  FROM eltv
                  WHERE marketing_channel is not null and enrollment_date is null
                  GROUP BY date, marketing_channel)

-- Account eLTV from eLTV by Org Day

    ,account as (SELECT date, 
                        marketing_channel, 
                        avg(eltv_2) as account_eLTV 
                 FROM eltv
                 WHERE marketing_channel is not null and enrollment_date is not null
                 GROUP BY date, marketing_channel)
  
-- Weights on a moving 30 day before and after window

    ,weights as (SELECT t1.*,
                        (SELECT count(id)
                         FROM touches t2
                         WHERE t2.id = t1.id and t2.date < t1.date and t2.date >= dateadd('day', -30, t1.date)) as prior_30,
                         (SELECT count(id)
                          FROM touches t2
                          WHERE t2.id = t1.id and t2.date >= t1.date and t2.date <= dateadd('day', 30, t1.date)) as next_30,
                          1 / (prior_30 + next_30) as "New Weight per MA"
                  FROM touches t1)
                   
SELECT population.date,
       population.marketing_channel,
       touches.marketing_subchannel,
       population.marketing_channel || ' : ' || touches.marketing_subchannel as for_filter,
       touches.id,
       counts.org,
       counts.vertical,
       counts."Marketing Channel Array",
       counts."Marketing Channel Array with Touch Date",
       counts."MA Count",
       counts."Weight per MA",
       weights."New Weight per MA",
       case when booked_date is not null and population.date <= counts.booked_date then 1 else 0 end as "Demo Booked Count",
       case when attended_date is not null and population.date <= counts.attended_date then 1 else 0 end as "Demo Attended Count",
       case when counts.most_recent_enrollment_date is not null and population.date <= counts.most_recent_enrollment_date then 1 else 0 end as "Enrolled Count",
       population.date as lead_date,
       counts.booked_date,
       counts.attended_date,
       counts.most_recent_enrollment_date,
       counts.amount as ASP,
       counts.pod,
       counts."Loss 1 Count",
       counts."Loss 2 Count",
       avg(spend.spend) as spend,
       avg(prior_spend.spend) as prior_spend,
       avg(spend.clicks) as clicks,
       avg(spend.impressions) as impressions,
       prospect.prospect_eLTV,
       account.account_eLTV
FROM population LEFT JOIN touches on touches.date::date = population.date and touches.marketing_channel = population.marketing_channel
                LEFT JOIN spend on spend.date = population.date and spend.marketing_channel = population.marketing_channel
                LEFT JOIN prior_spend on prior_spend.date = dateadd('month', -1, population.date) and prior_spend.marketing_channel = touches.marketing_channel
                LEFT JOIN counts on counts.id = touches.id
                LEFT JOIN weights on weights.date::date = population.date and weights.marketing_channel = population.marketing_channel and touches.id = weights.id
                LEFT JOIN prospect on prospect.marketing_channel = population.marketing_channel and prospect.org_signup_date = population.date
                LEFT JOIN account on account.marketing_channel = population.marketing_channel and account.org_signup_date = population.date
WHERE touches.id is not null
GROUP BY population.date,
         touches.date,
         population.marketing_channel,
         touches.marketing_subchannel,
         touches.id,
         counts.org,
         counts.vertical,
         counts."Marketing Channel Array",
         counts."Marketing Channel Array with Touch Date",
         counts."MA Count",
         counts."Weight per MA",
         weights."New Weight per MA",
         counts.most_recent_enrollment_date,
         counts.amount,
         counts.booked_date,
         counts.attended_date,
         counts.pod,
         counts."Loss 1 Count",
         counts."Loss 2 Count",
         prospect.prospect_eLTV,
         account.account_eLTV,
         counts.most_recent_enrollment_date