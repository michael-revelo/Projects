-- Pulls all last touch marketing attributions partitioned by org

with last_touch as (SELECT date,
                           coalesce(marketing_channel, 'null') as marketing_channel,
                           1 as leads
                    FROM (SELECT ma.createddate::timestamp_ltz::date as date,
                                 lower(ma.channel) as marketing_channel,
                                 lower(ma.subchannel) as marketing_subchannel,
                                 CASE WHEN ma.lead__c is not null and ma.account__c is null then ma.lead__c
                                      WHEN ma.account__c is not null then ma.account__c
                                      END as id,
                                 CASE WHEN ma.lead__c is not null and ma.account__c is null then l.org_id__c
                                      WHEN ma.account__c is not null then a.org_id__c
                                      END as organization_id,
                                 case when a.vertical is null then lower(l.vertical) else lower(a.vertical) end as vertical,
                                 case when a.pod__c is null then lower(l.pod__c) else lower(a.pod__c) end as pod,
                                 CASE WHEN ma.lead__c is not null and ma.account__c is null then row_number() over (partition by ma.lead__c order by ma.createddate desc)
                                      WHEN ma.account__c is not null then row_number() over (partition by ma.account__c order by ma.createddate desc)
                                      END as rank
                          FROM marketing ma
                               LEFT JOIN lead l on l.id = ma.lead__c 
                               LEFT JOIN account a on a.id = ma.account__c 
                          WHERE ma.isdeleted = 'False' and (ma.account__c is not null or ma.lead__c is not null))
                          WHERE rank = 1) 

-- All marketiung touches related to the account object in Salesforce as the foundation to find the last touch before enrollment

    ,account_touches as (SELECT datetime, 
                                account__c as id,
                                organization_id,
                                category,
                                coalesce(marketing_channel, 'null') as marketing_channel
                         FROM (SELECT ma.createddate::timestamp_ltz as datetime,
                                      a.org as organization_id,
                                      ma.account__c,
                                      case when mcg.super_group is not null then mcg.super_group else 'Organic Other' end as category,
                                      lower(ma.channel) as marketing_channel,
                                      timestamp_from_parts(to_date(ma.createddate::timestamp_ltz),to_time(hour(ma.createddate::timestamp_ltz)||':'||floor(minute(ma.createddate::timestamp_ltz), -.5)||':00')) as mod_createdate,
                                      row_number() over (partition by ma.account__c, ma.marketing_channel, mod_createdate order by ma.createddate desc) as rank
                               FROM marketing ma
                               LEFT JOIN account a on a.id = ma.account__c and a.isdeleted = false
                               LEFT JOIN channel mcg on lower(mcg.marketing_channel) = lower(ma.marketing_channel)
                               WHERE ma.isdeleted = 'False' and ma.account__c is not null)
                               WHERE rank = 1)

-- Cross joins every day with every marketing channel to provide a proper left join foundation

    ,lead_foundation as (SELECT distinct cd.cal_date as date, 
                                         coalesce(marketing_channel, 'null') as marketing_channel
                         FROM "DATAZOO"."BIZ_REPORTS"."CALENDAR_DATES" cd
                         CROSS JOIN (SELECT distinct lower(marketing_channel) as marketing_channel FROM marketing) as ma
                         WHERE date <= dateadd('day', -1, current_date()))

-- Aggregate last touch lead counts by time period

    ,leads as (SELECT lf.date,
                      case when mcg.super_group is not null then mcg.super_group else 'Organic Other' end as category,
                      coalesce(sum(leads) over (partition by date_trunc('day', lf.date), category), 0) as daily_leads,
                      coalesce(sum(leads) over (partition by date_trunc('week', lf.date), category), 0) as weekly_leads,
                      coalesce(sum(leads) over (partition by date_trunc('quarter', lf.date), category), 0) as quarterly_leads,
                      coalesce(sum(leads) over (partition by date_trunc('month', lf.date), category), 0) as monthly_leads,
                      coalesce(sum(leads) over (partition by date_trunc('year', lf.date), category), 0) as yearly_leads
               FROM lead_foundation lf
               LEFT JOIN channel mcg on lower(mcg.marketing_channel) = lf.marketing_channel
               LEFT JOIN last_touch lt on lt.date = lf.date and lt.marketing_channel = lf.marketing_channel)

-- Spend by marketing channel group for each time period

    ,lead_spend as (SELECT lf.date,
                           case when mcg.super_group is not null then mcg.super_group else 'Other' end as category,
                           round(coalesce(sum(spend) over (partition by date_trunc('day', lf.date), category), 0)) as daily_spend,
                           round(coalesce(sum(spend) over (partition by date_trunc('week', lf.date), category), 0)) as weekly_spend,
                           round(coalesce(sum(spend) over (partition by date_trunc('quarter', lf.date), category), 0)) as quarterly_spend,
                           round(coalesce(sum(spend) over (partition by date_trunc('month', lf.date), category), 0)) as monthly_spend,
                           round(coalesce(sum(spend) over (partition by date_trunc('year', lf.date), category), 0)) as yearly_spend
                    FROM lead_foundation lf
                    LEFT JOIN channel mcg on lower(mcg.marketing_channel) = lf.marketing_channel
                    LEFT JOIN spend ms on ms.date = lf.date and lower(ms.marketing_channel) = lower(lf.marketing_channel)
                    GROUP BY lf.date, category, ms.spend)
             
-- Cross joins every day with every plan tier to provide a proper left join foundation

    ,enrollments_foundation_tier as (SELECT distinct cd.cal_date as date, 
                                                     coalesce(ppha.tier, 'legacy') as tier
                                     FROM dates cd 
                                     CROSS JOIN (SELECT distinct tier FROM table  WHERE tier not in ('x', 'y', 'z') or tier is null) as ppha
                                     WHERE date <= dateadd('day', -1, current_date())) 
                                     
-- Cross joins every day with every marketing channel category to provide a proper left join foundation
                                     
    ,enrollments_foundation_mc as (SELECT distinct cd.cal_date as date, 
                                                   mcg.super_group as category
                                   FROM dates cd 
                                   CROSS JOIN (SELECT distinct super_group FROM channel) as mcg
                                   WHERE date <= dateadd('day', -1, current_date())) 

-- Find all saas enrollments from ppha minus internal accounts 

    ,enrollments as (SELECT * FROM
                    (SELECT ppha.id,
                            coalesce(tier, 'legacy') as tier,
                            ppha.enrollment_date::timestamp_ltz as enrollment_datetime,
                            round((saas + featuresum)) as asp,
                            enrollment,
                            row_number() over (partition by ppha.id, enrollment_datetime::date order by enrollment_datetime desc) as rank
                     FROM table ppha
                     LEFT JOIN tag on tag.organization_id = ppha.organization_id and tag.name = 'internal'
                     LEFT JOIN table ppe on ppe.payment_plan_id = ppha.organization_payment_plan_id
                     LEFT JOIN table oi on oi.organization_id = ppha.organization_id
                     LEFT JOIN tabnle o on o.id = ppha.organization_id
                     WHERE enrollment = 1 and entry_point not ilike '%xyz%'
                           AND (tag.name <> 'xyz' or tag.name is null) AND (ppe.billing_suspended = false or ppe.billing_suspended is null)
                           AND ((oi.email not ilike '%xyz%' and oi.email not ilike '%@xyz%') or oi.email is null)
                           AND ((o.email not ilike '%xyz%' and o.email not ilike '%@xyz%') or o.email is null))
                    WHERE rank = 1)

-- Matches the marketing channel before an enrollment from Saleforce to the enrollment in the database

    ,last_enrollment_touch as (SELECT e.enrollment_datetime::date as enrollment_date,
                                      e.id,
                                      coalesce(category, 'Organic Other') as category,
                                      channel.marketing_channel,
                                      enrollment
                               FROM enrollments e
                               LEFT JOIN (SELECT enrollment_date,
                                                 id,
                                                 category,
                                                 marketing_channel
                                          FROM (SELECT e.enrollment_datetime::date as enrollment_date,
                                                       e.id,
                                                       at.datetime::date as date,
                                                       at.category,
                                                       coalesce(at.marketing_channel, 'null') as marketing_channel,
                                                       row_number() over (partition by e.id, enrollment_date order by at.datetime desc) as rank
                                                FROM enrollments e
                                                LEFT JOIN account_touches at on at.id = e.id and at.datetime::date <= e.enrollment_datetime::date
                                                WHERE at.id is not null)
                                          WHERE rank = 1) as channel on channel.id = e.id and channel.enrollment_date = e.enrollment_datetime::date)

-- Find all movetofree accounts from ppha minus internal accounts 

    ,retention as (SELECT * FROM
                  (SELECT ppha.id, 
                          ppha.mostrecentenrollmentdate::timestamp_ltz as most_recent_enrollment_datetime,
                          ppha.mostrecentmtfdate::timestamp_ltz as mtf_datetime,
                          coalesce(ppha.tier, 'legacy') as tier,
                          coalesce(lag(ppha.tier) over (partition by ppha.organization_id order by ppha.date::timestamp_ltz), 'null') as previous_tier,
                          round(coalesce(lag(ppha.saas) over (partition by ppha.organization_id order by ppha.date::timestamp_ltz), 0)) as previous_saas,
                          round(coalesce(lag(ppha.featuresum) over (partition by ppha.organization_id order by ppha.date::timestamp_ltz), 0)) as previous_featuresum,
                          previous_saas + previous_featuresum as previous_asp,
                          ppha.movetofree,
                          ppha.lit,
                          ppha.churn,
                          ppha.entry_point
                   FROM table ppha
                   LEFT JOIN table tag on tag.organization_id = ppha.organization_id and tag.name = 'internal'
                   LEFT JOIN table ppe on ppe.payment_plan_id = ppha.organization_payment_plan_id
                   LEFT JOIN table oi on oi.organization_id = ppha.organization_id
                   LEFT JOIN table o on o.id = ppha.organization_id
                   WHERE entry_point not ilike '%xyz%'
                         AND (tag.name <> 'xyz' or tag.name is null) AND (ppe.billing_suspended = false or ppe.billing_suspended is null)
                         AND ((oi.email not ilike '%xyz%' and oi.email not ilike '%xyz%') or oi.email is null)
                         AND ((o.email not ilike '%xyz%' and o.email not ilike '%xyz%') or o.email is null))  
                   WHERE movetofree = 1 and entry_point ilike 'xyz')

-- Left join the enrollments anmd retentions pieces together to match lits & churns to the enrollment

      ,saas_enrolled_and_retention as (SELECT ef.date,
                                              ef.tier,
                                              e.id,
                                              e.enrollment_datetime,
                                              e.asp,
                                              e.enrollment,
                                              r.mtf_datetime,
                                              coalesce(r.movetofree, 0) as movetofree,
                                              coalesce(r.lit, 0) as lit,
                                              coalesce(r.churn, 0) as churn
                                       FROM enrollments_foundation_tier ef
                                       LEFT JOIN enrollments e on e.enrollment_datetime::date = ef.date and e.tier = ef.tier
                                       LEFT JOIN retention r on r.id = e.id and e.enrollment_datetime = r.most_recent_enrollment_datetime)
 
-- Finds aggregate lit counts, asp, and mrr from retention CTE by time period

     ,lit as (SELECT sear.date,
                     sear.tier,
                     coalesce(sum(lit) over (partition by date_trunc('day', sear.date), sear.tier), 0) as daily_lit,
                     coalesce(sum(lit) over (partition by date_trunc('week', sear.date), sear.tier), 0) as weekly_lit,
                     coalesce(sum(lit) over (partition by date_trunc('quarter', sear.date), sear.tier), 0) as quarterly_lit,
                     coalesce(sum(lit) over (partition by date_trunc('month', sear.date), sear.tier), 0) as monthly_lit,
                     coalesce(sum(lit) over (partition by date_trunc('year', sear.date), sear.tier), 0) as yearly_lit,
                     round(coalesce(avg(asp) over (partition by date_trunc('day', sear.date), sear.tier), 0)) as daily_asp,
                     round(coalesce(avg(asp) over (partition by date_trunc('week', sear.date), sear.tier), 0)) as weekly_asp,
                     round(coalesce(avg(asp) over (partition by date_trunc('quarter', sear.date), sear.tier), 0)) as quarterly_asp,
                     round(coalesce(avg(asp) over (partition by date_trunc('month', sear.date), sear.tier), 0)) as monthly_asp,
                     round(coalesce(avg(asp) over (partition by date_trunc('year', sear.date), sear.tier), 0)) as yearly_asp,
                     daily_lit * daily_asp as daily_mrr,
                     weekly_lit * weekly_asp as weekly_mrr,
                     monthly_lit * monthly_asp as monthly_mrr,
                     quarterly_lit * quarterly_asp as quarterly_mrr,
                     yearly_lit * yearly_asp as yearly_mrr,
                     coalesce(sum(lit) over (partition by sear.tier order by sear.date), 0) as total_lit,
                     round(coalesce(avg(asp) over (partition by sear.tier order by sear.date), 0)) as total_asp,
                     total_lit * total_asp as total_mrr
              FROM saas_enrolled_and_retention sear)

-- Finds aggregate churn counts, asp, and mrr from retention CTE by time period

     ,churn as (SELECT sear.date,
                       sear.tier,
                       coalesce(sum(churn) over (partition by date_trunc('day', ef.date), ef.tier), 0) as daily_churn,
                       coalesce(sum(churn) over (partition by date_trunc('week', ef.date), ef.tier), 0) as weekly_churn,
                       coalesce(sum(churn) over (partition by date_trunc('quarter', ef.date), ef.tier), 0) as quarterly_churn,
                       coalesce(sum(churn) over (partition by date_trunc('month', ef.date), ef.tier), 0) as monthly_churn,
                       coalesce(sum(churn) over (partition by date_trunc('year', ef.date), ef.tier), 0) as yearly_churn,
                       round(coalesce(avg(previous_asp) over (partition by date_trunc('day', ef.date), ef.tier), 0)) as daily_asp,
                       round(coalesce(avg(previous_asp) over (partition by date_trunc('week', ef.date), ef.tier), 0)) as weekly_asp,
                       round(coalesce(avg(previous_asp) over (partition by date_trunc('quarter', ef.date), ef.tier), 0)) as quarterly_asp,
                       round(coalesce(avg(previous_asp) over (partition by date_trunc('month', ef.date), ef.tier), 0)) as monthly_asp,
                       round(coalesce(avg(previous_asp) over (partition by date_trunc('year', ef.date), ef.tier), 0)) as yearly_asp,
                       daily_churn * daily_asp as daily_mrr,
                       weekly_churn * weekly_asp as weekly_mrr,
                       monthly_churn * monthly_asp as monthly_mrr,
                       quarterly_churn * quarterly_asp as quarterly_mrr,
                       yearly_churn * yearly_asp as yearly_mrr,
                       coalesce(sum(churn) over (partition by ef.tier order by ef.date), 0) as total_churn,
                       round(coalesce(avg(previous_saas) over (partition by ef.tier order by ef.date), 0)) as total_asp,
                       total_churn * total_asp as total_mrr
                FROM saas_enrolled_and_retention sear)
                
-- Finds aggregate gross saas enrollments & asp & mrr, net saas enrollments & asp & mrr, and ending accounts & asp & mrr

     ,saas_enrolled_gross as (SELECT gross.*,
                               gross.daily_gross_enrollments - lit.daily_lit as daily_net_enrollments,
                               gross.weekly_gross_enrollments - lit.weekly_lit as weekly_net_enrollments,
                               gross.monthly_gross_enrollments - lit.monthly_lit as monthly_net_enrollments,
                               gross.quarterly_gross_enrollments - lit.quarterly_lit as quarterly_net_enrollments,
                               gross.yearly_gross_enrollments - lit.yearly_lit as yearly_net_enrollments,
                               (gross.daily_gross_mrr - lit.daily_mrr) as daily_net_mrr,
                               (gross.weekly_gross_mrr - lit.weekly_mrr) as weekly_net_mrr,
                               (gross.monthly_gross_mrr - lit.monthly_mrr) as monthly_net_mrr,
                               (gross.quarterly_gross_mrr - lit.quarterly_mrr) as quarterly_net_mrr,
                               (gross.yearly_gross_mrr - lit.yearly_mrr) as yearly_net_mrr,
                               round(case when daily_net_enrollments > 0 then daily_net_mrr / daily_net_enrollments else 0 end) as daily_net_asp,
                               round(case when weekly_net_enrollments > 0 then weekly_net_mrr / weekly_net_enrollments else 0 end) as weekly_net_asp,
                               round(case when monthly_net_enrollments > 0 then monthly_net_mrr / monthly_net_enrollments else 0 end) as monthly_net_asp,
                               round(case when quarterly_net_enrollments > 0 then quarterly_net_mrr / quarterly_net_enrollments else 0 end) as quarterly_net_asp,
                               round(case when yearly_net_enrollments > 0 then yearly_net_mrr / yearly_net_enrollments else 0 end) as yearly_net_asp,
                               gross.total_gross_accounts - (lit.total_lit + churn.total_churn) as ending_accounts,
                               gross.total_gross_mrr - (lit.total_mrr + churn.total_mrr) as ending_accounts_mrr,
                               round(case when ending_accounts > 0 then ending_accounts_mrr / ending_accounts else 0 end) as ending_accounts_asp
                        FROM (SELECT distinct ef.date,
                                              ef.tier,
                                              coalesce(sum(enrollment) over (partition by date_trunc('day', ef.date), ef.tier), 0) as daily_gross_enrollments,
                                              coalesce(sum(enrollment) over (partition by date_trunc('week', ef.date), ef.tier), 0) as weekly_gross_enrollments,
                                              coalesce(sum(enrollment) over (partition by date_trunc('quarter', ef.date), ef.tier), 0) as quarterly_gross_enrollments,
                                              coalesce(sum(enrollment) over (partition by date_trunc('month', ef.date), ef.tier), 0) as monthly_gross_enrollments,
                                              coalesce(sum(enrollment) over (partition by date_trunc('year', ef.date), ef.tier), 0) as yearly_gross_enrollments,
                                              coalesce(sum(enrollment) over (partition by ef.tier order by date), 0) as total_gross_accounts,
                                              round(coalesce(avg(asp) over (partition by date_trunc('day', ef.date), ef.tier), 0)) as daily_gross_asp,
                                              round(coalesce(avg(asp) over (partition by date_trunc('week', ef.date), ef.tier), 0)) as weekly_gross_asp,
                                              round(coalesce(avg(asp) over (partition by date_trunc('quarter', ef.date), ef.tier), 0)) as quarterly_gross_asp,
                                              round(coalesce(avg(asp) over (partition by date_trunc('month', ef.date), ef.tier), 0)) as monthly_gross_asp,
                                              round(coalesce(avg(asp) over (partition by date_trunc('year', ef.date), ef.tier), 0)) as yearly_gross_asp,
                                              round(coalesce(avg(asp) over (partition by ef.tier order by date), 0)) as total_gross_asp,
                                              daily_gross_enrollments * daily_gross_asp as daily_gross_mrr,
                                              weekly_gross_enrollments * weekly_gross_asp as weekly_gross_mrr,
                                              monthly_gross_enrollments * monthly_gross_asp as monthly_gross_mrr,
                                              quarterly_gross_enrollments * quarterly_gross_asp as quarterly_gross_mrr,
                                              yearly_gross_enrollments * yearly_gross_asp as yearly_gross_mrr,
                                              total_gross_accounts * total_gross_asp as total_gross_mrr
                              FROM enrollments_foundation_tier ef
                              LEFT JOIN enrollments e on e.enrollment_datetime::date = ef.date and e.tier = ef.tier) as gross
                        LEFT JOIN (SELECT distinct * from lit) as lit on lit.date = gross.date and lit.tier = gross.tier
                        LEFT JOIN (SELECT distinct * from churn) as churn on churn.date = gross.date and churn.tier = gross.tier)

-- Gross saas enrollments by time period split by marketing channel group

      ,saas_enrolled_by_mc as (SELECT distinct ef.date,
                                               ef.category,
                                               coalesce(sum(enrollment) over (partition by date_trunc('day', ef.date), ef.category), 0) as daily_gross_enrollments,
                                               coalesce(sum(enrollment) over (partition by date_trunc('week', ef.date), ef.category), 0) as weekly_gross_enrollments,
                                               coalesce(sum(enrollment) over (partition by date_trunc('quarter', ef.date), ef.category), 0) as quarterly_gross_enrollments,
                                               coalesce(sum(enrollment) over (partition by date_trunc('month', ef.date), ef.category), 0) as monthly_gross_enrollments,
                                               coalesce(sum(enrollment) over (partition by date_trunc('year', ef.date), ef.category), 0) as yearly_gross_enrollments,
                                               coalesce(sum(enrollment) over (partition by ef.category order by ef.date), 0) as total_gross_accounts
                               FROM enrollments_foundation_mc ef
                               LEFT JOIN last_enrollment_touch let on let.enrollment_date = ef.date and let.category = ef.category)

-- Cost per lead (spend / leads) and ffc (enrollments / leads)

      ,lead_cpl_and_ffc as (SELECT l.date,
                                   l.category,
                                   round(case when l.daily_leads > 0 then ls.daily_spend / l.daily_leads else 0 end) as daily_cpl,
                                   round(case when l.weekly_leads > 0 then ls.weekly_spend / l.weekly_leads else 0 end) as weekly_cpl,
                                   round(case when l.monthly_leads > 0 then ls.monthly_spend / l.monthly_leads else 0 end) as monthly_cpl,
                                   round(case when l.quarterly_leads > 0 then ls.quarterly_spend / l.quarterly_leads else 0 end) as quarterly_cpl,
                                   round(case when l.yearly_leads > 0 then ls.yearly_spend / l.yearly_leads else 0 end) as yearly_cpl,
                                   to_decimal(case when l.daily_leads > 0 then (sebm.daily_gross_enrollments / l.daily_leads) * 100 else 0 end, 9, 1) as daily_ffc,
                                   to_decimal(case when l.weekly_leads > 0 then (sebm.weekly_gross_enrollments / l.weekly_leads) * 100 else 0 end, 9, 1) as weekly_ffc,
                                   to_decimal(case when l.monthly_leads > 0 then (sebm.monthly_gross_enrollments / l.monthly_leads) * 100 else 0 end, 9, 1) as monthly_ffc,
                                   to_decimal(case when l.quarterly_leads > 0 then (sebm.quarterly_gross_enrollments / l.quarterly_leads) * 100 else 0 end, 9, 1) as quarterly_ffc,
                                   to_decimal(case when l.yearly_leads > 0 then (sebm.yearly_gross_enrollments / l.yearly_leads) * 100 else 0 end, 9, 1) as yearly_ffc
                            FROM leads l
                            LEFT JOIN lead_spend ls on ls.date = l.date and ls.category = l.category
                            LEFT JOIN saas_enrolled_by_mc sebm on sebm.date = l.date and sebm.category = l.category)

-- Finds beginning_accounts for each month to be used in churn percentage calculation

      ,beginning_accounts as (SELECT se.*, 
                                     ea.ending_accounts as beginning_accounts
                              FROM (SELECT distinct date,
                                                    tier,
                                                    ending_accounts,
                                                    dateadd(day, -1, min(date) over (partition by month(date), year(date) order by month(date))) as max_date_prev_month
                                    FROM saas_enrolled) as se
                              LEFT JOIN (SELECT distinct date, tier, ending_accounts FROM saas_enrolled) as ea on ea.date = se.max_date_prev_month and ea.tier = se.tier)

-- Churn in month / beginnning accounts

      ,churn_percentage as (SELECT c.date,
                                   c.tier,
                                   round(case when ba.beginning_accounts = 0 then 0 else (c.monthly_churn / ba.beginning_accounts) * 100 end) as monthly_churn_percentage
                            FROM churn c
                            LEFT JOIN beginning_accounts ba on ba.date = c.date and ba.tier = c.tier)

-- LIT in month / gross enrollments in month 

      ,lit_percentage as (SELECT l.date,
                                 l.tier,
                                 case when se.monthly_gross_enrollments = 0 then 0
                                      else round((l.monthly_lit / se.monthly_gross_enrollments) * 100) end as monthly_lit_percentage
                          FROM lit l
                          LEFT JOIN saas_enrolled se on se.date = l.date and se.tier = l.tier)                            


      ,fin_tech_foundation as (SELECT pbo.paymentdate as date, 
                                      type.type 
                               FROM ((SELECT 'xyz' as type UNION SELECT 'xyz' as type UNION SELECT 'xyz' as type) type
                                          CROSS JOIN 
                                     (SELECT distinct paymentdate FROM table) pbo)
                               WHERE date <= dateadd('day', -1, current_date()))
                         
-- CC attach, gmv, and gmv / org

      ,cc_attach as (SELECT paymentdate as attach_date,
                            coalesce(count(distinct organization_id) over (partition by date_trunc('day', attach_date)), 0) as daily_attach,
                            coalesce(count(distinct organization_id) over (partition by date_trunc('week', attach_date)), 0) as weekly_attach,
                            coalesce(count(distinct organization_id) over (partition by date_trunc('quarter', attach_date)), 0) as quarterly_attach,
                            coalesce(count(distinct organization_id) over (partition by date_trunc('month', attach_date)), 0) as monthly_attach,
                            coalesce(count(distinct organization_id) over (partition by date_trunc('year', attach_date)), 0) as yearly_attach,
                            coalesce(count(distinct organization_id) over (order by attach_date), 0) as total_attach,
                            round(coalesce(sum(cc_amount) over (partition by date_trunc('day', attach_date) order by attach_date), 0)) as daily_gmv,
                            round(coalesce(sum(cc_amount) over (partition by date_trunc('week', attach_date) order by attach_date), 0)) as weekly_gmv,
                            round(coalesce(sum(cc_amount) over (partition by date_trunc('quarter', attach_date) order by attach_date), 0)) as quarterly_gmv,
                            round(coalesce(sum(cc_amount) over (partition by date_trunc('month', attach_date) order by attach_date), 0)) as monthly_gmv,
                            round(coalesce(sum(cc_amount) over (partition by date_trunc('year', attach_date) order by attach_date), 0)) as yearly_gmv,
                            round(coalesce(sum(cc_amount) over (order by attach_date), 0)) as total_gmv,
                            round(case when daily_attach > 0 then daily_gmv / daily_attach else 0 end) as daily_gmv_per_org,
                            round(case when weekly_attach > 0 then weekly_gmv / weekly_attach else 0 end) as weekly_gmv_per_org,
                            round(case when monthly_attach > 0 then monthly_gmv / monthly_attach else 0 end) as monthly_gmv_per_org,
                            round(case when quarterly_attach > 0 then quarterly_gmv / quarterly_attach else 0 end) as quarterly_gmv_per_org,
                            round(case when yearly_attach > 0 then yearly_gmv / yearly_attach else 0 end) as yearly_gmv_per_org,
                            round(case when total_attach > 0 then total_gmv / total_attach else 0 end) as total_gmv_per_org
                      FROM (SELECT paymentdate,
                                   organization_id,
                                   cc_amount
                            FROM table)
                            WHERE cc_amount > 1)

      -- Instapay attach, gmv, and gmv / org 

      ,a_attach as (SELECT paymentdate as attach_date,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('day', attach_date)), 0) as daily_attach,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('week', attach_date)), 0) as weekly_attach,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('quarter', attach_date)), 0) as quarterly_attach,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('month', attach_date)), 0) as monthly_attach,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('year', attach_date)), 0) as yearly_attach,
                           coalesce(count(distinct organization_id) over (order by attach_date), 0) as total_attach,
                           round(coalesce(sum(instapay_amount) over (partition by date_trunc('day', attach_date) order by attach_date), 0)) as daily_gmv,
                           round(coalesce(sum(instapay_amount) over (partition by date_trunc('week', attach_date) order by attach_date), 0)) as weekly_gmv,
                           round(coalesce(sum(instapay_amount) over (partition by date_trunc('quarter', attach_date) order by attach_date), 0)) as quarterly_gmv,
                           round(coalesce(sum(instapay_amount) over (partition by date_trunc('month', attach_date) order by attach_date), 0)) as monthly_gmv,
                           round(coalesce(sum(instapay_amount) over (partition by date_trunc('year', attach_date) order by attach_date), 0)) as yearly_gmv,
                           round(coalesce(sum(instapay_amount) over (order by attach_date), 0)) as total_gmv,
                           round(case when daily_attach > 0 then daily_gmv / daily_attach else 0 end) as daily_gmv_per_org,
                           round(case when weekly_attach > 0 then weekly_gmv / weekly_attach else 0 end) as weekly_gmv_per_org,
                           round(case when monthly_attach > 0 then monthly_gmv / monthly_attach else 0 end) as monthly_gmv_per_org,
                           round(case when quarterly_attach > 0 then quarterly_gmv / quarterly_attach else 0 end) as quarterly_gmv_per_org,
                           round(case when yearly_attach > 0 then yearly_gmv / yearly_attach else 0 end) as yearly_gmv_per_org,
                           round(case when total_attach > 0 then total_gmv / total_attach else 0 end) as total_gmv_per_org
                    FROM (SELECT paymentdate,
                                 organization_id,
                                 instapay_amount
                          FROM table)
                          WHERE instapay_amount > 1)

      -- ACH attach, gmv, and gmv / org

      ,b_attach as (SELECT paymentdate as attach_date,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('day', attach_date)), 0) as daily_attach,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('week', attach_date)), 0) as weekly_attach,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('quarter', attach_date)), 0) as quarterly_attach,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('month', attach_date)), 0) as monthly_attach,
                           coalesce(count(distinct organization_id) over (partition by date_trunc('year', attach_date)), 0) as yearly_attach,
                           coalesce(count(distinct organization_id) over (order by attach_date), 0) as total_attach,
                           round(coalesce(sum(ach_amount) over (partition by date_trunc('day', attach_date) order by attach_date), 0)) as daily_gmv,
                           round(coalesce(sum(ach_amount) over (partition by date_trunc('week', attach_date) order by attach_date), 0)) as weekly_gmv,
                           round(coalesce(sum(ach_amount) over (partition by date_trunc('quarter', attach_date) order by attach_date), 0)) as quarterly_gmv,
                           round(coalesce(sum(ach_amount) over (partition by date_trunc('month', attach_date) order by attach_date), 0)) as monthly_gmv,
                           round(coalesce(sum(ach_amount) over (partition by date_trunc('year', attach_date) order by attach_date), 0)) as yearly_gmv,
                           round(coalesce(sum(ach_amount) over (order by attach_date), 0)) as total_gmv,
                           round(case when daily_attach > 0 then daily_gmv / daily_attach else 0 end) as daily_gmv_per_org,
                           round(case when weekly_attach > 0 then weekly_gmv / weekly_attach else 0 end) as weekly_gmv_per_org,
                           round(case when monthly_attach > 0 then monthly_gmv / monthly_attach else 0 end) as monthly_gmv_per_org,
                           round(case when quarterly_attach > 0 then quarterly_gmv / quarterly_attach else 0 end) as quarterly_gmv_per_org,
                           round(case when yearly_attach > 0 then yearly_gmv / yearly_attach else 0 end) as yearly_gmv_per_org,
                           round(case when total_attach > 0 then total_gmv / total_attach else 0 end) as total_gmv_per_org
                      FROM (SELECT paymentdate,
                                   organization_id,
                                   ach_amount
                            FROM pbo)
                            WHERE ach_amount > 1)
                
-----Begin union of all the pieces-----

-- Leads 

SELECT distinct date,
                'lead_count' as type,
                category,
                daily_leads as daily,
                weekly_leads as weekly,
                monthly_leads as monthly,
                quarterly_leads as quarterly,
                yearly_leads as annually
FROM leads
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- Lead Spend

SELECT distinct date,
                'lead_spend' as type,
                category,
                daily_spend as daily,
                weekly_spend as weekly,
                monthly_spend as monthly,
                quarterly_spend as quarterly,
                yearly_spend as annually
FROM lead_spend
WHERE date = dateadd('day', -1, current_date())
                     
UNION ALL

-- Cost per Lead

SELECT distinct date,
                'lead_cpl' as type,
                category,
                daily_cpl as daily,
                weekly_cpl as weekly,
                monthly_cpl as monthly,
                quarterly_cpl as quarterly,
                yearly_cpl as annually
FROM lead_cpl_and_ffc
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- Full funnel conversion

SELECT distinct date,
                'ffc' as type,
                category,
                daily_ffc as daily,
                weekly_ffc as weekly,
                monthly_ffc as monthly,
                quarterly_ffc as quarterly,
                yearly_ffc as annually
FROM lead_cpl_and_ffc
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- Gross SaaS Enrollments
                     
SELECT distinct date,
                'gross_saas_enrolled' as type,
                tier as category,
                daily_gross_enrollments as daily,
                weekly_gross_enrollments as weekly,
                monthly_gross_enrollments as monthly,
                quarterly_gross_enrollments as quarterly,
                yearly_gross_enrollments as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- Gross SaaS ASP

SELECT distinct date,
                'gross_saas_asp' as type,
                tier as category,
                daily_gross_asp as daily,
                weekly_gross_asp as weekly,
                monthly_gross_asp as monthly,
                quarterly_gross_asp as quarterly,
                yearly_gross_asp as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- Gross SaaS MRR

SELECT distinct date,
                'gross_saas_mrr' as type,
                tier as category,
                daily_gross_mrr as daily,
                weekly_gross_mrr as weekly,
                monthly_gross_mrr as monthly,
                quarterly_gross_mrr as quarterly,
                yearly_gross_mrr as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- Net SaaS Enrollments

SELECT distinct date,
                'net_saas_enrolled' as type,
                tier as category,
                daily_net_enrollments as daily,
                weekly_net_enrollments as weekly,
                monthly_net_enrollments as monthly,
                quarterly_net_enrollments as quarterly,
                yearly_net_enrollments as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- Net SaaS ASP

SELECT distinct date,
                'net_saas_asp' as type,
                tier as category,
                daily_net_asp as daily,
                weekly_net_asp as weekly,
                monthly_net_asp as monthly,
                quarterly_net_asp as quarterly,
                yearly_net_asp as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- Net SaaS MRR

SELECT distinct date,
                'net_saas_mrr' as type,
                tier as category,
                daily_net_mrr as daily,
                weekly_net_mrr as weekly,
                monthly_net_mrr as monthly,
                quarterly_net_mrr as quarterly,
                yearly_net_mrr as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS Ending Accounts

SELECT distinct date,
                'saas_ending_accounts' as type,
                tier as category,
                ending_accounts as daily,
                ending_accounts as weekly,
                ending_accounts as monthly,
                ending_accounts as quarterly,
                ending_accounts as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS Ending Accounts ASP

SELECT distinct date,
                'saas_ending_accounts_asp' as type,
                tier as category,
                ending_accounts_asp as daily,
                ending_accounts_asp as weekly,
                ending_accounts_asp as monthly,
                ending_accounts_asp as quarterly,
                ending_accounts_asp as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS Ending Accounts MRR

SELECT distinct date,
                'saas_ending_accounts_mrr' as type,
                tier as category,
                ending_accounts_mrr as daily,
                ending_accounts_mrr as weekly,
                ending_accounts_mrr as monthly,
                ending_accounts_mrr as quarterly,
                ending_accounts_mrr as annually
FROM saas_enrolled
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS LIT Count

SELECT distinct date,
                'saas_lit_count' as type,
                tier as category,
                daily_lit as daily,
                weekly_lit as weekly,
                monthly_lit as monthly,
                quarterly_lit as quarterly,
                yearly_lit as annually
FROM lit
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS LIT ASP

SELECT distinct date,
                'saas_lit_asp' as type,
                tier as category,
                daily_asp as daily,
                weekly_asp as weekly,
                monthly_asp as monthly,
                quarterly_asp as quarterly,
                yearly_asp as annually
FROM lit
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS LIT MRR

SELECT distinct date,
                'saas_lit_mrr' as type,
                tier as category,
                daily_mrr as daily,
                weekly_mrr as weekly,
                monthly_mrr as monthly,
                quarterly_mrr as quarterly,
                yearly_mrr as annually
FROM lit
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS Churn Count

SELECT distinct date,
                'saas_churn_count' as type,
                tier as category,
                daily_churn as daily,
                weekly_churn as weekly,
                monthly_churn as monthly,
                quarterly_churn as quarterly,
                yearly_churn as annually
FROM churn
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS Churn ASP

SELECT distinct date,
                'saas_churn_asp' as type,
                tier as category,
                daily_asp as daily,
                weekly_asp as weekly,
                monthly_asp as monthly,
                quarterly_asp as quarterly,
                yearly_asp as annually
FROM churn
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS Churn MRR

SELECT distinct date,
                'saas_churn_mrr' as type,
                tier as category,
                daily_mrr as daily,
                weekly_mrr as weekly,
                monthly_mrr as monthly,
                quarterly_mrr as quarterly,
                yearly_mrr as annually
FROM churn
WHERE date = dateadd('day', -1, current_date())

UNION ALL

--SaaS LIT Percentage

SELECT distinct date,
                'saas_lit_percentage' as type,
                tier as category,
                monthly_lit_percentage as daily,
                monthly_lit_percentage as weekly,
                monthly_lit_percentage as monthly,
                monthly_lit_percentage as quarterly,
                monthly_lit_percentage as annually
FROM lit_percentage
WHERE date = dateadd('day', -1, current_date())

UNION ALL

-- SaaS Churn Percentage

SELECT distinct date,
                'saas_churn_percentage' as type,
                tier as category,
                monthly_churn_percentage as daily,
                monthly_churn_percentage as weekly,
                monthly_churn_percentage as monthly,
                monthly_churn_percentage as quarterly,
                monthly_churn_percentage as annually
FROM churn_percentage
WHERE date = dateadd('day', -1, current_date())

UNION ALL

--  CC attach

SELECT distinct attach_date as date,
               'fin_tech_attached' as tier,
               'Credit Card' as category,
               daily_attach as daily,
               weekly_attach as weekly,
               monthly_attach as monthly,
               quarterly_attach as quarterly,
               yearly_attach as annually
FROM a_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

UNION ALL

-- Instapay attach

SELECT distinct attach_date as date,
               'fin_tech_attached' as tier,
               'Instapay' as category,
               daily_attach as daily,
               weekly_attach as weekly,
               monthly_attach as monthly,
               quarterly_attach as quarterly,
               yearly_attach as annually
FROM b_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

UNION ALL

-- ACH attach

SELECT distinct attach_date as date,
               'fin_tech_attached' as tier,
               'ACH' as category,
               daily_attach as daily,
               weekly_attach as weekly,
               monthly_attach as monthly,
               quarterly_attach as quarterly,
               yearly_attach as annually
FROM c_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

UNION ALL

-- CC GMV

SELECT distinct attach_date as date,
               'fin_tech_gmv' as tier,
               'Credit Card' as category,
               daily_gmv as daily,
               weekly_gmv as weekly,
               monthly_gmv as monthly,
               quarterly_gmv as quarterly,
               yearly_gmv as annually
FROM a_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

UNION ALL

--Instapay GMV

SELECT distinct attach_date as date,
               'fin_tech_gmv' as tier,
               'Instapay' as category,
               daily_gmv as daily,
               weekly_gmv as weekly,
               monthly_gmv as monthly,
               quarterly_gmv as quarterly,
               yearly_gmv as annually
FROM b_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

UNION ALL

-- ACH GMV

SELECT distinct attach_date as date,
               'fin_tech_gmv' as tier,
               'ACH' as category,
               daily_gmv as daily,
               weekly_gmv as weekly,
               monthly_gmv as monthly,
               quarterly_gmv as quarterly,
               yearly_gmv as annually
FROM c_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

UNION ALL

-- CC GMV / ORG

SELECT distinct attach_date as date,
               'fin_tech_gmv_per_org' as tier,
               'Credit Card' as category,
               daily_gmv_per_org as daily,
               weekly_gmv_per_org as weekly,
               monthly_gmv_per_org as monthly,
               quarterly_gmv_per_org as quarterly,
               yearly_gmv_per_org as annually
FROM a_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

UNION ALL

-- Instapay GMV / ORG

SELECT distinct attach_date as date,
               'fin_tech_gmv_per_org' as tier,
               'Instapay' as category,
               daily_gmv_per_org as daily,
               weekly_gmv_per_org as weekly,
               monthly_gmv_per_org as monthly,
               quarterly_gmv_per_org as quarterly,
               yearly_gmv_per_org as annually
FROM b_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

UNION ALL

-- ACH GMV / ORG

SELECT distinct attach_date as date,
               'fin_tech_gmv_per_org' as tier,
               'ACH' as category,
               daily_gmv_per_org as daily,
               weekly_gmv_per_org as weekly,
               monthly_gmv_per_org as monthly,
               quarterly_gmv_per_org as quarterly,
               yearly_gmv_per_org as annually
FROM c_attach 
WHERE attach_date = dateadd('day', -1, current_date()) 

ORDER BY 2, 3
