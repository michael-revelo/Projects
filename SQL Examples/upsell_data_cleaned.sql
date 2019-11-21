with employee_count as (SELECT id, 
                               SUM(new) - SUM(old) as employee_count
                        FROM (SELECT organization_id, 
                                     ifnull(CASE WHEN kind = 'new' THEN COUNT(DISTINCT service_pro_id) END, 0) as new,
                                     ifnull(CASE WHEN kind = 'old' THEN COUNT(DISTINCT service_pro_id) END, 0) as old
                        FROM table
                        GROUP BY id, kind)
                        GROUP BY id)

     ,payment_plan_histories as (SELECT id,
                                        date,
                                        previous_billingplanid,
                                        previous_plan.display_name as previous_plan_name,
                                        billing_plan_id,
                                        current_plan.display_name as current_plan_name,
                                        additional_features,
                                        featurecount,
                                        planchange,
                                        movetofree,
                                        mostrecentenrollmentdate,
                                        mostrecentmtfdate,
                                        case when contains(additional_features, '"sms_number","price":0') then 'free'
                                             when contains(additional_features, 'sms_number') then '1'
                                             else '0' end as sms_number_flag,
                                        case when contains(additional_features, '"marketing","price":0') then 'free'
                                             when contains(additional_features, 'marketing') then '1'
                                             else '0' end as marketing_flag,
                                        case when contains(additional_features, '"time_tracking","price":0') then 'free'
                                             when contains(additional_features, 'time_tracking') then '1'
                                             else '0' end as time_tracking_flag,
                                        case when contains(additional_features, '"service_agreements","price":0') then 'free'
                                             when contains(additional_features, 'service_agreements') then '1'
                                             else '0' end as service_agreements_flag,
                                        case when contains(additional_features, '"employee_default_report","price":0') then 'free'
                                             when contains(additional_features, 'employee_default_report') then '1'
                                             else '0' end as employee_default_flag,
                                        case when contains(additional_features, '"additional_employee","price":0') then 'free'
                                             when contains(additional_features, 'additional_employee') then '1'
                                             else '0' end as additional_employee_flag,
                                        case when contains(additional_features, '"hide_time_tracking_card","price":0') then 'free'
                                             when contains(additional_features, 'hide_time_tracking_card') then '1'
                                             else '0' end as hide_time_tracking_card_flag,
                                        case when contains(additional_features, '"old_mobile_billing","price":0') then 'free'
                                             when contains(additional_features, 'old_mobile_billing') then '1'
                                             else '0' end as old_mobile_billing_flag,
                                        case when contains(additional_features, '"tags_report,"price":0') then 'free'
                                             when contains(additional_features, 'tags_report') then '1'
                                             else '0' end as tags_report_flag,
                                        case when contains(additional_features, '"unlimited_employees","price":0') then 'free'
                                             when contains(additional_features, 'unlimited_employees') then '1'
                                             else '0' end as unlimited_employees_flag,
                                        case when contains(additional_features, '"add_employee","price":0') then 'free'
                                             when contains(additional_features, 'add_employee') then '1'
                                             else '0' end as add_employee_flag,
                                        case when contains(additional_features, '"stripe_terminal","price":0') then 'free'
                                             when contains(additional_features, 'stripe_terminal') then '1'
                                             else '0' end as stripe_terminal_flag,
                                        case when contains(additional_features, '"employee_custom_report","price":0') then 'free'
                                             when contains(additional_features, 'employee_custom_report') then '1'            
                                             else '0' end as employee_custom_report_flag,
                                        case when contains(additional_features, '"quickbooks","price":0') then 'free'
                                             when contains(additional_features, 'quickbooks') then '1'           
                                             else '0' end as quickbooks_flag,
                                        case when contains(additional_features, '"employee_time_tracking","price":0') then 'free'
                                             when contains(additional_features, 'employee_time_tracking') then '1'
                                             else '0' end as employee_time_tracking_flag,
                                        case when contains(additional_features, '"zapier","price":0') then 'free'
                                             when contains(additional_features, 'zapier') then '1'
                                             else '0' end as zapier_flag,
                                        case when contains(additional_features, '"employee_report","price":0') then 'free'
                                             when contains(additional_features, 'employee_report') then '1'
                                             else '0' end as employee_report_flag,
                                        case when contains(additional_features, '"inter_company_chat","price":0') then 'free'
                                             when contains(additional_features, 'inter_company_chat') then '1'
                                             else '0' end as inter_company_chat_flag,
                                        case when contains(additional_features, '"employee_live_map","price":0') then 'free'
                                             when contains(additional_features, 'employee_live_map') then '1'
                                             else '0' end as employee_live_map_flag,
                                        case when contains(additional_features, '"booking_widget","price":0') then 'free'
                                             when contains(additional_features, 'booking_widget') then '1'
                                             else '0' end as booking_widget_flag,
                                        case when contains(additional_features, '"mobile_intercom_support","price":0') then 'free'
                                             when contains(additional_features, 'mobile_intercom_support') then '1'
                                             else '0' end as mobile_intercom_support_flag,
                                        case when contains(additional_features, '"intercom","price":0') then 'free'
                                             when contains(additional_features, 'intercom') and not contains(additional_features, 'mobile_intercom_support') then '1'
                                             else '0' end as intercom_flag
                                 FROM table
                                       LEFT JOIN previous_plan on previous_plan.id = previous_billingplanid
                                       LEFT JOIN current_plan on current_plan.id = billing_plan_id)
                                 
      ,upsell_opps as (SELECT opp.org,
                              opp.vertical,
                              opp.closedate
                       FROM opportunity
                             LEFT JOIN rt on rt.id = opp.recordtypeid
                       WHERE rt.name = 'xyz' and opp.isdeleted = 'False')
                       
      ,projects as (SELECT status,
                           org
                    FROM project
                         LEFT JOIN record_type on record_type.id = project.recordtypeid
                    WHERE record_type.name = 'xyz' and project.isdeleted = 'False' and status = 'Completed')
                       
      ,org_info as (SELECT id,
                           vertical,
                           tier,
                           type
                    FROM table)
                    
      ,billing_plans as (SELECT id,
                                display_name,
                                name,
                                tier,
                                plan_descriptions
                         FROM table)

SELECT array_construct_compact(case when marketing_flag <> 'free' and marketing_flag <> lag(marketing_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'marketing' else null end,
                               case when booking_widget_flag <> 'free' and booking_widget_flag <> lag(booking_widget_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'booking_widget' else null end,
                               case when time_tracking_flag <> 'free' and time_tracking_flag <> lag(time_tracking_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'time_tracking' else null end,
                               case when service_agreements_flag <> 'free' and service_agreements_flag <> lag(service_agreements_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'service_agreements' else null end,
                               case when employee_default_flag <> 'free' and employee_default_flag <> lag(employee_default_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'employee_default' else null end,
                               case when additional_employee_flag <> 'free' and additional_employee_flag <> lag(additional_employee_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'additonal_employee' else null end,
                               case when hide_time_tracking_card_flag <> 'free' and hide_time_tracking_card_flag <> lag(hide_time_tracking_card_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'hide_time_tracking_card' else null end,
                               case when old_mobile_billing_flag <> 'free' and old_mobile_billing_flag <> lag(old_mobile_billing_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'old_mobile_billing' else null end,
                               case when tags_report_flag <> 'free' and tags_report_flag <> lag(tags_report_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'tags_report' else null end,
                               case when unlimited_employees_flag <> 'free' and unlimited_employees_flag <> lag(unlimited_employees_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'unlimited_employees' else null end,
                               case when add_employee_flag <> 'free' and add_employee_flag <> lag(add_employee_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'add_employee' else null end,
                               case when stripe_terminal_flag <> 'free' and stripe_terminal_flag <> lag(stripe_terminal_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'stripe_terminal' else null end,
                               case when employee_custom_report_flag <> 'free' and employee_custom_report_flag <> lag(employee_custom_report_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'employee_custom_report' else null end,
                               case when quickbooks_flag <> 'free' and quickbooks_flag <> lag(quickbooks_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'quickbooks' else null end,
                               case when employee_time_tracking_flag <> 'free' and employee_time_tracking_flag <> lag(employee_time_tracking_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'employee_time_tracking' else null end,
                               case when zapier_flag <> 'free' and zapier_flag <> lag(zapier_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'zapier' else null end,
                               case when employee_report_flag <> 'free' and employee_report_flag <> lag(employee_report_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'employee_report' else null end,
                               case when inter_company_chat_flag <> 'free' and inter_company_chat_flag <> lag(inter_company_chat_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'inter_company_chat' else null end,
                               case when employee_live_map_flag <> 'free' and employee_live_map_flag <> lag(employee_live_map_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'employee_live_map' else null end,
                               case when mobile_intercom_support_flag <> 'free' and mobile_intercom_support_flag <> lag(mobile_intercom_support_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'mobile_intercom_support' else null end,
                               case when intercom_flag <> 'free' and intercom_flag <> lag(intercom_flag) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) then 'intercom' else null end)
                               as specific_feature_change,
       featurecount - lag(featurecount, 1, 0) over (partition by payment_plan_histories.organization_id order by payment_plan_histories.date) as feature_count_delta,
       array_size(specific_feature_change) as paid_feature_count_delta,
       abs(feature_count_delta) - paid_feature_count_delta as free_feature_count_delta,
       case when feature_count_delta < 0 and paid_feature_count_delta > 0 and current_plan_name <> 'Expired' then 'downsell'
            when feature_count_delta < 0 and (current_plan_name = 'Expired' or current_plan_name is null) then 'loss'
            when feature_count_delta > 0 and paid_feature_count_delta > 0 then 'upsell'
            when feature_count_delta > 0 and paid_feature_count_delta = 0 and planchange > 0 then 'plan change'
            else 'same' end as feature_change,
       case when planchange > 0 and previous_billingplanid < billing_plan_id then 'plan upgrade'
            when planchange > 0 and previous_billingplanid > billing_plan_id then 'plan downgrade'
            else 'same' end as plan_change,
       payment_plan_histories.*,
       org_info.*,
       employee_count.employee_count,
       upsell_opps.*,
       projects.upsell_status__c,
       case when (payment_plan_histories.additional_features <> '[]' or payment_plan_histories.planchange > 0) and (upsell_opps.closedate is not null or projects.upsell_status__c is not null) then 'sales_attached'
            when (payment_plan_histories.additional_features <> '[]' or payment_plan_histories.planchange > 0) and (upsell_opps.closedate is null or projects.upsell_status__c is null) then 'self_attached'
            else 'not_attached' end as attach_type
FROM payment_plan_histories
     LEFT JOIN org_info on org_info.org_id = payment_plan_histories.organization_id
     LEFT JOIN employee_count on employee_count.organization_id = payment_plan_histories.organization_id
     LEFT JOIN upsell_opps on upsell_opps.org_id__c = payment_plan_histories.organization_id
     LEFT JOIN projects on projects.org_id__c = payment_plan_histories.organization_id
ORDER BY payment_plan_histories.organization_id, payment_plan_histories.date;