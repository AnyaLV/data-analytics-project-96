
with att as (
    with visit_paid as (
        select
            *,
            case
                when
                    medium in (
                        'cpc',
                        'cpm',
                        'cpa',
                        'youtube',
                        'cpp',
                        'tg',
                        'social'
                    )
                    then 1
                else 0
            end as is_paid
        from sessions
    ),

    visitors_with_leads as (
        select
            s.visitor_id,
            s.visit_date,
            s.source as utm_source,
            s.medium as utm_medium,
            s.campaign as utm_campaign,
            l.created_at,
            l.amount,
            l.closing_reason,
            l.status_id,
            row_number() over (
                partition by s.visitor_id
                order by s.is_paid desc, s.visit_date desc
            ) as rn
        from visit_paid as s
        left join leads as l
            on
                l.visitor_id = s.visitor_id
                and l.created_at >= s.visit_date
    )

    select *
    from visitors_with_leads
    where rn = 1
)

select
    utm_source,
    utm_medium,
    percentile_disc(0.90) within group (
        order by date_part('day', created_at - visit_date)
    ) as days_to_lead
from att
group by 1, 2



with visitors_pay as (
	select ses.visitor_id
		, ses.visit_date
		, ses.source as utm_source
		, ses.medium as utm_medium
		, ses.campaign as utm_campaign
		, l.lead_id 
		, l.amount
		, l.created_at 
		, l.closing_reason 
		, l.status_id
		, row_number() over (partition by ses.visitor_id order by ses.visit_date desc) as RN
	from sessions as ses
		left join leads as l on ses.visitor_id=l.visitor_id
								and ses.visit_date<=l.created_at
	--where ses. medium != 'organic'
),
total_visitors as (
	select  utm_source
        , utm_medium
        , utm_campaign
        , visit_date::date as visit_date
        , count(visitor_id) as visitors_count
        , count(
            case when created_at is not null 
            then visitor_id
            end) as leads_count,
        count(case when status_id = 142 
        	then visitor_id 
        	end) as purchases_count,
        sum(case when status_id = 142 
        	then amount 
        	end) as revenue
	from visitors_pay as vp
	where RN=1
	group by utm_source
        , utm_medium
        , utm_campaign
        , visit_date::date
	order by revenue desc nulls last
),
costs_marketing as (
	select
        campaign_date::date as visit_date
        , utm_source
        , utm_medium
        , utm_campaign
        , sum(daily_spent) as total_cost
    from ya_ads
    group by campaign_date
        , utm_source
        , utm_medium
        , utm_campaign
    union all
    select
        campaign_date::date as visit_date
        , utm_source
        , utm_medium
        , utm_campaign
        ,sum(daily_spent) as total_cost
    from vk_ads
    group by campaign_date
        , utm_source
        , utm_medium
        , utm_campaign)
select tv.visit_date
	, tv.utm_source
	, tv.utm_medium
	, tv.utm_campaign
	, visitors_count
	, total_cost
	, leads_count
	, purchases_count
	, revenue
from total_visitors as tv
	left join costs_marketing as cm on  tv.visit_date = cm.visit_date
								        and tv.utm_source = cm.utm_source
								        and tv.utm_medium = cm.utm_medium
								        and tv.utm_campaign = cm.utm_campaign
order by revenue desc nulls last
		, visit_date asc 
		, visitors_count desc
		, utm_source asc
		, utm_medium asc
		, utm_campaign asc
								        
