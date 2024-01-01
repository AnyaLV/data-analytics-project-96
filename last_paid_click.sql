with visitors_pay as (
	select ses.*
		, l.lead_id 
		, l.amount
		, l.created_at 
		, l.closing_reason 
		, l. status_id
		, row_number() over (partition by ses.visitor_id order by ses.visit_date desc) as RN
	from sessions as ses
		left join leads as l on ses.visitor_id=l.visitor_id
								and ses.visit_date<=l.created_at
	where ses. medium != 'organic'
)
select vp.visitor_id
		, vp.visit_date
		, vp.source as utm_source
		, vp.medium as utm_medium
		, vp.campaign as utm_campaign
		, vp.lead_id
		, vp.created_at
		, vp.amount
		, vp.closing_reason
		, vp.status_id
from visitors_pay as vp
where RN=1
order by amount desc nulls last
		, vp.visit_date asc 
		, utm_source asc 
		, utm_medium asc 
		, utm_campaign asc
