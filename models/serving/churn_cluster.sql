with nps as (
		select distinct 
			cd_cliente,
			nps
		from {{ref('clientes_consolidado')}} cc
		where nps is not null
	)
select
	cr.CD_CLIENTE,
	nps.nps,
	cr.risco_churn_max,
	case
		when nps.nps < 6 and cr.risco_churn_max > 0.5 then 'Alto'
		when nps.nps < 8 and cr.risco_churn_max > 0.5 then 'Atenção'
		when nps.nps < 8 and cr.risco_churn_max <= 0.5 then 'Neutro'
		when nps.nps >= 8 and cr.risco_churn_max <= 0.5 then 'Fomento'
	end as CLUSTER_RISCO 
from {{source('serving', 'clientes_risco')}} cr 
	join nps on nps.cd_cliente = cr.CD_CLIENTE