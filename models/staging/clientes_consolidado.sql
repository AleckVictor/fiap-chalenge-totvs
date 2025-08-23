with nps_relacional as (
	select 
		metadata_codcliente as CD_CLIENTE,
		resposta_NPS as nps,
		cast(respondedAt as datetime) as nps_respondedAt,
		row_number() over(partition by metadata_codcliente order by respondedAt desc) as rn
	from {{ref('nps_relacional')}}
	group by metadata_codcliente, resposta_NPS, respondedAt
),
ticket_qtd as (
	select 
		CODIGO_ORGANIZACAO as CD_CLIENTE,
		count(*) as tickets
	from {{ref('tickets')}}
	group by CODIGO_ORGANIZACAO
),
mrr as (
	select 
		cliente as CD_CLIENTE,
		cast(MRR_12M as float) as MRR_12M
	from {{ref('mrr')}} 
)
select
	dc.CD_CLIENTE,
	DS_PROD,
	DS_LIN_REC,
	CIDADE,
	DS_CNAE,
	DS_SEGMENTO,
	DS_SUBSEGMENTO,
	FAT_FAIXA,
	MARCA_TOTVS,
	MODAL_COMERC,
	PAIS,
	PERIODICIDADE,
	SITUACAO_CONTRATO,
	UF,
	TRY_CONVERT(decimal(38,18),
		REPLACE(REPLACE(LTRIM(RTRIM(VL_TOTAL_CONTRATO)), '.', ''), ',', '.')) AS VL_TOTAL_CONTRATO,
	DT_ASSINATURA_CONTRATO,
	nps,
	nps_respondedAt,
	tickets as qtd_tickets,
	MRR_12M
from {{ref('dados_clientes')}} dc
	left join nps_relacional on nps_relacional.CD_CLIENTE = dc.CD_CLIENTE
		and rn = 1
	left join ticket_qtd on ticket_qtd.CD_CLIENTE = dc.CD_CLIENTE
	left join mrr on mrr.CD_CLIENTE = dc.CD_CLIENTE