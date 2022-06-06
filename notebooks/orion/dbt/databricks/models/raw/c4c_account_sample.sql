with final as (
	select * from {{ source('staging', 'c4c_account') }}
	limit 90
)

select * from final