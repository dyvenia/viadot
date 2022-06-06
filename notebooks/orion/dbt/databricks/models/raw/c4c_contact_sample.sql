with final as (
	select * from {{ source('staging', 'c4c_contact') }}
	limit 100
)



-- final as (
-- 	select
-- 	Id,
-- 	AccountId,
-- 	FirstName,
-- 	LastName,
-- 	-- concat(FirstName, ' ', LastName) as Name
-- 	ContactEMail,
-- 	from raw_c4c_test
-- 	limit 3
-- )

select * from final