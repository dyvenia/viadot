with final as (
    select c.id, a.id as account_id, c.contactemail as contact_email
    from {{ ref('c4c_contact_sample') }} c
    left join {{ ref('c4c_account_sample') }} a
    on c.accountid = a.id
)

select * from final