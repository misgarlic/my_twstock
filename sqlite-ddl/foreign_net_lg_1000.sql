select * from daily_three t
left join daily_price p on p.stock_id = t.stock_id
where t.deal_date >= '2018-08-01'
and for_net > 1000000 --and deal_net < 0
and p.deal_date = t.deal_date
and t.stock_id = 2883