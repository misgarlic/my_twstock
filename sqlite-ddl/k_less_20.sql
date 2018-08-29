select * from daily_ta t
left join daily_price p on t.stock_id = p.stock_id and t.deal_date = p.deal_date
where 1=1 and k < 30 and d < k
and p.deal_date = '2018-08-28' --and p.stock_id = '3481'