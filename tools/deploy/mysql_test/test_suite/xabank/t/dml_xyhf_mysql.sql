
select * from cool_business as cb
left join cool_agent as ca on ca.agent_id = cb.agent_id
left join cool_channel as ch on ch.channel_id = ca.channel_id
left join cool_openplatform_businessmainkey as cob on cob.business_id = cb.business_id
where ch.channel_code = '10014' and cob.bill_reback_url <> '';

select count(1) from (select business.agent_id,clearing.mchnt_no,clearing.clear_date from cool_clearing as clearing 
left join cool_business as business on clearing.mchnt_no = business.mch_code) as b 
LEFT JOIN cool_agent as agent on b.agent_id = agent.agent_id 
where agent.parent_id = '';

SELECT q.refund_seq,q.mch_code,q.channel_code,r.out_trade_no,r.pay_pass,q.number FROM cool_refund_query as q 
left join cool_refund as r  ON r.refund_seq = q.refund_seq and r.out_trade_no = q.out_trade_no 
WHERE r.status = 3 AND q.number < 2 AND q.created_at < '1511770504' AND q.created_at >= '1543457431';

select o.open_flag,o.business_id,o.app_id from cool_deal r 
left join cool_business b ON r.business_id = b.business_id 
left join cool_openplatform o on b.business_id = o.business_id 
where r.out_trade_id='44479005094076018101909300000144';