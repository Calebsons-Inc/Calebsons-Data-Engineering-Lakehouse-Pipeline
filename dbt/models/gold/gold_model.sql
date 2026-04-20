select
  category,
  count(*) as transaction_count,
  round(sum(value), 2) as total_value,
  round(avg(value), 2) as average_value,
  max(order_date) as latest_order_date
from {{ ref('silver_model') }}
group by category
order by category
