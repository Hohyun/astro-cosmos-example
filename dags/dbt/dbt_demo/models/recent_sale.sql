select date, count(*) cnt, sum(amt) total_sales
from edgar_sale
group by date
