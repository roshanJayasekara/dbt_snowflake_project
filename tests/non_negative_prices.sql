-- Fails if any price/fee is negative
select *
from {{ ref('silver_bookings') }}
where (base_price is not null and base_price < 0)
   or (cleaning_fee is not null and cleaning_fee < 0)
   or (service_fee is not null and service_fee < 0)
