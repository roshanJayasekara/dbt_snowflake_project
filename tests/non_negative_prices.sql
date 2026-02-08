-- Fails if any price/fee is negative
select *
from {{ ref('silver_bookings') }}
where (cleaning_fee is not null and cleaning_fee < 0)
   or (service_fee is not null and service_fee < 0)
