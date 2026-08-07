-- The merge has to keep the latest arrival rather than merely one of them, and
-- a row count cannot tell the two apart. Every restatement the loader logged
-- should show its post-restatement amount here.
select
    log.claim_id,
    log.amount_after as injected,
    fct.billed_amount as landed
from {{ source('meta', 'injection_log') }} as log
join {{ ref('fct_claim') }} as fct on fct.claim_id = log.claim_id
where log.is_restatement
  and round(fct.billed_amount, 2) <> round(log.amount_after, 2)
