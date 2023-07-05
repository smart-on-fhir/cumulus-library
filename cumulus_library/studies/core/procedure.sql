create table core__procedure AS
WITH join_coding as
(
    select id, proc_coding
    from procedure,
    unnest (code.coding) as t(proc_coding)
)
select
    coalesce(proc_coding.system, '?') as proc_system,
    coalesce(proc_coding.code, '?') as proc_code,
    coalesce(proc_coding.display, '?') as proc_display,
    performeddatetime,
    status,
    concat('Patient/', subject.reference) as subject_ref,
    concat('Encounter/', encounter.reference) as encounter_ref,
    concat('Proecure/', P.id) as procedure_ref,
    P.id as proc_id
from        procedure   as P
left join   join_coding on P.id = join_coding.id;