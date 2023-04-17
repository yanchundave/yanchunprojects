CREATE OR REPLACE FUNCTION DBT.DEV_YANCHUN_PUBLIC.chime_remove_duplicate( balance_date DATE, balance FLOAT)
returns table(balance_time DATE, balance_value FLOAT)
language python
runtime_version=3.8
handler='ChimeRemoveDuplicate'
as $$
class ChimeRemoveDuplicate:
    def __init__(self):
        self._currentamount = 0


    def process(self, balance_date, balance):
        if self._currentamount == 0:
            self._currentamount = balance
            yield(balance_date, -1 * balance)

        elif balance != self._currentamount:
            if balance < self._currentamount:
                diff = self._currentamount - balance
                self._currentamount = balance
                yield(balance_date, diff)
            else:
                 self._currentamount = balance
        else:
            self._currentamount = balance
            yield None

    def end_partition(self):
        yield None
$$;

CREATE OR REPLACE FUNCTION DBT.DEV_YANCHUN_PUBLIC.UDF_NONCHIME_COMPETITOR(description STRING) RETURNS STRING AS
$$
    -- Top competitior: Albert, Brigit, Empower, Earnin --
    -- exclued chime since it is unique --
    CASE WHEN LOWER(description) LIKE 'albert instant%' OR LOWER(description) LIKE 'albert savings%' THEN 'Albert'
         WHEN LOWER(description) LIKE '%brigit%' THEN 'Brigit'
         WHEN LOWER(description) LIKE '%empower%' THEN 'Empower'
         WHEN LOWER(description) LIKE '%earnin%' AND LOWER(description) NOT LIKE '%learnin%' THEN 'EarnIn'
         WHEN LOWER(description) LIKE '%money%lion%' THEN 'Money Lion'
         WHEN LOWER(description) LIKE '%varo%' THEN 'Varo'
         WHEN LOWER(description) LIKE '%cash app%' THEN 'Cash App'
         ELSE NULL
    END
$$;

CREATE OR REPLACE FUNCTION DBT.DEV_YANCHUN_PUBLIC.UDF_NONCHIME_COMPETITOR(description STRING) RETURNS STRING AS
$$
    -- Top competitior: Albert, Brigit, Empower, Earnin --
    -- exclued chime since it is unique --
    CASE WHEN LOWER(description) LIKE 'albert instant%' OR LOWER(description) LIKE 'albert savings%' THEN 'Albert'
         WHEN LOWER(description) LIKE '%brigit%' THEN 'Brigit'
         WHEN LOWER(description) LIKE '%empower%' THEN 'Empower'
         WHEN LOWER(description) LIKE '%earnin%' AND LOWER(description) NOT LIKE '%learnin%' THEN 'EarnIn'
         WHEN LOWER(description) LIKE '%money%lion%' THEN 'Money Lion'
         ELSE NULL
    END
$$;