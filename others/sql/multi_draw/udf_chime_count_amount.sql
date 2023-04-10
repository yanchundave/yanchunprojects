CREATE OR REPLACE FUNCTION chime_count_value( T_AMOUNT FLOAT, FUNDING_NAME VARCHAR)
returns table(count_value number, total_amount FLOAT)
language python
runtime_version=3.8
handler='ChimeCountValue'
as $$
class ChimeCountValue:
    def __init__(self):
        self._count = 0
        self._amount = 0
        self._currentamount = 0
        self._lastamount = 0


    def process(self, t_amount):
        if self._currentamount == 0:
            self._currentamount = t_amount
            self._count += 1
            self._amount = t_amount
        elif t_amount != self._currentamount:
            if t_amount > self._lastamount:
                self._count += 1
                self._lastamount = t_amount
            if t_amount > self._amount:
                self._amount = t_amount
        else:
            pass
        yield None

    def end_partition(self):
        yield(self._count, self._amount)
$$;



