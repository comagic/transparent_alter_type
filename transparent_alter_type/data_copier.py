import datetime
import time


class DataCopier:
    def __init__(self, args, table, db):
        self.args = args
        self.table = table
        self.table_name = self.table['name']
        self.pk_columns = self.table['pk_columns']
        self.pk_types = self.table['pk_types']
        self.db = db
        self.last_pk = None

    def log(self, message):
        print(f'{self.table_name}: {message}')

    @staticmethod
    def duration(start_time):
        return str(datetime.timedelta(seconds=int(time.time() - start_time)))

    async def copy_data(self, i):
        ts = time.time()
        self.log(f'copy data: start ({i}: {self.table["pretty_data_size"]})')
        if self.args.batch_size == 0:
            await self.copy_data_direct()
        else:
            await self.copy_data_batches()
        self.log(f'copy data: done ({i}: {self.table["pretty_data_size"]}) in {self.duration(ts)}')

    async def copy_data_direct(self):
        await self.db.execute(f'''
            insert into {self.table_name}__tat_new
              select *
                from only {self.table_name}
        ''')

    async def copy_data_batches(self):
        last_batch_size = await self.copy_next_batch()
        while last_batch_size == self.args.batch_size:
            last_batch_size = await self.copy_next_batch()

    async def copy_next_batch(self):
        pk_columns = ', '.join(self.pk_columns)
        predicate = self.get_predicate()
        if len(self.pk_columns) == 1:
            select_query = f'''
                select max({self.pk_columns[0]}) as {self.pk_columns[0]}, count(1)
                  from batch
            '''
        else:
            select_query = f'''
                select {pk_columns}, count
                  from (select {pk_columns}, row_number() over (), count(1) over ()
                          from batch) x
                 where x.row_number = x.count
            '''
        batch = await self.db.fetchrow(f'''
            with batch as (
              insert into {self.table_name}__tat_new
                select *
                  from only {self.table_name}
                 where {predicate}
                 order by {pk_columns}
                 limit {self.args.batch_size}
              returning {pk_columns}
            )
            {select_query}
        ''')

        if batch is None or batch['count'] == 0:
            return 0
        self.last_pk = [batch[column] for column in self.pk_columns]
        return batch['count']

    def get_last_pk_value(self, i):
        col_type = self.pk_types[i]
        if col_type in ['integer', 'bigint']:
            return str(self.last_pk[i])
        return f"'{self.last_pk[i]}'::{col_type}"

    def get_predicate(self):
        if self.last_pk is None:
            return 'true'
        if len(self.pk_columns) == 1:
            return f"{self.pk_columns[0]} > {self.get_last_pk_value(0)}"
        else:
            pk_columns = ', '.join(self.pk_columns)
            pk_values = ', '.join(self.get_last_pk_value(i) for i in range(len(self.pk_columns)))
            return f'({pk_columns}) > ({pk_values})'
