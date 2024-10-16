import json
from contextlib import asynccontextmanager

import asyncpg


def print_query(query, args):
    if args:
        args = f', {args=}'
    else:
        args = ''
    print(f'QUERY: {query}{args}\n')


class ConnectWrapper:
    def __init__(self, con, show_queries=False):
        self.con = con
        self.show_queries = show_queries

    def show_query(self, query, args):
        if not self.show_queries:
            return
        print_query(query, args)

    async def execute(self, query, *args):
        if not query:
            return
        self.show_query(query, args)
        return await self.con.execute(query, *args)

    async def fetch(self, query, *args):
        self.show_query(query, args)
        return await self.con.fetch(query, *args)

    async def fetchrow(self, query, *args):
        self.show_query(query, args)
        return await self.con.fetchrow(query, *args)

    async def fetchval(self, query, *args):
        res = await self.fetchrow(query, *args)
        if res:
            return res[0]


class PgPool:
    pool: asyncpg.Pool

    def __init__(self, args):
        self.args = args

    async def init_pool(self) -> None:
        async def init_connection(con):
            con._reset_query = ''
            await con.set_type_codec(
                typename="json",
                schema="pg_catalog",
                encoder=lambda x: x,
                decoder=json.loads,
            )
            await con.execute(f"set lock_timeout = '{self.args.lock_timeout}s';")
            await con.execute(f"set work_mem = '{self.args.work_mem}';")
            await con.execute(f"set maintenance_work_mem = '{self.args.work_mem}';")

        self.pool = await asyncpg.create_pool(
            database=self.args.dbname,
            user=self.args.user,
            password=self.args.password,
            host=self.args.host,
            port=self.args.port,
            min_size=self.args.jobs,
            max_size=self.args.jobs,
            statement_cache_size=0,
            init=init_connection
        )

    def show_query(self, query, args):
        if not self.args.show_queries:
            return
        print_query(query, args)

    async def execute(self, query, *args):
        if not query:
            return
        async with self.pool.acquire() as con:
            self.show_query(query, args)
            return await con.execute(query, *args)

    async def fetch(self, query, *args):
        async with self.pool.acquire() as con:
            self.show_query(query, args)
            return await con.fetch(query, *args)

    async def fetchrow(self, query, *args):
        async with self.pool.acquire() as con:
            self.show_query(query, args)
            return await con.fetchrow(query, *args)

    async def fetchval(self, query, *args):
        res = await self.fetchrow(query, *args)
        if res:
            return res[0]

    @asynccontextmanager
    async def transaction(self) -> asyncpg.Connection:
        async with self.pool.acquire() as con:
            async with con.transaction():
                yield ConnectWrapper(con, self.args.show_queries)
