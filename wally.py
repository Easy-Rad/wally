
import asyncio
import logging
from os import environ
from psycopg_pool import AsyncConnectionPool
from xmpp import XMPP
from ps360 import PS360

DB_CONN = environ['DB_CONN']

async def main():
    async with AsyncConnectionPool(
        DB_CONN,
        min_size=1,
        max_size=4,
        open=False,
    ) as pool:
        xmpp = XMPP(pool) # type: ignore
        xmpp_task = asyncio.create_task(xmpp.main_loop())
        ps360 = PS360(pool) # type: ignore
        ps360_task = asyncio.create_task(ps360.main_loop())
        await asyncio.gather(xmpp_task, ps360_task)
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(message)s')
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down due to KeyboardInterrupt...")