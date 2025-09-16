import asyncio
import logging
import httpx
from os import environ
from datetime import datetime, timedelta
from enum import StrEnum
from dataclasses import dataclass
from psycopg_pool import AsyncConnectionPool
from zeep import AsyncClient, Plugin
from zeep.cache import SqliteCache
from zeep.transports import AsyncTransport
from zeep.ns import SOAP_ENV_12
from lxml import etree # type: ignore

HOST = environ['PS360_HOST']
USERNAME = environ['PS360_USER']
PASSWORD = environ['PS360_PASSWORD']
TIME_ZONE_ID = 'New Zealand Standard Time'
LOCALE = 'en-NZ'
PS_VERSION = '7.0.212.0'
SITE_ID = 0

SEARCH_PREVIOUS_MINUTES = 60
SESSION_DURATION_SECONDS = 24 * 60 * 60  # 24 hours
POLLING_INTERVAL_SECONDS = 60

class EventType(StrEnum):
    SIGN = 'Sign'
    EDIT = 'Edit'
    QUEUE_FOR_SIGNATURE = 'QueueForSignature'
    OVERREAD = 'Overread'

@dataclass
class UserLastEvent():
    event_type: EventType
    timestamp: datetime
    workstation: str
    additional_info: str

@dataclass
class User():
    id: int
    name: str
    last_event: UserLastEvent

class PS360:

    _account_session: etree.Element | None
    _account_id: int
    first_name: str
    last_name: str
    last_updated: datetime
    users: dict[int, User] = {}

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool
        self.last_updated = datetime.now().astimezone() - timedelta(minutes=SEARCH_PREVIOUS_MINUTES)
        self._account_session = None
        self._transport = AsyncTransport(
            cache=SqliteCache(timeout=None), # type: ignore
            wsdl_client=httpx.Client(),
            client=httpx.AsyncClient(),
            )
        self.session_client = AsyncClient(f'http://{HOST}/RAS/Session.svc?wsdl', transport=self._transport, plugins=[SaveAccountSessionPlugin(self)])
        self.explorer_client = AsyncClient(f'http://{HOST}/RAS/Explorer.svc?wsdl', transport=self._transport)
        self.report_client = AsyncClient(f'http://{HOST}/RAS/Report.svc?wsdl', transport=self._transport)

    async def login(self, username: str, password: str):
        sign_in_result = await self.session_client.service.SignIn(
            loginName=username,
            password=password,
            adminMode=False,
            version=PS_VERSION,
            workstation='',
            locale=LOCALE,
            timeZoneId=TIME_ZONE_ID,
        )
        assert self._account_session is not None
        self._account_id = sign_in_result.SignInResult.AccountID
        self.first_name = sign_in_result.SignInResult.Person.FirstName
        self.last_name = sign_in_result.SignInResult.Person.LastName
        logging.info(f'New PS360 session: {self.first_name} {self.last_name} with account ID {self._account_id} and session ID {self._account_session.text}')

    async def logout(self):
        if self._account_session is not None:
            sessionId = self._account_session.text
            if await self.session_client.service.SignOut(_soapheaders=[self._account_session]):
                self._account_session = None
                logging.info(f'PS360 signed out: session ID {sessionId}')

    async def get_latest_orders(self):
        response = await self.explorer_client.service.BrowseOrders(
            siteID=SITE_ID,
            time=dict(
                Period='Custom',
                From=(self.last_updated + timedelta(milliseconds=500)).isoformat(timespec='milliseconds'),
                To=datetime.now().astimezone().isoformat(timespec='milliseconds'),
            ),
            orderStatus='Completed',
            transferStatus='All',
            reportStatus='Reported',
            # accountID=123,
            sort='LastModifiedDate ASC',
            pageSize=3000,
            pageNumber=1,
            _soapheaders=[self._account_session],
        ) or []
        if len(response):
            logging.info (f'Found {len(response)} updated orders since {self.last_updated}')
        users_to_upload: set[int] = set()
        for report in response:
            if report.LastModifiedDate > self.last_updated:
                self.last_updated = report.LastModifiedDate
            if (events := await self.report_client.service.GetReportEvents(
                    reportID=report.ReportID,
                    eventsWithContent=True,
                    excludeViewEvents=True,
                    fetchBlob=False,
                    _soapheaders=[self._account_session],
                )) is not None:
                for event in events:
                    try:
                        event_type = EventType(event.Type)
                    except ValueError:
                        continue
                    last_event = UserLastEvent(
                        event_type,
                        event.EventTime,
                        event.Workstation,
                        event.AdditionalInfo,
                    )
                    userId = event.Account.ID
                    try:
                        user = self.users[userId]
                        if user.last_event.timestamp < last_event.timestamp:
                            user.last_event = last_event
                        else:
                            continue
                    except KeyError:
                        user = User(
                            userId,
                            event.Account.Name,
                            last_event,
                        )
                        self.users[userId] = user
                    users_to_upload.add(userId)
                    logging.info(f'{user.last_event.timestamp}: {user.last_event.event_type} by {user.name} (ID: {user.id}) on {user.last_event.workstation} ({user.last_event.additional_info})')
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany('''update users set ps360_last_event_type=%s, ps360_last_event_timestamp=%s, ps360_last_event_workstation=%s where ps360=%s''', [(
                    self.users[userId].last_event.event_type,
                    self.users[userId].last_event.timestamp,
                    self.users[userId].last_event.workstation,
                    userId,
                ) for userId in users_to_upload])

    async def main_loop(self):
        while True:
            logging.info("Starting new PS360 session")
            try:
                await self.login(USERNAME, PASSWORD)
                session_start_time = asyncio.get_event_loop().time()
                while (asyncio.get_event_loop().time() - session_start_time) < SESSION_DURATION_SECONDS:
                    await self.get_latest_orders()
                    await asyncio.sleep(POLLING_INTERVAL_SECONDS)
                logging.info("PS360 session finished.")
            except Exception as e:
                logging.error(f"PS360 error occurred: {e}")
                logging.info("Waiting for 1 minute before retrying...")
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                logging.info("PS360 loop cancelled, exiting...")
                break
            finally:
                await self.logout()


class SaveAccountSessionPlugin(Plugin):
    def __init__(self, ps : PS360):
        self.ps = ps

    def ingress(self, envelope, http_headers, operation):
        self.ps._account_session = envelope.find('./s:Header/AccountSession', {'s': SOAP_ENV_12})
        return envelope, http_headers

    def egress(self, envelope, http_headers, operation, binding_options):
        return envelope, http_headers