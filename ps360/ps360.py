import time
import logging
from os import environ
from datetime import datetime, timedelta
from enum import StrEnum
from dataclasses import dataclass
from zeep import Client, ns, Plugin
from zeep.cache import SqliteCache
from zeep.transports import Transport
from zeep.ns import SOAP_ENV_12
from lxml import etree # type: ignore
import psycopg


HOST = environ['PS360_HOST']
USERNAME = environ['PS360_USER']
PASSWORD = environ['PS360_PASSWORD']
SEARCH_PREVIOUS_MINUTES = 10
TIME_ZONE_ID = 'New Zealand Standard Time'
LOCALE = 'en-NZ'
PS_VERSION = '7.0.212.0'
SITE_ID = 0

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

class Powerscribe:

    _account_session: etree.Element | None
    _account_id: int
    first_name: str
    last_name: str
    last_updated: datetime
    users: dict[int, User] = {}

    def __init__(self):
        self.last_updated = datetime.now().astimezone() - timedelta(minutes=SEARCH_PREVIOUS_MINUTES)
        self._account_session = None
        self._transport = Transport(cache=SqliteCache())
        self.session_client = Client(f'http://{HOST}/RAS/Session.svc?wsdl', transport=self._transport, plugins=[SaveAccountSessionPlugin(self)])
        self.explorer_client = Client(f'http://{HOST}/RAS/Explorer.svc?wsdl', transport=self._transport)
        self.report_client = Client(f'http://{HOST}/RAS/Report.svc?wsdl', transport=self._transport)

    def login(self, username: str, password: str):
        sign_in_result = self.session_client.service.SignIn(
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
        logging.info(f'New Powerscribe session: {self.first_name} {self.last_name} with account ID {self._account_id} and session ID {self._account_session.text}')

    def logout(self):
        if self._account_session is not None:
            sessionId = self._account_session.text
            if self.session_client.service.SignOut(_soapheaders=[self._account_session]):
                self._account_session = None
                logging.info(f'Signed out: session ID {sessionId}')

    def get_latest_orders(self):
        now = datetime.now().astimezone()
        response = self.explorer_client.service.BrowseOrders(
            siteID=SITE_ID,
            time=dict(
                Period='Custom',
                From=self.last_updated.isoformat(timespec='milliseconds'),
                To=now.isoformat(timespec='milliseconds'),
            ),
            orderStatus='Completed',
            transferStatus='All',
            reportStatus='Reported',
            sort='LastModifiedDate ASC',
            pageSize=3000,
            pageNumber=1,
            _soapheaders=[self._account_session],
        ) or []
        if len(response):
            logging.info (f'Found {len(response)} updated orders since {self.last_updated}')
        self.last_updated = now
        users_to_upload: set[int] = set()
        for report in response:
            if report.Signer is not None and (events := self.report_client.service.GetReportEvents(
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
        with psycopg.connect(environ['DB_CONN']) as conn:
            with conn.cursor() as cur:
                cur.executemany('''update users set ps360_last_event_type=%s, ps360_last_event_timestamp=%s, ps360_last_event_workstation=%s where ps360=%s''', [(
                    self.users[userId].last_event.event_type,
                    self.users[userId].last_event.timestamp,
                    self.users[userId].last_event.workstation,
                    userId,
                ) for userId in users_to_upload])

class SaveAccountSessionPlugin(Plugin):
    def __init__(self, ps : Powerscribe):
        self.ps = ps

    def ingress(self, envelope, http_headers, operation):
        self.ps._account_session = envelope.find('./s:Header/AccountSession', {'s': SOAP_ENV_12})
        return envelope, http_headers

    def egress(self, envelope, http_headers, operation, binding_options):
        return envelope, http_headers

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(message)s')
    ps = Powerscribe()
    while True:  # Outer loop for the login/logout cycle
        logging.info("Starting new session")
        try:
            ps.login(USERNAME, PASSWORD)
            session_duration = 24 * 60 * 60  # 24 hours in seconds
            session_start_time = time.time()
            while (time.time() - session_start_time) < session_duration:
                ps.get_latest_orders()
                time.sleep(60) # 1 minute in seconds
            logging.info("Session finished.")
        except Exception as e:
            logging.error(f"An error occurred in the main loop: {e}")
            logging.info("Retrying after 1 minute...")
            time.sleep(60)
        finally:
            ps.logout()