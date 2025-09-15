import threading
import logging
import slixmpp
from slixmpp.xmlstream import ET
import asyncio
import ssl
import enum
import re
import pymssql
from os import environ
from dataclasses import dataclass
from psycopg_pool import ConnectionPool
from datetime import datetime

JID = environ['XMPP_JID']
PASSWORD = environ['XMPP_PASSWORD']
SERVER = environ.get('XMPP_SERVER', 'app-inteleradha-p.healthhub.health.nz')
SERVER_PORT = int(environ.get('XMPP_PORT', '5222'))
DB_CONN = environ['DB_CONN']

class Presence(enum.StrEnum):
    AVAILABLE = "Available"
    AWAY = "Away"
    BUSY = "Busy"
    OFFLINE = "Offline"

def presence_from_dict(d: dict[str, dict]) -> Presence:

    try:
        presence = next(iter(d.values()))
        match presence['show']:
            case '':
                return Presence.AVAILABLE
            case 'away':
                return Presence.AWAY
            case 'dnd':
                return Presence.BUSY
            case _:
                return Presence.OFFLINE
    except StopIteration:
        return Presence.OFFLINE

def generate_jid(pacs: str):
    return re.sub(r'([A-Z])', lambda m: '|' + m.group(1).lower(), pacs) + '@cdhb'

def generate_pacs(jid: str):
    return re.sub(r'\|([a-z])', lambda m: m.group(1).upper(), jid.split('@')[0])

def phys_sched_connection():
    return pymssql.connect(
        server=environ['PHYSCH_HOST'],
        user=f"cdhb\\{environ['SSO_USER']}",
        password=environ['SSO_PASSWORD'],
        database='PhySch',
        tds_version='7.4',
    )

@dataclass
class User:
    name: str
    presence: Presence
    updated: int

    def toJSON(self):
        return dict(
            name=self.name,
            presence=self.presence.value,
            updated=self.updated,
        )

class XMPP(slixmpp.ClientXMPP):

    def __init__(self, jid, password):
        super().__init__(jid, password)
        self.pool = ConnectionPool(
            DB_CONN,
            min_size=1,
            max_size=4,
            open=True,
        )
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(r"""update users set pacs_presence = 'Offline'""")
                cur.execute(r"""select pacs from users where show_in_locator""")
                self.jids = set(generate_jid(pacs) for (pacs,) in cur)
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.enable_direct_tls = False
        self.auto_authorize = False
        self.auto_subscribe = False

        self.users_lock = threading.Lock()
        self.users: dict[str, User] = {}

        self.add_event_handler("session_start", self.start)
        self.add_event_handler("roster_update", self.handle_roster_update)
        self.add_event_handler("changed_status", self.handle_changed_status)
        self.add_event_handler("message", self.message_received)


    async def start(self, event):
        await self.get_roster()
        self.send_presence()

    def handle_changed_status(self, presence):
        jid:str = presence['from'].bare
        pacs = generate_pacs(jid)
        with self.pool.connection() as conn:
            conn.execute('''update users set pacs_presence=%s, pacs_last_updated=now() where pacs=%s''', (
                presence_from_dict(self.client_roster.presence(jid)),
                pacs,
            ))
        with self.users_lock:
            if pacs in self.users:
                user = self.users[pacs]
                new_presence = presence_from_dict(self.client_roster.presence(jid))
                if user.presence != new_presence:
                    logging.info(f"{user.name}: {user.presence} -> {new_presence}")
                    user.presence = new_presence
                    user.updated = int(datetime.now().timestamp())

    async def handle_roster_update(self, iq):
        valid_jids = [jid.bare for jid in iq['roster']['items'] if jid.bare in self.jids]
        if len(valid_jids) == 0: return
        new_query = self.make_iq_get()
        query = ET.Element('{jabber:iq:roster-dynamic}query')
        new_query.set_payload(query)
        batch = ET.Element('item', attrib={'type': 'batch'})
        query.append(batch)
        for jid in valid_jids:
            item = ET.Element('item', attrib={'jid': jid})
            batch.append(item)
        people = await new_query.send()
        with self.users_lock:
            for person in people.xml.iterfind(".//{jabber:iq:roster-dynamic}item[@jid]"):
                jid = person.attrib['jid']
                user = User(
                    person.findtext("{jabber:iq:roster-dynamic}full-name"),
                    presence_from_dict(self.client_roster.presence(jid)),
                    0,
                )
                self.users[generate_pacs(jid)] = user

    def get_response(self, msg) -> str:
        match msg['body']:
            case 'roster':
                pacs = generate_pacs(msg['from'].bare)
                with self.pool.connection() as conn:
                    with conn.execute("select first_name, last_name, physch from users where pacs=%s", (pacs,)) as cur:
                        result = cur.fetchone()
                if result is not None:
                    first_name, last_name, physch = result
                    with phys_sched_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(r"""
                                select ShiftName
                                from SchedData
                                join Employee on SchedData.EmployeeID = Employee.EmployeeID
                                join Shift on SchedData.ShiftID = Shift.ShiftID
                                where AssignDate = year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP)
                                and Employee.Abbr = %s
                                order by Shift.DisplayOrder, Shift.ShiftName
                                """, (physch,))
                            return f'''\nToday's roster for {first_name} {last_name} ({physch}):\n{'\n'.join(shift for shift, in cursor)}'''
                else:
                    return f'Username "{pacs}" is not registered in the database, please contact my overlords'
            case _:
                return '''\nEchobot commands:\nroster: Show today's roster'''

    def message_received(self, msg):
        if msg['type'] == 'chat':
            logging.info(f'{generate_pacs(msg['from'].bare)}: "{msg['body']}"')
            payload = next((p for p in (
                msg.xml.find('{com.intelerad.viewer.im.extensions.orderContainer2}orderContainer'),
                msg.xml.find('{com.intelerad.viewer.im.extensions.orderContainer}orderContainer'),
                msg.xml.find('{com.intelerad.viewer.im.extensions.phoneRequestAction}phoneRequestAction'),
            ) if p is not None), None)
            reply = msg.reply(msg['body'] if payload is not None else f'{self.get_response(msg)}')
            reply['to']=reply['to'].bare
            if payload is not None: reply.set_payload(payload)
            reply.send()

    def reconnect(self, wait: int | float = 2, reason: str = "Reconnecting") -> None:
        logging.info('Scheduled reconnect...')
        super().reconnect(wait, reason)


# @app.get('/xmpp/online')
# def get_online():
#     return {pacs: user.toJSON() for pacs, user in xmpp_client.users.items() if user.presence != Presence.OFFLINE}

# @app.get('/xmpp/all')
# def get_all():
#     return {pacs: user.toJSON() for pacs, user in xmpp_client.users.items()}
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(message)s')

    xmpp_client = XMPP(JID, PASSWORD)

    if xmpp_client.connect(SERVER, SERVER_PORT):
        xmpp_client.schedule("Daily reconnect", 60*60*24, xmpp_client.reconnect, repeat=True)
        asyncio.get_event_loop().run_forever()

    else:
        logging.error("Unable to connect to the XMPP server.")