import logging
import slixmpp
import asyncio
import ssl
import enum
import re
import pymssql
from os import environ
from dataclasses import dataclass
from psycopg_pool import AsyncConnectionPool

XMPP_JID = environ['XMPP_JID']
XMPP_PASSWORD = environ['XMPP_PASSWORD']
XMPP_SERVER = environ.get('XMPP_SERVER', 'app-inteleradha-p.healthhub.health.nz')
XMPP_PORT = int(environ.get('XMPP_PORT', '5222'))
XMPP_RESET_PRESENCE = (environ.get('XMPP_RESET_PRESENCE', 'False') == 'True')
PHYSCH_HOST = environ['PHYSCH_HOST']
PHYSCH_DB = environ.get('PHYSCH_DB', 'PhySch')
SSO_USER = environ['SSO_USER']
SSO_PASSWORD = environ['SSO_PASSWORD']

RECONNECT_DELAY = 15 # seconds

def phys_sched_connection():
    return pymssql.connect(
        server=PHYSCH_HOST,
        user=f"cdhb\\{SSO_USER}",
        password=SSO_PASSWORD,
        database=PHYSCH_DB,
        tds_version='7.4',
    )

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

    def __init__(self, pool: AsyncConnectionPool):
        super().__init__(XMPP_JID, XMPP_PASSWORD)
        self.pool = pool
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.enable_direct_tls = False
        self.auto_authorize = False
        self.auto_subscribe = False
        self.add_event_handler("session_start", self.on_session_start)
        self.add_event_handler("changed_status", self.on_changed_status)
        self.add_event_handler("message", self.on_message_received)

    async def on_session_start(self, _):
        await self.get_roster()
        self.send_presence()

    async def on_changed_status(self, presence: slixmpp.Presence):
        jid = presence.get_from().bare
        pacs = generate_pacs(jid)
        new_presence = presence_from_dict(self.client_roster.presence(jid)) # type: ignore
        async with self.pool.connection() as conn:
            async with await conn.execute('''update users set pacs_presence=%s, pacs_last_updated=now() where pacs=%s and pacs_presence<>%s''', (
                new_presence,
                pacs,
                new_presence,
            )) as cur:
                if (cur.rowcount > 0):
                    logging.info(f'{pacs}: {new_presence}')
                

    async def on_message_received(self, msg: slixmpp.Message):
        if msg.get_type() == 'chat':
            logging.info(f'{generate_pacs(msg.get_from().bare)}: "{msg['body']}"')
            payload = next((p for p in (
                msg.xml.find('{com.intelerad.viewer.im.extensions.orderContainer2}orderContainer'),
                msg.xml.find('{com.intelerad.viewer.im.extensions.orderContainer}orderContainer'),
                msg.xml.find('{com.intelerad.viewer.im.extensions.phoneRequestAction}phoneRequestAction'),
            ) if p is not None), None)
            reply = msg.reply(msg['body'] if payload is not None else f'{await self.generate_response(msg)}')
            reply.set_to(reply.get_to().bare)
            if payload is not None: reply.set_payload(slixmpp.ElementBase(payload))
            reply.send()


    def phys_sched_roster(self, first_name: str, last_name: str, physch_abbr: str, tomorrow: bool) -> str:
        with phys_sched_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(r"""
                    select ShiftName
                    from SchedData
                    join Employee on SchedData.EmployeeID = Employee.EmployeeID
                    join Shift on SchedData.ShiftID = Shift.ShiftID
                    where AssignDate = year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP) + %s
                    and Employee.Abbr = %s
                    order by Shift.StartTime, Shift.EndTime, Shift.DisplayOrder, Shift.ShiftName, Shift.ShiftID
                    """, (1 if tomorrow else 0, physch_abbr))
                shifts = [shift for shift, in cursor] # type: ignore
                if len(shifts) > 0:
                    return f'''\n{"Tomorrow" if tomorrow else "Today"}'s roster for {first_name} {last_name} ({physch_abbr}):\n{'\n'.join(shifts)}'''
                else:
                    return f'{first_name} {last_name} ({physch_abbr}) is not rostered {"tomorrow" if tomorrow else "today"}'

    def phys_sched_meetings(self, tomorrow: bool) -> str:
        with phys_sched_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(r"""
                    select
                        timefromparts(Shift.StartTime/100, Shift.StartTime%100, 0, 0, 0),
                        ShiftName,
                        Employee.FirstName,
                        Employee.LastName
                    from SchedData
                            join Employee on SchedData.EmployeeID = Employee.EmployeeID
                            join Shift on SchedData.ShiftID = Shift.ShiftID
                    where AssignDate = year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP) + %s
                    and (AssignID in (109, 37) -- 3 SMO Clinical MDMs am, 4 SMO Clinical MDMs pm
                    or Shift.ShiftID = 1982) -- Gen Med Meeting Reg
                    and ShiftName not like '%Prep'
                    and Employee.EmployeeID <> 33 -- RMO/Fellow placeholder
                    order by Shift.StartTime, ShiftName
                    """, (1 if tomorrow else 0,))
                meetings = [f'{start_time:%H:%M}: {shift_name}: {first_name} {last_name}' for start_time, shift_name, first_name, last_name in cursor] # type: ignore
                if len(meetings) > 0:
                    return f'''\n{"Tomorrow" if tomorrow else "Today"}'s meetings:\n{'\n'.join(meetings)}'''
                else:
                    return f'No meetings {"tomorrow" if tomorrow else "today"}'

    async def generate_response(self, msg: slixmpp.Message) -> str:
        message: str = msg['body'].strip()
        if roster_match := re.search(r"^roster(.*)$", message, re.IGNORECASE):
            custom_user: str | None = None
            tomorrow = False
            if roster_match.group(1).strip().lower() == 'tomorrow':
                tomorrow = True
            else:
                custom_user = roster_match.group(1).strip() or None
            async with self.pool.connection() as conn:
                pacs = generate_pacs(msg.get_from().bare) if custom_user is None else None
                if custom_user is None:
                    pacs = generate_pacs(msg.get_from().bare)
                    coroutine = conn.execute("select first_name, last_name, physch from users where pacs=%s", (pacs,))
                else:
                    pacs = None
                    coroutine = conn.execute("select first_name, last_name, physch from users where %s in (lower(pacs), lower(ris), lower(physch), lower(sso), lower(first_name), lower(last_name)) limit 1", (custom_user.lower(),))
                async with await coroutine as cur:
                    result = await cur.fetchone()
            if result is not None:
                first_name, last_name, physch = result
                if physch is not None:
                    return self.phys_sched_roster(first_name, last_name, physch, tomorrow)
                else:
                    return f'My database does not contain the PhysSched code for {first_name} {last_name}'
            elif pacs is not None:
                return f'Username "{pacs}" is not registered in the database, please contact my overlords'
            else:
                return f'Could not find a user matching "{custom_user}"'
        elif meetings_match := re.search(r"^meetings(.*)$", message, re.IGNORECASE):
            tomorrow = meetings_match.group(1).strip().lower() == 'tomorrow'
            return self.phys_sched_meetings(tomorrow)
        else:
            return "\n" \
                "EchoBot commands:\n" \
                "roster: Show today's roster\n" \
                "roster tomorrow: Show tomorrow's roster\n" \
                "roster <name or login>: Show roster for another user\n" \
                "meetings: Show today's meetings\n" \
                "meetings tomorrow: Show tomorrow's meetings"

    async def main_loop(self):
        if XMPP_RESET_PRESENCE:
            logging.info("XMPP setting all users to offline...")
            async with self.pool.connection() as conn:
                await conn.execute("update users set pacs_presence='Offline'")
        while True:
            try:
                logging.info("XMPP client connecting...")
                await self.connect(XMPP_SERVER, XMPP_PORT)
                logging.info("XMPP client connected and processing.")
                await self.disconnected
            except Exception as e:
                # This could catch OS-level errors if the server is unreachable
                logging.error(f"XMPP connection failed: {e}")
            finally:
                logging.info(f"Reconnecting in {RECONNECT_DELAY} seconds...")
                await asyncio.sleep(RECONNECT_DELAY)
