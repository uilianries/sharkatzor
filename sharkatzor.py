from discord.ext import tasks
import discord
import requests
import googleapiclient.discovery
from googleapiclient.errors import HttpError

import logging
import json
import os
import base64
import asyncio
from copy import copy
from datetime import datetime
from zoneinfo import ZoneInfo


DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", None)
GENERAL_CHANNEL_ID = int(os.getenv("GENERAL_CHANNEL_ID", 0))
PRIVATE_CHANNEL_ID = int(os.getenv("PRIVATE_CHANNEL_ID", 0))
SHARED_CHANNEL_ID = int(os.getenv("SHARED_CHANNEL_ID", 0))
TWITCH_CHANNEL = os.getenv("TWITCH_CHANNEL", "tomahawk_aoe")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", None)
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", None)
YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID", "UCJ0vp6VTn7JuFNEMj5YIRcQ")
TIME_INTERVAL_SECONDS = int(os.getenv("TIME_INTERVAL_SECONDS", 60))
DND_INTERVAL_MINUTES = int(os.getenv("DND_INTERVAL_MINUTES", 15))
TWITCH_COOLDOWN = int(os.getenv("TWITCH_COOLDOWN", 6))
DISCORD_COOLDOWN = int(os.getenv("DISCORD_COOLDOWN", 6))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
DISCORD_ALLOWED_ROLES = [int(it) for it in os.getenv("DISCORD_ALLOWED_ROLES", "0").split(",")]
DISCORD_ALLOWED_USERS = [int(it) for it in os.getenv("DISCORD_ALLOWED_USERS", "0").split(",")]
DATABASE_PATH = os.getenv("DATABASE_PATH", "database.json")
GCP_API_KEYS = os.getenv("GCP_API_KEYS", []).split(",")
DND_INTERVAL = os.getenv("DND_INTERVAL", "00,09")
RETRY_MAX = 5
RETRY_TIME_INTERNAL = 10

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
S_HANDLER = logging.StreamHandler()
S_HANDLER.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
S_HANDLER.setFormatter(formatter)
LOGGER.addHandler(S_HANDLER)


class SharkatzorError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class Video(object):
    def __init__(self, id=None, title=None, json_data=None, time=None):
        self.id = id
        self.title = title
        self.time = time
        if json_data:
            self.id = json_data["resourceId"]["videoId"]
            try:
                self.title = json_data["title"]
            except Exception:
                self.title = ""

    def __str__(self):
        return json.dumps(dict(self), ensure_ascii=False)

    def __eq__(self, other):
        return self.id == other.id

    def __iter__(self):
        yield from {"yt_id": self.id, "yt_title": self.title}.items()

    def __repr__(self):
        return self.__str__()

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    @property
    def link(self):
        return f"https://www.youtube.com/watch?v={self.id}"

    @staticmethod
    def generate(json_data):
        return Video(json_data["yt_id"], json_data["yt_title"])

    def is_stale(self):
        if self.time is None:
            return True
        now = datetime.now()
        diff = now - self.time
        diff_hours = diff.total_seconds() / 3600
        return diff_hours > DISCORD_COOLDOWN


class Live(object):
    _format = "%Y%m%d%H%M"

    def __init__(self, time=None, title=None):
        self.time = time
        self.title = title

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __str__(self):
        return json.dumps(dict(self), ensure_ascii=False)

    def __eq__(self, other):
        return self.time == other.time

    def __iter__(self):
        yield from {"tw_time": Live.timetostr(self.time), "tw_title": self.title}.items()

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def strtotime(value):
        if value:
            return datetime.strptime(value, Live._format)

    @staticmethod
    def timetostr(value):
        if value:
            return value.strftime(Live._format)

    @property
    def link(self):
        return f"https://www.twitch.tv/{TWITCH_CHANNEL}"

    def is_stale(self):
        now = datetime.now()
        diff = now - self.time
        diff_hours = diff.total_seconds() / 3600
        return diff_hours > TWITCH_COOLDOWN

    @staticmethod
    def generate(json_data):
        return Live(Live.strtotime(json_data["tw_time"]), json_data["tw_title"])


class DBEntry(object):
    def __init__(self, video, live):
        self.video = copy(video)
        self.live = copy(live)

    def __str__(self):
        items = dict(self.video)
        items.update(dict(self.live))
        return json.dumps(items, ensure_ascii=False)

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        items = {**self.video.__dict__}
        items.update(self.live)
        yield from items

    def __dict__(self):
        items = {**self.video.__dict__}
        items.update(self.live)
        yield from items

    def __eq__(self, other):
        return self.video == other.video and self.live == other.live

    def b64encode(self):
        encoded = str(self).encode('utf-8')
        return base64.b64encode(encoded).decode()

    @staticmethod
    def generate(json_data):
        video = Video(json_data["yt_id"], json_data["yt_title"])
        live = Live(Live.strtotime(json_data["tw_time"]), json_data["tw_title"])
        return DBEntry(video, live)


class Sharkatzor(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.logger = LOGGER
        self.logger.info('Starting ...')
        self.access_token = None
        self.channel = None
        self.private_channel = None
        self.shared_channel = None
        self.db_entry = None
        self.live = None
        self.video = None
        self.youtube = None
        self.playlist = None
        self.loop_interval = TIME_INTERVAL_SECONDS

        self.logger.info(f'Twitch channel: {TWITCH_CHANNEL}')
        self.logger.info(f'Youtube channel ID: {YOUTUBE_CHANNEL_ID}')
        self.logger.info(f'Loop interval (secs): {TIME_INTERVAL_SECONDS}')
        self.logger.info(f'Twitch cooldown (hours): {TWITCH_COOLDOWN}')
        self.logger.info(f'Discord cooldown (hours): {DISCORD_COOLDOWN}')
        self.logger.info('Discord Token: {}****'.format(DISCORD_TOKEN[:4]))
        self.logger.info('General Discord channel: {}****'.format(str(GENERAL_CHANNEL_ID)[:4]))
        self.logger.info('Private Discord channel: {}****'.format(str(PRIVATE_CHANNEL_ID)[:4]))
        self.logger.info('Shared  Discord channel: {}****'.format(str(SHARED_CHANNEL_ID)[:4]))
        self.logger.info(f'Allowed Discord users: {DISCORD_ALLOWED_USERS}')
        self.logger.info(f'Allowed Discord roles: {DISCORD_ALLOWED_ROLES}')
        self.logger.info('Github Token: {}****'.format(str(GITHUB_TOKEN)[:4]))
        self.logger.info('Youtube keys: {}'.format(len(GCP_API_KEYS)))

        self.logger.info("Reading DB ...")
        self._read_db()
        self.logger.debug(f"Latest video: {self.video}")
        self.logger.debug(f"Latest Live: {self.live}")

        self.background_task.start()

    async def on_ready(self):
        self.logger.info(f'We have logged in as {self.user}')
        self.channel = self.get_channel(GENERAL_CHANNEL_ID)
        self.private_channel = self.get_channel(PRIVATE_CHANNEL_ID)
        self.shared_channel = self.get_channel(SHARED_CHANNEL_ID)
        self.logger.info(f"Started as `{self.user}`.")
        await self._login_twitch()
        await self._login_youtube()

    @tasks.loop(seconds=TIME_INTERVAL_SECONDS)
    async def background_task(self):
        dnd = await self._do_not_disturb()
        if dnd:
            self.loop_interval = DND_INTERVAL_MINUTES
            self.background_task.change_interval(minutes=self.loop_interval)
            return
        elif not dnd and self.loop_interval == DND_INTERVAL_MINUTES:
            self.loop_interval = TIME_INTERVAL_SECONDS
            self.background_task.change_interval(seconds=self.loop_interval)
        self.logger.debug("Pooling task")
        await self.publish_new_video()
        await self.publish_live()

    @background_task.before_loop
    async def before_task(self):
        self.logger.debug("Running before_task")
        await self.wait_until_ready()

    async def _login_youtube(self):
        self.logger.debug("Executing YT login")
        for key in GCP_API_KEYS:
            try:
                self.youtube = None
                self.logger.info("Connecting to YT with key {}****".format(key[:8]))
                youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=key)
                request = youtube.channels().list(part="id,contentDetails", id=YOUTUBE_CHANNEL_ID, maxResults=1)
                response = request.execute()
                if not response:
                    message = "Could not login on Youtube!"
                    self.logger.error(message)
                self.youtube = youtube
                self.playlist = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
                self.logger.info(f"Logged-in on youtube, Playlist ID: {self.playlist}")
                return
            except HttpError as err:
                self.logger.error(err.reason)
                pass
        raise SharkatzorError("Could not login on YT! Giving up!")

    async def _get_newest_video(self):
        try:
            if self.youtube is None:
                await self._login_youtube()
            request = self.youtube.playlistItems().list(part="id,snippet",
                                                        playlistId=self.playlist,
                                                        maxResults=1)
            response = request.execute()
            if not response:
                message = f"Could not scrap YT channel {YOUTUBE_CHANNEL_ID}!"
                self.logger.error(message)
                await self.private_channel.send(message)
                return None
            video = response["items"][0]["snippet"]
            self.logger.debug("Latest video on YT: {}".format(video["resourceId"]["videoId"]))
            return video
        except HttpError as err:
            self.logger.error(err.reason)
        except SharkatzorError as err:
            self.logger.error(err.message)

    async def _is_alive(self):
        if not await self._is_logged_in():
            self.logger.debug("Twitch login expired")
            await self._login_twitch()
        params = {'Client-ID': TWITCH_CLIENT_ID, 'Authorization':  "Bearer " + self.access_token}
        response = requests.get(f'https://api.twitch.tv/helix/streams?user_login={TWITCH_CHANNEL}', headers=params)
        if not response.ok:
            message = f"Could not fetch Twitch channel {TWITCH_CHANNEL}"
            self.logger.error(message)
            await self.private_channel.send(message)
            return False, None
        data = json.loads(response.text)['data']
        if not data:
            return False, None
        is_alive = data[0]['type'] == 'live'
        title = data[0]['title']
        self.logger.debug("Is alive on Twitch: {}".format("YES" if is_alive else "NO"))
        return is_alive, title

    async def _login_twitch(self):
        self.logger.debug("Login on Twitch")
        params = {'client_id': TWITCH_CLIENT_ID, 'client_secret': TWITCH_CLIENT_SECRET, 'grant_type': 'client_credentials'}
        retry_count = 0
        while retry_count < RETRY_MAX:
            try:
                response = requests.post(url='https://id.twitch.tv/oauth2/token', data=params)
                if not response.ok:
                    message = f"Could not login on Twitch: {response}"
                    await self.private_channel.send(message)
                    self.logger.error(message)
                    raise SharkatzorError(message)
                data = json.loads(response.text)
                if "access_token" not in data:
                    message = f"Could not find access_token: {response}"
                    await self.private_channel.send(message)
                    self.logger.error(message)
                    raise SharkatzorError(message)
                self.access_token = data["access_token"]
                self.logger.debug("Logged in on Twitch: {}*****".format(self.access_token[:4]))
                return
            except SharkatzorError as error:
                retry_count += 1
                if retry_count > RETRY_MAX:
                    raise Exception(error)
                self.logger.debug(f"Retry new login Twitch in {RETRY_TIME_INTERNAL} secs.")
                await asyncio.sleep(RETRY_TIME_INTERNAL)

    async def _is_logged_in(self):
        if self.access_token:
            params = {'Authorization':  "Bearer " + self.access_token, 'Client-ID': TWITCH_CLIENT_ID}
            response = requests.get(url='https://id.twitch.tv/oauth2/validate', headers=params)
            if response.ok:
                return True
            else:
                self.logger.info(f"Twitch login expired: {response.text}")
        return False

    @property
    def _database_url(self):
        return f"https://api.github.com/repos/uilianries/tomahawk-bot/contents/{DATABASE_PATH}"

    async def _write_db(self):
        sha = self._get_db_sha()
        dbentry = DBEntry(self.video, self.live)
        self.logger.debug(f"Write DB Entry: {dbentry}")
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        params = {"content": dbentry.b64encode(), "message": "Update DB", "branch": "database", "sha": sha}
        response = requests.put(url=self._database_url, headers=headers, json=params)
        if not response.ok:
            message = f"Could not update DB: {response.text}"
            await self.private_channel.send(message)
            self.logger.error(message)

    def _read_db(self):
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        params = {"ref": "database"}
        response = requests.get(url=self._database_url, headers=headers, params=params)
        if not response.ok:
            self.logger.error("Could not read DB")
            raise Exception(response)
        content = response.json()["content"]
        decoded = base64.b64decode(content).decode()
        json_data = json.loads(decoded)
        self.db_entry = DBEntry.generate(json_data)
        self.logger.info(f"Read DB Entry: {self.db_entry}")
        self.live = copy(self.db_entry.live)
        self.video = copy(self.db_entry.video)

    def _get_db_sha(self):
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        params = {"ref": "database"}
        response = requests.get(url=self._database_url, headers=headers, params=params)
        data = response.json()
        return data["sha"]

    async def publish_new_video(self):
        self.logger.debug("On publish_new_video")
        if self.video and not self.video.is_stale():
            self.logger.debug("Waiting for youtube cooldown")
            return
        video_data = await self._get_newest_video()
        if video_data is None:
            self.logger.error("Could not request newest video")
            return
        current_video = Video(json_data=video_data, time=datetime.now())
        if self.video is None:
            self.video = current_video
        elif self.video != current_video:
            self.logger.info(f"New YT video: {current_video.link}")
            await self.channel.send(f"Vídeo novo do Tomahawk no Youtube @everyone!\n**{current_video.title}**\n{current_video.link}")
            self.video = current_video
            await self._write_db()

    async def publish_live(self):
        self.logger.debug("On publish_live")
        if not self.live.is_stale():
            self.logger.debug(f"Waiting for live cooldown")
            return
        is_live, title = await self._is_alive()
        self.logger.debug("Is alive on Twitch: {}".format("YES" if is_live else "NO"))
        if is_live:
            self.live = Live(datetime.now(), title)
            self.logger.info(f"Live on Twitch is started.")
            await self.channel.send(f"Tomahawk está ao vivo na Twitch @everyone!\n**{self.live.title}**\n{self.live.link}")
            await self._write_db()

    async def on_message(self, message):
        await self._remove_twitch_message(message)

    async def on_message_edit(self, _, message):
        await self._remove_twitch_message(message)

    async def _remove_twitch_message(self, message):
        if message.embeds:
            if message.author.id not in DISCORD_ALLOWED_USERS and not any(role.id in DISCORD_ALLOWED_ROLES for role in message.author.roles):
                if message.channel.id == GENERAL_CHANNEL_ID:
                    for embed in message.embeds:
                        if ("//www.twitch.tv/" in embed.url or "//twitch.tv/" in embed.url) and "twitch.tv/tomahawk_aoe" not in embed.url:
                            self.logger.warning(f"Delete message - #{message.author.name}: {message.content}")
                            await message.delete()
                            await self.channel.send(f"{message.author.mention} favor utilizar o canal {self.shared_channel.mention} para postar link da Twitch.")
                            return
            if "free distribution of discord nitro" in str(message.content).lower():
                self.logger.warning(f"Delete message - #{message.author.name}: {message.content}")
                await message.delete()
                await message.author.kick("Usuário caiu no golpe do litrão e enviou phising no servidor.")
                await self.private_channel.send(f"Usuário {message.author.mention} caiu no golpe do litrão. Mensagem removida e usuário kickado.")

    async def _do_not_disturb(self):
        now = datetime.now(tz=ZoneInfo("America/Sao_Paulo"))
        min, max = DND_INTERVAL.split(",")
        dnd = int(min) <= now.hour <= int(max)
        if dnd:
            self.logger.debug(f"DND interval: ({min}) <= ({now.hour:02d}:{now.minute:02d}) <= ({max})")
        return dnd


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise ValueError("DISCORD_TOKEN is missing")
    if not GENERAL_CHANNEL_ID:
        raise ValueError("GENERAL_CHANNEL_ID is missing")
    if not PRIVATE_CHANNEL_ID:
        raise ValueError("PRIVATE_CHANNEL_ID is missing")
    if not SHARED_CHANNEL_ID:
        raise ValueError("SHARED_CHANNEL_ID is missing")
    if not TWITCH_CLIENT_ID:
        raise ValueError("TWITCH_CLIENT_ID is missing")
    if not TWITCH_CLIENT_SECRET:
        raise ValueError("TWITCH_CLIENT_SECRET is missing")
    if not GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN is missing")
    if not GCP_API_KEYS:
        raise ValueError("GCP_API_KEYS is missing")
    client = Sharkatzor()
    client.run(DISCORD_TOKEN)
