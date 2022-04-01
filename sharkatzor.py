from discord.ext import tasks
import discord
import requests
import scrapetube
import logging
import json
import os
import base64
import asyncio
from copy import copy
from datetime import datetime


DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", None)
GENERAL_CHANNEL_ID = int(os.getenv("GENERAL_CHANNEL_ID", 0))
PRIVATE_CHANNEL_ID = int(os.getenv("PRIVATE_CHANNEL_ID", 0))
TWITCH_CHANNEL = os.getenv("TWITCH_CHANNEL", "tomahawk_aoe")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", None)
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", None)
YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID", "UCJ0vp6VTn7JuFNEMj5YIRcQ")
TIME_INTERVAL_SECONDS = int(os.getenv("TIME_INTERVAL_SECONDS", 60))
TWITCH_COOLDOWN = int(os.getenv("TWITCH_COOLDOWN", 6))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
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
    def __init__(self, id=None, title=None, json_data=None):
        self.id = id
        self.title = title
        if json_data:
            self.id = json_data["videoId"]
            try:
                self.title = json_data["title"]["runs"][0]["text"]
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
        self.db_entry = None
        self.live = None
        self.video = None

        self.logger.info(f'Twitch channel: {TWITCH_CHANNEL}')
        self.logger.info(f'Youtube channel ID: {YOUTUBE_CHANNEL_ID}')
        self.logger.info(f'Loop interval (secs): {TIME_INTERVAL_SECONDS}')
        self.logger.info(f'Twitch cooldown (hours): {TWITCH_COOLDOWN}')
        self.logger.info('Discord Token: {}****'.format(DISCORD_TOKEN[:4]))
        self.logger.info('General Discord channel: {}****'.format(str(GENERAL_CHANNEL_ID)[:4]))
        self.logger.info('Private Discord channel: {}****'.format(str(PRIVATE_CHANNEL_ID)[:4]))
        self.logger.info('Github Token: {}****'.format(str(GITHUB_TOKEN)[:4]))

        self.logger.info("Reading DB ...")
        self._read_db()
        self.logger.debug(f"Latest video: {self.video}")
        self.logger.debug(f"Latest Live: {self.live}")

        self.background_task.start()

    async def on_ready(self):
        self.logger.info(f'We have logged in as {self.user}')
        self.channel = self.get_channel(GENERAL_CHANNEL_ID)
        self.private_channel = self.get_channel(PRIVATE_CHANNEL_ID)
        self.logger.info(f"Acabo de ser inicializado como usuário o `{self.user}`.")
        await self._login_twitch()

    @tasks.loop(seconds=TIME_INTERVAL_SECONDS)
    async def background_task(self):
        self.logger.debug("Pooling task")
        await self.publish_new_video()
        await self.publish_live()

    @background_task.before_loop
    async def before_task(self):
        self.logger.debug("Running before_task")
        await self.wait_until_ready()

    async def _get_newest_video(self):
        videos = scrapetube.get_channel(channel_id=YOUTUBE_CHANNEL_ID, limit=1, sort_by="newest")
        if not videos:
            message = f"Could not scrap YT channel {YOUTUBE_CHANNEL_ID}!"
            self.logger.error(message)
            await self.private_channel.send(message)
            return None

        video = next(videos)
        self.logger.debug("Latest video on YT: {}".format(video["videoId"]))
        return video

    async def _is_alive(self):
        if not await self._is_logged_in():
            self.logger.debug("Twitch login expired")
            await self._login_twitch()
        params = {'Client-ID' : TWITCH_CLIENT_ID, 'Authorization':  "Bearer " + self.access_token}
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

    async def _write_db(self):
        sha = self._get_db_sha()
        dbentry = DBEntry(self.video, self.live)
        self.logger.debug(f"Write DB Entry: {dbentry}")
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        params = {"content": dbentry.b64encode(), "message": "Update DB", "branch": "database", "sha": sha}
        response = requests.put(url="https://api.github.com/repos/uilianries/tomahawk-bot/contents/db%2Ejson",
                                headers=headers, params=params)
        if not response.ok:
            message = f"Could not update DB: {response.text}"
            await self.private_channel.send(message)
            self.logger.error(message)

    def _read_db(self):
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        params = {"ref": "database"}
        response = requests.get(url="https://api.github.com/repos/uilianries/tomahawk-bot/contents/database%2Ejson",
                                headers=headers, params=params)
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
        response = requests.get(url="https://api.github.com/repos/uilianries/tomahawk-bot/contents/database%2Ejson",
                                headers=headers, params=params)
        data = response.json()
        return data["sha"]

    async def publish_new_video(self):
        self.logger.debug("On publish_new_video")
        current_video = Video(json_data=await self._get_newest_video())
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


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise ValueError("DISCORD_TOKEN is missing")
    if not GENERAL_CHANNEL_ID:
        raise ValueError("GENERAL_CHANNEL_ID is missing")
    if not PRIVATE_CHANNEL_ID:
        raise ValueError("PRIVATE_CHANNEL_ID is missing")
    if not TWITCH_CLIENT_ID:
        raise ValueError("TWITCH_CLIENT_ID is missing")
    if not TWITCH_CLIENT_SECRET:
        raise ValueError("TWITCH_CLIENT_SECRET is missing")
    if not GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN is missing")
    client = Sharkatzor()
    client.run(DISCORD_TOKEN)
