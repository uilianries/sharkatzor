#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from discord.ext import tasks
import discord
import requests
import googleapiclient.discovery
from googleapiclient.errors import HttpError

import logging.handlers
import json
import os
import asyncio
import configparser
import peewee
from datetime import datetime, timedelta


SHARKTAZOR_CONF = os.getenv("SHARKATZOR_CONF", "/etc/sharkatzor.conf")
LOGGING_FILE = os.getenv("LOGGING_FILE", "/home/orangepi/.sharkatzor/sharkatzor.log")
DATABASE_PATH = os.getenv("DATABASE_PATH", "/home/orangepi/.sharkatzor/database.json")
SHARKTAZOR_DRY_RUN = os.getenv("SHARKTAZOR_DRY_RUN", None)
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", None)
GENERAL_CHANNEL_ID = int(os.getenv("GENERAL_CHANNEL_ID", 0))
PRIVATE_CHANNEL_ID = int(os.getenv("PRIVATE_CHANNEL_ID", 0))
SHARED_CHANNEL_ID = int(os.getenv("SHARED_CHANNEL_ID", 0))
TWITCH_CHANNEL = os.getenv("TWITCH_CHANNEL", "tomahawk_aoe")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", None)
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", None)
YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID", "UCJ0vp6VTn7JuFNEMj5YIRcQ")
TIME_INTERVAL_SECONDS = int(os.getenv("TIME_INTERVAL_SECONDS", 60))
TWITCH_COOLDOWN = int(os.getenv("TWITCH_COOLDOWN", 6))
DISCORD_COOLDOWN = int(os.getenv("DISCORD_COOLDOWN", 6))
DISCORD_ALLOWED_ROLES = [int(it) for it in os.getenv("DISCORD_ALLOWED_ROLES", "0").split(",")]
DISCORD_ALLOWED_USERS = [int(it) for it in os.getenv("DISCORD_ALLOWED_USERS", "0").split(",")]
GCP_API_KEYS = os.getenv("GCP_API_KEYS", "").split(",")
RETRY_MAX = 5
RETRY_TIME_INTERNAL = 10

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
S_HANDLER = logging.StreamHandler()
S_HANDLER.setLevel(logging.DEBUG)
S_HANDLER.setFormatter(formatter)
F_HANDLER = logging.handlers.RotatingFileHandler(LOGGING_FILE, maxBytes=5*1024*1024, backupCount=2)
F_HANDLER.setLevel(logging.INFO)
F_HANDLER.setFormatter(formatter)
LOGGER.addHandler(S_HANDLER)
LOGGER.addHandler(F_HANDLER)

DATABASE = peewee.SqliteDatabase(DATABASE_PATH)


class SharkatzorError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class Video(peewee.Model):
    id = peewee.TextField(primary_key=True)
    title = peewee.TextField()
    time = peewee.DateTimeField(default=datetime.now)

    class Meta:
        database = DATABASE
        db_table = 'Video'

    @property
    def link(self):
        return f"https://www.youtube.com/watch?v={self.id}"

    def is_stale(self):
        if self.time is None:
            return True
        now = datetime.now()
        diff = now - self.time
        diff_hours = diff.total_seconds() / 3600
        return diff_hours > DISCORD_COOLDOWN

    @staticmethod
    def generate(json_data):
        if json_data:
            video_id = "---"
            video_title = ""
            video_date = datetime.now()
            try:
                video_id = json_data["resourceId"]["videoId"]
            except Exception:
                pass
            try:
                video_title = json_data["title"]
            except Exception:
                pass
            try:
                video_date = datetime.strptime(json_data["publishedAt"], "YYYY-MM-DDThh:mm:ss")
            except Exception:
                pass
            return Video(id=str(video_id), title=str(video_title), time=video_date)

    @staticmethod
    def get_latest_video():
        try:
            return Video.select().order_by(Video.time.desc()).get()
        except Exception:
            return None

    @staticmethod
    def delete_old_entries():
        try:
            now = datetime.now()
            one_month_ago = now - timedelta(days=30)
            query = Video.delete().where(Video.time < one_month_ago)
            query.execute()
        except Exception as error:
            LOGGER.error(f"Could not recycle Video DB: {error}")

    def __str__(self):
        return f"{self.id}: {self.title}"


class Live(peewee.Model):
    title = peewee.TextField()
    time = peewee.DateTimeField(default=datetime.now)

    class Meta:
        database = DATABASE
        db_table = 'Live'

    @property
    def link(self):
        return f"https://www.twitch.tv/{TWITCH_CHANNEL}"

    def is_stale(self):
        now = datetime.now()
        diff = now - self.time
        diff_hours = diff.total_seconds() / 3600
        return diff_hours > TWITCH_COOLDOWN

    @staticmethod
    def get_latest_live():
        try:
            return Live.select().order_by(Live.time.desc()).get()
        except Exception:
            return None

    @staticmethod
    def delete_old_entries():
        try:
            now = datetime.now()
            one_month_ago = now - timedelta(days=30)
            query = Live.delete().where(Live.time < one_month_ago)
            query.execute()
        except Exception as error:
            LOGGER.error(f"Could not recycle Live DB: {error}")

    def __str__(self):
        return f"{self.time}: {self.title}"


Video.create_table()
Live.create_table()


class Youtube(object):

    def __init__(self, logger=LOGGER):
        self._logger = logger
        self._yt_api = None
        self._playlist = None

    async def login(self):
        self._logger.debug("Executing YT login")
        for key in GCP_API_KEYS:
            try:
                self._logger.info("Connecting to YT with key {}****".format(key[:8]))
                self._yt_api = googleapiclient.discovery.build("youtube", "v3", developerKey=key)
                request = self._yt_api.channels().list(part="id,contentDetails", id=YOUTUBE_CHANNEL_ID, maxResults=1)
                response = request.execute()
                if not response:
                    message = "Could not login on Youtube!"
                    self._logger.error(message)
                self._playlist = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
                self._logger.info(f"Logged-in on youtube, Playlist ID: {self._playlist}")
                return
            except HttpError as err:
                self._logger.error(err.reason)
                pass
        raise SharkatzorError("Could not login on YT! Giving up!")

    async def get_latest_video(self):
        try:
            if self._yt_api is None:
                await self.login()
            request = self._yt_api.playlistItems().list(part="id,snippet", playlistId=self._playlist, maxResults=1)
            response = request.execute()
            if not response:
                message = f"Could not scrap YT channel {YOUTUBE_CHANNEL_ID}!"
                self._logger.error(message)
                return None
            video_data = response["items"][0]["snippet"]
            self._logger.debug("Latest video on YT: {}".format(video_data["resourceId"]["videoId"]))
            return Video.generate(video_data)
        except HttpError as err:
            self._logger.error(err.reason)
        except SharkatzorError as err:
            self._logger.error(err.message)


class Twitch(object):

    def __init__(self, discord, logger=LOGGER):
        self._logger = logger
        self._discord = discord
        self._access_token = None

    async def is_alive(self):
        if not await self.is_logged_in():
            self._logger.debug("Twitch login expired")
            await self.login()
        params = {'Client-ID': TWITCH_CLIENT_ID, 'Authorization':  "Bearer " + self._access_token}
        response = requests.get(f'https://api.twitch.tv/helix/streams?user_login={TWITCH_CHANNEL}', headers=params)
        if not response.ok:
            message = f"Could not fetch Twitch channel {TWITCH_CHANNEL}"
            self._logger.error(message)
            await self._discord.post_on_private_channel(message)
            return False, None
        data = json.loads(response.text)['data']
        if not data:
            return False, None
        is_alive = data[0]['type'] == 'live'
        title = data[0]['title']
        self._logger.debug("Is alive on Twitch: {}".format("YES" if is_alive else "NO"))
        return is_alive, Live(title=title)

    async def login(self):
        self._logger.debug("Login on Twitch")
        params = {'client_id': TWITCH_CLIENT_ID, 'client_secret': TWITCH_CLIENT_SECRET, 'grant_type': 'client_credentials'}
        retry_count = 0
        while retry_count < RETRY_MAX:
            try:
                response = requests.post(url='https://id.twitch.tv/oauth2/token', data=params)
                if not response.ok:
                    message = f"Could not login on Twitch: {response}"
                    await self._discord.post_on_private_channel(message)
                    self._logger.error(message)
                    raise SharkatzorError(message)
                data = json.loads(response.text)
                if "access_token" not in data:
                    message = f"Could not find access_token: {response}"
                    await self._discord.post_on_private_channel(message)
                    self._logger.error(message)
                    raise SharkatzorError(message)
                self._access_token = data["access_token"]
                self._logger.debug("Logged in on Twitch: {}*****".format(self._access_token[:4]))
                return
            except SharkatzorError as error:
                retry_count += 1
                if retry_count > RETRY_MAX:
                    raise Exception(error)
                self._logger.debug(f"Retry new login Twitch in {RETRY_TIME_INTERNAL} secs.")
                await asyncio.sleep(RETRY_TIME_INTERNAL)

    async def is_logged_in(self):
        if self._access_token:
            params = {'Authorization':  "Bearer " + self._access_token, 'Client-ID': TWITCH_CLIENT_ID}
            response = requests.get(url='https://id.twitch.tv/oauth2/validate', headers=params)
            if response.ok:
                return True
            else:
                self._logger.info(f"Twitch login expired: {response.text}")
        return False


class Discord(object):

    def __init__(self, channel, shared_channel, private_channel, logger=LOGGER):
        self._general_channel = channel
        self._shared_channel = shared_channel
        self._private_channel = private_channel
        self._logger = logger

    async def publish_new_video(self, video):
        await self._general_channel.send(f"Vídeo novo do Tomahawk no Youtube @everyone!\n**{video.title}**\n{video.link}")

    async def publish_live(self, live):
        await self._general_channel.send(f"Tomahawk está ao vivo na Twitch @everyone!\n**{live.title}**\n{live.link}")

    async def delete_forbidden_message(self, message):
        if message.embeds:
            if message.author.id not in DISCORD_ALLOWED_USERS and not any(role.id in DISCORD_ALLOWED_ROLES for role in message.author.roles):
                if message.channel.id == GENERAL_CHANNEL_ID:
                    for embed in message.embeds:
                        if ("//www.twitch.tv/" in embed.url or "//twitch.tv/" in embed.url) and "twitch.tv/tomahawk_aoe" not in embed.url:
                            self._logger.warning(f"Delete message - #{message.author.name}: {message.content}")
                            await message.delete()
                            await self._general_channel.send(f"{message.author.mention} favor utilizar o canal {self._shared_channel.mention} para postar link da Twitch.")
                            return
            phishing_links = ["free distribution of discord nitro", "free nitro"]
            for link in phishing_links:
                if link in str(message.content).lower():
                    self._logger.warning(f"Delete message - #{message.author.name}: {message.content}")
                    await message.delete()
                    await message.author.kick("Usuário caiu no golpe do litrão e enviou phising no servidor.")
                    await self._private_channel.send(f"Usuário {message.author.mention} caiu no golpe do litrão. Mensagem removida e usuário kickado.")
                    break

    async def post_on_private_channel(self, message):
        self._private_channel.send(message)


class Sharkatzor(discord.Client):
    def __init__(self, *args, **kwargs):
        intents = discord.Intents.default()
        intents.messages = True
        intents.message_content = True
        super().__init__(intents=intents)

        self.logger = LOGGER
        self.logger.info('Starting ...')
        self.discord = None
        self.twitch = None
        self.youtube = None

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
        self.logger.info('Youtube keys: {}'.format(len(GCP_API_KEYS)))
        self.logger.info('Dry Run: {}'.format(True if SHARKTAZOR_DRY_RUN else False))

    async def setup_hook(self) -> None:
        self.logger.info("Reading DB ...")
        video = Video.get_latest_video()
        live = Live.get_latest_live()
        if video:
            self.logger.debug(f"Latest video: {video}")
            Video.delete_old_entries()
        if live:
            self.logger.debug(f"Latest Live: {live}")
            Live.delete_old_entries()
        self.background_task.start()

    async def on_ready(self):
        self.logger.info(f'We have logged in as {self.user}')
        general = self.get_channel(int(GENERAL_CHANNEL_ID))
        shared = self.get_channel(int(SHARED_CHANNEL_ID))
        private = self.get_channel(int(PRIVATE_CHANNEL_ID))
        self.discord = Discord(general, shared, private)
        self.logger.info(f"Started as `{self.user}`.")
        self.twitch = Twitch(discord)
        await self.twitch.login()
        self.youtube = Youtube()
        await self.youtube.login()

    @tasks.loop(seconds=TIME_INTERVAL_SECONDS)
    async def background_task(self):
        self.logger.debug("Pooling task")
        await self._process_new_youtube_video()
        await self._process_new_twitch_live()

    @background_task.before_loop
    async def before_task(self):
        self.logger.debug("Running before_task")
        await self.wait_until_ready()

    async def on_message(self, message):
        await self.discord.delete_forbidden_message(message)

    async def on_message_edit(self, _, message):
        await self.discord.delete_forbidden_message(message)

    async def _process_new_youtube_video(self):
        recorded_video = Video.get_latest_video()
        if recorded_video is None or (recorded_video is not None and recorded_video.is_stale()):
            posted_video = await self.youtube.get_latest_video()

            if posted_video is None:
                await self.discord.post_on_private_channel(f"Could not scrap YT channel {YOUTUBE_CHANNEL_ID}!")
                return

            now = datetime.now()
            if (recorded_video is None and
                posted_video.time.day == now.day and
                posted_video.time.month == now.month and
                posted_video.time.year == now.year) or \
               posted_video.time > recorded_video.time:
                result = posted_video.save(force_insert=True)
                if result and not SHARKTAZOR_DRY_RUN:
                    await self.discord.publish_new_video(posted_video)
                else:
                    self.logger.error(f"Could not add a new entry on Video table: {posted_video}")

    async def _process_new_twitch_live(self):
        recorded_live = Live.get_latest_live()
        if recorded_live is None or (recorded_live is not None and recorded_live.is_stale()):
            is_alive, live = await self.twitch.is_alive()
            if is_alive and (recorded_live is None or live.time > recorded_live.time):
                result = live.save(force_insert=True)
                if result and not SHARKTAZOR_DRY_RUN:
                    await self.discord.publish_live(live)
                else:
                    self.logger.error(f"Could not add a new entry on Twitch table: {live}")


def load_configuration():
    global DISCORD_ALLOWED_ROLES
    global DISCORD_ALLOWED_USERS
    global DISCORD_TOKEN
    global GCP_API_KEYS
    global GENERAL_CHANNEL_ID
    global PRIVATE_CHANNEL_ID
    global SHARED_CHANNEL_ID
    global TWITCH_CLIENT_ID
    global TWITCH_CLIENT_SECRET
    config = configparser.ConfigParser()
    config.read(SHARKTAZOR_CONF)
    DISCORD_ALLOWED_ROLES = config["conf"]["DISCORD_ALLOWED_ROLES"].split(",")
    DISCORD_ALLOWED_USERS = config["conf"]["DISCORD_ALLOWED_USERS"].split(",")
    DISCORD_TOKEN = config["conf"]["DISCORD_TOKEN"]
    GCP_API_KEYS = config["conf"]["GCP_API_KEYS"].split(",")
    GENERAL_CHANNEL_ID = int(config["conf"]["GENERAL_CHANNEL_ID"])
    PRIVATE_CHANNEL_ID = int(config["conf"]["PRIVATE_CHANNEL_ID"])
    SHARED_CHANNEL_ID = int(config["conf"]["SHARED_CHANNEL_ID"])
    TWITCH_CLIENT_ID = config["conf"]["TWITCH_CLIENT_ID"]
    TWITCH_CLIENT_SECRET = config["conf"]["TWITCH_CLIENT_SECRET"]


def main():
    load_configuration()
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
    if not GCP_API_KEYS:
        raise ValueError("GCP_API_KEYS is missing")
    client = Sharkatzor()
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
