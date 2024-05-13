"""Support for Zabbix."""
import asyncio
from contextlib import suppress
import json
import logging
import math
import queue
import socket
import threading
import time
from typing import Any, cast
from urllib.parse import urljoin, urlparse

import voluptuous as vol
from zabbix_utils import (
    APIRequestError,
    AsyncSender,
    ItemValue,
    ProcessingError,
    Sender,
    ZabbixAPI,
)

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PATH,
    CONF_SSL,
    CONF_TOKEN,
    CONF_URL,
    CONF_USERNAME,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_STATE_CHANGED,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import event as event_helper, state as state_helper
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entityfilter import (
    CONF_EXCLUDE_DOMAINS,
    CONF_EXCLUDE_ENTITIES,
    CONF_EXCLUDE_ENTITY_GLOBS,
    CONF_INCLUDE_DOMAINS,
    CONF_INCLUDE_ENTITIES,
    CONF_INCLUDE_ENTITY_GLOBS,
    INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA,
    EntityFilter,
    convert_filter,
    convert_include_exclude_filter,
)
from homeassistant.helpers.typing import ConfigType

from .const import (
    BATCH_BUFFER_SIZE,
    BATCH_TIMEOUT,
    CONF_PUBLISH_STATES_HOST,
    CONF_USE_API,
    CONF_USE_SENDER,
    CONF_USE_SENSORS,
    CONF_USE_TOKEN,
    DEFAULT_PATH,
    DEFAULT_SSL,
    DEFAULT_ZABBIX_SENDER_PORT,
    DOMAIN,
    ENTITIES_FILTER,
    ENTRY_ID,
    INCLUDE_EXCLUDE_FILTER,
    NEW_CONFIG,
    QUEUE_BACKLOG_SECONDS,
    RETRY_DELAY,
    RETRY_INTERVAL,
    RETRY_MESSAGE,
    ZABBIX_SENDER,
    ZABBIX_THREAD_INSTANCE,
    ZAPI,
)

_LOGGER = logging.getLogger(__name__)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA.extend(
            {
                vol.Required(CONF_HOST): cv.string,
                vol.Optional(CONF_PASSWORD): cv.string,
                vol.Optional(CONF_PATH, default=DEFAULT_PATH): cv.string,
                vol.Optional(CONF_SSL, default=DEFAULT_SSL): cv.boolean,
                vol.Optional(CONF_USERNAME): cv.string,
                vol.Optional(CONF_PUBLISH_STATES_HOST): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


def event_to_metrics(
    event, float_keys, string_keys, entities_filter, publish_states_host
):
    """Add an event to the outgoing Zabbix list."""
    metrics: list[ItemValue]
    entity_id: str
    state = event.data.get("new_state")
    if state is None or state.state in (STATE_UNKNOWN, "", STATE_UNAVAILABLE):
        return
    entity_id = state.entity_id
    if not entities_filter(entity_id):
        return
    floats = {}
    strings = {}
    try:
        _state_as_value = float(state.state)
        floats[entity_id] = _state_as_value
    except ValueError:
        try:
            _state_as_value = float(state_helper.state_as_number(state))
            floats[entity_id] = _state_as_value
        except ValueError:
            strings[entity_id] = state.state
    for key, value in state.attributes.items():
        # For each value we try to cast it as float
        # But if we cannot do it we store the value
        # as string
        attribute_id = f"{entity_id}/{key}"
        try:
            float_value = float(value)
        except (ValueError, TypeError):
            float_value = None
        if float_value is None or not math.isfinite(float_value):
            strings[attribute_id] = str(value)
        else:
            floats[attribute_id] = float_value

        metrics = []
        float_keys_count = len(float_keys)
        float_keys.update(floats)
        if len(float_keys) != float_keys_count:
            floats_discovery = [
                {"{#KEY}": str(float_key)[:230]} for float_key in float_keys
            ]
            metric = ItemValue(
                publish_states_host,
                "homeassistant.floats_discovery",
                json.dumps(floats_discovery),
            )
            metrics.append(metric)
        for key, value in floats.items():
            metric = ItemValue(
                publish_states_host, f"homeassistant.float[{str(key)[:230]}]", value
            )
            metrics.append(metric)

        string_keys.update(strings)
        return metrics


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Zabbix component from a ConfigEntry."""
    zabbix_sender: AsyncSender
    host: str
    filter_dict: dict[str, list[str]] = {}
    hass_data: dict
    instance: ZabbixThread

    hass.data.setdefault(DOMAIN, {})
    new_config: bool = hass.data[DOMAIN].get(NEW_CONFIG)
    # Already configured from configuration.yaml. Probably no need, but to be sure, to be sure.
    if not new_config:
        return True

    # Continue with the new config format
    hass_data = dict(entry.data)
    # Registers update listener to update config entry when options are updated.
    unsub_options_update_listener = entry.add_update_listener(options_update_listener)
    # Store a reference to the unsubscribe function to cleanup if an entry is unloaded.
    hass_data["unsub_options_update_listener"] = unsub_options_update_listener

    hass.data[DOMAIN][entry.entry_id] = hass_data
    hass.data[DOMAIN][entry.entry_id][ENTRY_ID] = entry.entry_id

    if hass_data.get(CONF_USE_SENDER, False):
        host = urlparse(hass_data[CONF_URL]).hostname
        port = urlparse(hass_data[CONF_URL]).port
        if not port:
            port = DEFAULT_ZABBIX_SENDER_PORT

        for filter_key in (
            CONF_INCLUDE_DOMAINS,
            CONF_INCLUDE_ENTITY_GLOBS,
            CONF_INCLUDE_ENTITIES,
            CONF_EXCLUDE_DOMAINS,
            CONF_EXCLUDE_ENTITY_GLOBS,
            CONF_EXCLUDE_ENTITIES,
        ):
            if hass_data[INCLUDE_EXCLUDE_FILTER].get(filter_key) is not None:
                filter_dict[filter_key] = hass_data[INCLUDE_EXCLUDE_FILTER].get(
                    filter_key
                )
            else:
                filter_dict[filter_key] = []
        entities_filter = convert_filter(filter_dict)

        zabbix_sender = await hass.async_add_executor_job(
            lambda: Sender(server=host, port=port)
        )

        hass.data[DOMAIN][entry.entry_id][ZABBIX_SENDER] = zabbix_sender
        hass.data[DOMAIN][entry.entry_id][ENTITIES_FILTER] = entities_filter
        instance = ZabbixThread(hass, entry.entry_id)
        hass.data[DOMAIN][entry.entry_id][ZABBIX_THREAD_INSTANCE] = instance
        await hass.async_add_executor_job(instance.setup, hass)
        _LOGGER.info(
            "Started Zabbix thread for sharing events to zabbix_sender for config entry %s:",
            entry.entry_id,
        )

    if hass_data.get(CONF_USE_API, False) and hass_data.get(CONF_USE_SENSORS, False):
        # define Zabbix API for sensors
        try:
            zapi = await hass.async_add_executor_job(
                lambda: ZabbixAPI(url=hass_data[CONF_URL])
            )
            if hass_data[CONF_USE_TOKEN]:
                await hass.async_add_executor_job(
                    lambda: zapi.login(
                        token=hass_data[CONF_TOKEN],
                    )
                )
            else:
                await hass.async_add_executor_job(
                    lambda: zapi.login(
                        user=hass_data[CONF_USERNAME],
                        password=hass_data[CONF_PASSWORD],
                    )
                )
            await hass.async_add_executor_job(zapi.check_auth)
            _LOGGER.info("Connected to Zabbix API Version %s", zapi.api_version())
        except APIRequestError as login_exception:
            _LOGGER.error("Unable to login to the Zabbix API: %s", login_exception)
            return False
        except ProcessingError as http_error:
            _LOGGER.error("HTTPError when connecting to Zabbix API: %s", http_error)
            zapi = None
            return False

        # Forward the setup to the sensor platform.
        hass.data[DOMAIN][entry.entry_id][ZAPI] = zapi
        hass.async_create_task(
            hass.config_entries.async_forward_entry_setup(entry, "sensor")
        )

    return True


async def options_update_listener(hass: HomeAssistant, config_entry: ConfigEntry):
    """Handle options update."""
    await hass.config_entries.async_reload(config_entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    instance: ZabbixThread
    zapi: ZabbixAPI

    unload_ok = all(
        await asyncio.gather(
            *[hass.config_entries.async_forward_entry_unload(entry, "sensor")]
        )
    )
    # Remove options_update_listener.
    hass.data[DOMAIN][entry.entry_id]["unsub_options_update_listener"]()

    # Remove Zabbix thread if was running
    if entry.entry_id in hass.data[DOMAIN]:
        instance = hass.data[DOMAIN][entry.entry_id][ZABBIX_THREAD_INSTANCE]
    else:
        instance = hass.data[DOMAIN][ZABBIX_THREAD_INSTANCE]
    if instance is not None:
        instance.thread_shutdown()

    # Logout from Zabbix is API with Username and password is used.
    if entry.entry_id in hass.data[DOMAIN]:
        if hass.data[DOMAIN][entry.entry_id].get(CONF_USE_TOKEN) is False:
            if zapi := hass.data[DOMAIN][entry.entry_id].get(ZAPI, None):
                await hass.async_add_executor_job(zapi.logout)
    elif hass.data[DOMAIN].get(CONF_USE_TOKEN) is False:
        if zapi := hass.data[DOMAIN].get(ZAPI, None):
            await hass.async_add_executor_job(zapi.logout)

    # Remove DOMAIN with config entry from hass.
    if unload_ok:
        if entry.entry_id in hass.data[DOMAIN]:
            hass.data[DOMAIN].pop(entry.entry_id)
        else:
            hass.data.pop(DOMAIN)

    return unload_ok


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the Zabbix component from yaml configuration for backward compatibility."""
    conf: Any | None
    protocol: str
    url: str
    username: Any | None
    password: Any | None
    publish_states_host: Any | None
    entities_filter: EntityFilter
    zapi: ZabbixAPI
    zabbix_sender: Sender
    instance: ZabbixThread

    hass.data.setdefault(DOMAIN, {})

    # if no zabbix section in configuration.yaml just continue to setup_entry (new configuration via flow)
    if (conf := config.get(DOMAIN)) is None:
        hass.data[DOMAIN][NEW_CONFIG] = True
        return True

    conf = config[DOMAIN]
    protocol = "https" if conf[CONF_SSL] else "http"
    url = urljoin(f"{protocol}://{conf[CONF_HOST]}", conf[CONF_PATH])
    username = conf.get(CONF_USERNAME)
    password = conf.get(CONF_PASSWORD)
    publish_states_host = conf.get(CONF_PUBLISH_STATES_HOST)

    entities_filter = convert_include_exclude_filter(conf)

    # If not zabbix sensors, then skip starting ZabbixAPI
    def zabbix_sensor_exists(platforms: list[dict]) -> bool:
        for element in platforms:
            if element.get("platform") == "zabbix":
                return True
        return False

    if config.get("sensor") is not None:
        if zabbix_sensor_exists(cast(list[dict], config.get("sensor"))):
            try:
                zapi = await hass.async_add_executor_job(lambda: ZabbixAPI(url=url))
                await hass.async_add_executor_job(
                    lambda: zapi.login(
                        user=username,
                        password=password,
                    )
                )
                await hass.async_add_executor_job(zapi.check_auth)
                _LOGGER.info("Connected to Zabbix API Version %s", zapi.api_version())
            except APIRequestError as login_exception:
                _LOGGER.error("Unable to login to the Zabbix API: %s", login_exception)
                return False
            except ProcessingError as http_error:
                _LOGGER.error("HTTPError when connecting to Zabbix API: %s", http_error)
                zapi = None
                _LOGGER.error(RETRY_MESSAGE, http_error)
                await event_helper.call_later(  # type: ignore[misc]
                    hass,
                    RETRY_INTERVAL,
                    lambda _: async_setup(hass, config),  # type: ignore[arg-type,return-value]
                )
                return True

    hass.data[DOMAIN] = conf
    hass.data[DOMAIN][ZAPI] = zapi
    hass.data[DOMAIN][ENTITIES_FILTER] = entities_filter
    hass.data[DOMAIN][NEW_CONFIG] = False

    if publish_states_host:
        zabbix_sender = await hass.async_add_executor_job(
            lambda: Sender(server=conf[CONF_HOST], port=DEFAULT_ZABBIX_SENDER_PORT)
        )
        hass.data[DOMAIN][ZABBIX_SENDER] = zabbix_sender
        instance = ZabbixThread(hass)
        hass.data[DOMAIN][ZABBIX_THREAD_INSTANCE] = instance
        await hass.async_add_executor_job(instance.setup, hass)
        _LOGGER.info("Started Zabbix thread for sharing events to zabbix_sender")

    return True


class ZabbixThread(threading.Thread):
    """A threaded event handler class."""

    MAX_TRIES = 3
    thread_count: int = 0

    def __init__(self, hass: HomeAssistant, entry_id: str | None = None) -> None:
        """Initialize the listener."""
        if entry_id is None:
            threading.Thread.__init__(self, name=f"Zabbix_{ZabbixThread.thread_count}")
            self.zabbix_sender = hass.data[DOMAIN]["zabbix_sender"]
            self.entities_filter = hass.data[DOMAIN]["entities_filter"]
            self.publish_states_host = hass.data[DOMAIN].get(CONF_PUBLISH_STATES_HOST)
        else:
            threading.Thread.__init__(
                self, name=f"Zabbix_{ZabbixThread.thread_count}_{entry_id}"
            )
            self.zabbix_sender = hass.data[DOMAIN][entry_id]["zabbix_sender"]
            self.entities_filter = hass.data[DOMAIN][entry_id]["entities_filter"]
            self.publish_states_host = hass.data[DOMAIN][entry_id].get(
                CONF_PUBLISH_STATES_HOST
            )
        ZabbixThread.thread_count += 1
        self.queue: queue.Queue = queue.Queue()
        self.write_errors = 0
        self.shutdown = False
        self.float_keys: set[float] = set()
        self.string_keys: set[str] = set()

    def setup(self, hass: HomeAssistant) -> None:
        """Set up the thread and start it."""
        hass.bus.listen(EVENT_STATE_CHANGED, self._event_listener)
        hass.bus.listen_once(EVENT_HOMEASSISTANT_STOP, self._shutdown)
        self.start()
        _LOGGER.debug("Started publishing state changes to Zabbix")

    def _shutdown(self, event) -> None:
        """Shut down the thread."""
        self.queue.put(None)
        self.join()

    def thread_shutdown(self) -> None:
        """Shut down the thread."""
        ZabbixThread.thread_count -= 1
        self._shutdown(None)

    @callback
    def _event_listener(self, event) -> None:
        """Listen for new messages on the bus and queue them for Zabbix."""
        item = (time.monotonic(), event)
        self.queue.put(item)

    def get_metrics(self):
        """Return a batch of events formatted for writing."""
        queue_seconds: int = QUEUE_BACKLOG_SECONDS + self.MAX_TRIES * RETRY_DELAY

        count: int = 0
        metrics: list[ItemValue] = []

        dropped: int = 0

        with suppress(queue.Empty):
            while len(metrics) < BATCH_BUFFER_SIZE and not self.shutdown:
                timeout = None if count == 0 else BATCH_TIMEOUT
                item = self.queue.get(timeout=timeout)
                count += 1

                if item is None:
                    self.shutdown = True
                else:
                    timestamp, event = item
                    age = time.monotonic() - timestamp

                    if age < queue_seconds:
                        event_metrics = event_to_metrics(
                            event,
                            self.float_keys,
                            self.string_keys,
                            self.entities_filter,
                            self.publish_states_host,
                        )
                        if event_metrics:
                            metrics += event_metrics
                    else:
                        dropped += 1

        if dropped:
            _LOGGER.warning("Catching up, dropped %d old events", dropped)

        return count, metrics

    def write_to_zabbix(self, metrics: list[ItemValue]):
        """Write preprocessed events to zabbix, with retry."""

        for retry in range(self.MAX_TRIES + 1):
            try:
                self.zabbix_sender.send(metrics)

                if self.write_errors:
                    _LOGGER.error("Resumed, lost %d events", self.write_errors)
                    self.write_errors = 0

                _LOGGER.debug("Wrote %d metrics", len(metrics))
                break
            except (
                json.decoder.JSONDecodeError,
                ProcessingError,
                TimeoutError,
                socket.timeout,
                OSError,
                ConnectionResetError,
            ) as err:
                if retry < self.MAX_TRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    if not self.write_errors:
                        _LOGGER.error("Write error: %s", err)
                    self.write_errors += len(metrics)

    def run(self):
        """Process incoming events."""
        while not self.shutdown:
            count, metrics = self.get_metrics()
            if metrics:
                self.write_to_zabbix(metrics)
            for _ in range(count):
                self.queue.task_done()
