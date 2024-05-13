"""Config flow for zabbix integration."""
from __future__ import annotations

from collections.abc import Sequence
import logging
from typing import Any, Optional, cast

import voluptuous as vol
from zabbix_utils import APIRequestError, ProcessingError, ZabbixAPI

from homeassistant import config_entries
from homeassistant.const import (
    CONF_PASSWORD,
    CONF_SENSORS,
    CONF_TOKEN,
    CONF_URL,
    CONF_USERNAME,
)
from homeassistant.core import State
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entityfilter import (
    CONF_EXCLUDE_DOMAINS,
    CONF_EXCLUDE_ENTITIES,
    CONF_EXCLUDE_ENTITY_GLOBS,
    CONF_INCLUDE_DOMAINS,
    CONF_INCLUDE_ENTITIES,
    CONF_INCLUDE_ENTITY_GLOBS,
)
from homeassistant.helpers.selector import (
    BooleanSelector,
    BooleanSelectorConfig,
    EntitySelector,
    EntitySelectorConfig,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
    ALL_ZABBIX_HOSTS,
    CONF_ADD_ANOTHER_SENSOR,
    CONF_PUBLISH_STATES_HOST,
    CONF_PUBLISH_STATES_HOSTID,
    CONF_SENSOR_TRIGGERS,
    CONF_SENSOR_TRIGGERS_HOSTIDS,
    CONF_SENSOR_TRIGGERS_INDIVIDUAL,
    CONF_SENSOR_TRIGGERS_NAME,
    CONF_SKIP_CREATION_PUBLISH_STATES_HOST,
    CONF_USE_API,
    CONF_USE_SENDER,
    CONF_USE_SENSORS,
    CONF_USE_TOKEN,
    DEFAULT_PUBLISH_STATES_HOST,
    DEFAULT_ZABBIX_HOSTGROUP_NAME,
    DEFAULT_ZABBIX_TEMPLATE_NAME,
    DOMAIN,
    INCLUDE_EXCLUDE_FILTER,
)

_LOGGER = logging.getLogger(__name__)

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_URL): TextSelector(
            TextSelectorConfig(type=TextSelectorType.URL)
        ),
        vol.Required(CONF_USE_API, default=True): BooleanSelector(
            BooleanSelectorConfig()
        ),
        vol.Required(CONF_USE_SENDER, default=True): BooleanSelector(
            BooleanSelectorConfig()
        ),
    }
)

STEP_TOKEN_OR_USERPASS_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USE_TOKEN): BooleanSelector(BooleanSelectorConfig()),
    }
)

STEP_TOKEN_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_TOKEN): TextSelector(
            TextSelectorConfig(type=TextSelectorType.TEXT)
        ),
    }
)

STEP_USERPASS_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): TextSelector(
            TextSelectorConfig(type=TextSelectorType.TEXT)
        ),
        vol.Required(CONF_PASSWORD): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
)

STEP_PUBLISH_STATES_HOST_NO_API = vol.Schema(
    {
        vol.Required(
            CONF_PUBLISH_STATES_HOST, default=DEFAULT_PUBLISH_STATES_HOST
        ): TextSelector(TextSelectorConfig(type=TextSelectorType.TEXT))
    }
)


async def create_template(self) -> str:
    """Try to create in Zabbix a hostname to be used to receive entities data with the linked template."""

    result: Optional[Any]
    templateid: str
    ruleid: str

    # Check if template already exists
    try:
        result = await self.hass.async_add_executor_job(
            lambda: self.zabbix_api.template.get(
                output=["templateid", "name"],
                filter={"host": DEFAULT_ZABBIX_TEMPLATE_NAME},
            )
        )
        templateid = result[0]["templateid"]
    except (IndexError, KeyError):
        # Create template as not existing
        result = await self.hass.async_add_executor_job(
            lambda: self.zabbix_api.template.create(
                host=DEFAULT_ZABBIX_TEMPLATE_NAME,
                name=DEFAULT_ZABBIX_TEMPLATE_NAME,
                groups={"groupid": 1},
            ),
        )
        templateid = result["templateids"][0]
        result = await self.hass.async_add_executor_job(
            lambda: self.zabbix_api.discoveryrule.create(
                name="Floats Discovery",
                key="homeassistant.floats_discovery",
                hostid=templateid,
                type=2,
                delay=0,
            ),
        )
        ruleid = result["itemids"][0]
        result = await self.hass.async_add_executor_job(
            lambda: self.zabbix_api.itemprototype.create(
                ruleid=ruleid,
                hostid=templateid,
                name="{#KEY}",
                key="homeassistant.float[{#KEY}]",
                type=2,
                value_type=0,
                delay=0,
                history="1095d",
                trends=0,
                preprocessing=[{"type": 20, "params": "14400"}],
            ),
        )

    return templateid


async def create_hostname(self, user_input: dict[str, Any], templateid: str) -> str:
    """Try to create in Zabbix a hostname to be used to receive entities data with the linked template."""

    result: Optional[Any]
    groupid: str

    # Check if template already exists
    try:
        result = await self.hass.async_add_executor_job(
            lambda: self.zabbix_api.hostgroup.get(
                output=["groupid", "name"],
                filter={"name": DEFAULT_ZABBIX_HOSTGROUP_NAME},
            )
        )
        groupid = result[0]["groupid"]
    except (IndexError, KeyError):
        # No such SHost group - create.
        result = await self.hass.async_add_executor_job(
            lambda: self.zabbix_api.hostgroup.create(
                name=DEFAULT_ZABBIX_HOSTGROUP_NAME
            ),
        )
        groupid = result["groupids"][0]

    result = await self.hass.async_add_executor_job(
        lambda: self.zabbix_api.host.create(
            host=user_input[CONF_PUBLISH_STATES_HOST],
            name=user_input[CONF_PUBLISH_STATES_HOST],
            groups={"groupid": groupid},
            templates={"templateid": templateid},
        ),
    )
    return result["hostids"][0]


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for zabbix."""

    VERSION = 1
    data: Optional[dict[str, Any]]
    zabbix_api: Any

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step and ask for the URL to connect. Same host is used for zabbxi sender."""
        errors: dict[str, str] = {}
        description_placeholders: dict[str, str] = {}

        if user_input is not None:
            if user_input.get(CONF_USE_API):
                try:
                    self.zabbix_api = await self.hass.async_add_executor_job(
                        lambda: ZabbixAPI(url=user_input.get(CONF_URL))
                    )

                except ProcessingError as e:
                    errors["base"] = "cannot_connect"
                    description_placeholders["error"] = str(e.args)
                else:
                    self.data = user_input
                    if self.zabbix_api.version >= 5.4:
                        return await self.async_step_token_or_userpass()
                    return await self.async_step_userpass()
            elif user_input.get(CONF_USE_SENDER):
                self.data = user_input
                return await self.async_step_publish_states_host_no_api()
            else:
                errors["base"] = "api_or_sender"

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_SCHEMA,
            errors=errors,
            description_placeholders=description_placeholders,
        )

    async def async_step_token_or_userpass(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask if token or user/pass to be used (step not shown if zabbix version is below 5.4)."""
        errors: dict[str, str] = {}

        assert isinstance(self.data, dict)
        if user_input is not None:
            self.data.update({CONF_USE_TOKEN: user_input.get(CONF_USE_TOKEN)})
            if user_input.get(CONF_USE_TOKEN):
                return await self.async_step_token()

            return await self.async_step_userpass()

        return self.async_show_form(
            step_id="token_or_userpass",
            data_schema=STEP_TOKEN_OR_USERPASS_SCHEMA,
            errors=errors,
        )

    async def async_step_token(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask for token."""
        errors: dict[str, str] = {}
        description_placeholders: dict[str, str] = {}

        assert isinstance(self.data, dict)
        if user_input is not None:
            if len(user_input[CONF_TOKEN]) == 64:
                try:
                    await self.hass.async_add_executor_job(
                        lambda: self.zabbix_api.login(token=user_input.get(CONF_TOKEN))
                    )
                    await self.hass.async_add_executor_job(self.zabbix_api.check_auth)
                except APIRequestError as e:
                    errors["base"] = "invalid_auth"
                    description_placeholders["error"] = str(e.args)
                else:
                    self.data.update({CONF_TOKEN: user_input.get(CONF_TOKEN)})
                    return await self.async_step_publish_states_host()
            else:
                errors[CONF_TOKEN] = "token_length_invalid"

        return self.async_show_form(
            step_id="token",
            data_schema=STEP_TOKEN_SCHEMA,
            errors=errors,
            description_placeholders=description_placeholders,
        )

    async def async_step_userpass(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask for username and password."""
        errors: dict[str, str] = {}
        description_placeholders: dict[str, str] = {}

        assert isinstance(self.data, dict)
        if user_input is not None:
            try:
                await self.hass.async_add_executor_job(
                    lambda: self.zabbix_api.login(
                        user=user_input.get(CONF_USERNAME),
                        password=user_input.get(CONF_PASSWORD),
                    )
                )
                await self.hass.async_add_executor_job(self.zabbix_api.check_auth)
            except APIRequestError as e:
                errors["base"] = "invalid_auth"
                description_placeholders["error"] = str(e.args)
            else:
                self.data.update(
                    {
                        CONF_USERNAME: user_input.get(CONF_USERNAME),
                        CONF_PASSWORD: user_input.get(CONF_PASSWORD),
                    }
                )
                return await self.async_step_publish_states_host()

        return self.async_show_form(
            step_id="userpass",
            data_schema=STEP_USERPASS_SCHEMA,
            errors=errors,
            description_placeholders=description_placeholders,
        )

    async def async_step_publish_states_host(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask for hostname configured in Zabbix to send Home Assistant entities states."""
        errors: dict[str, str] = {}
        result: Optional[Any]
        def_publish_host: str
        templateid: str
        skip_creation: bool = False
        description_placeholders: dict[str, str] = {"message": ""}
        def_publish_host = DEFAULT_PUBLISH_STATES_HOST

        assert isinstance(self.data, dict)
        if not self.data.get(CONF_USE_SENDER):
            return await self.async_step_sensor_filter()
        if user_input is not None:
            if not user_input[CONF_SKIP_CREATION_PUBLISH_STATES_HOST]:
                # Check if provided publish_states_host is already existing in Zabbix
                try:
                    result = await self.hass.async_add_executor_job(
                        lambda: self.zabbix_api.host.get(
                            output=["hostid", "host"],
                            filter={"host": user_input.get(CONF_PUBLISH_STATES_HOST)},
                        )
                    )
                    assert isinstance(result, list)
                    def_publish_host = result[0].get("host")
                except (APIRequestError, ProcessingError) as e:
                    # No zabbix permissions to read data
                    errors["base"] = "no_permission_read"
                    description_placeholders["error"] = str(e.args)
                except (IndexError, KeyError):
                    # No hostname exists in Zabbix
                    try:
                        # Create or usr existing one
                        templateid = await create_template(self)
                    except (
                        APIRequestError,
                        ProcessingError,
                        IndexError,
                        KeyError,
                    ) as e:
                        # No zabbix permissions to create template and host
                        errors["base"] = "no_permission_write"
                        description_placeholders["error"] = str(e.args)
                    else:
                        # Try to create hostname with the linked template
                        try:
                            self.data.update(
                                {
                                    CONF_PUBLISH_STATES_HOSTID: await create_hostname(
                                        self, user_input, templateid
                                    )
                                }
                            )
                        except (APIRequestError, ProcessingError) as e:
                            # No zabbix permissions to create template and host
                            errors["base"] = "no_permission_write"
                            description_placeholders["error"] = str(e.args)

            if not errors:
                # publish_states_host is existing in Zabbix so continue
                self.data.update(
                    {CONF_PUBLISH_STATES_HOST: user_input.get(CONF_PUBLISH_STATES_HOST)}
                )
                return await self.async_step_include_exclude_filter()

        # Try to get the hostname from Zabbix server with the linked template 'Template Home Assistant'.
        # Assume it is created manually in zabbix by user, but get only the first host if multiple are configured.
        # Suggest it as a "default"
        # If any error - just ignore and suggest to use default name
        try:
            result = await self.hass.async_add_executor_job(
                lambda: self.zabbix_api.template.get(
                    output=["templateid", "name"],
                    filter={"host": DEFAULT_ZABBIX_TEMPLATE_NAME},
                    selectHosts=["hostid", "host", "name"],
                )
            )
            assert isinstance(result, list)
            def_publish_host = result[0].get("hosts")[0].get("host")
            skip_creation = True
        except (IndexError, KeyError, APIRequestError, ProcessingError):
            pass

        step_publish_states_host_schema = vol.Schema(
            {
                vol.Required(
                    CONF_PUBLISH_STATES_HOST, default=def_publish_host
                ): TextSelector(TextSelectorConfig(type=TextSelectorType.TEXT)),
                vol.Required(
                    CONF_SKIP_CREATION_PUBLISH_STATES_HOST, default=skip_creation
                ): BooleanSelector(BooleanSelectorConfig()),
            }
        )

        return self.async_show_form(
            step_id="publish_states_host",
            data_schema=step_publish_states_host_schema,
            errors=errors,
            description_placeholders=description_placeholders,
        )

    async def async_step_publish_states_host_no_api(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask for hostname configured in Zabbix to send Home Assistant entities states. No checks as no API."""
        errors: dict[str, str] = {}

        assert isinstance(self.data, dict)
        if user_input is not None:
            self.data.update(
                {CONF_PUBLISH_STATES_HOST: user_input.get(CONF_PUBLISH_STATES_HOST)}
            )
            return await self.async_step_include_exclude_filter()

        return self.async_show_form(
            step_id="publish_states_host_no_api",
            data_schema=STEP_PUBLISH_STATES_HOST_NO_API,
            errors=errors,
        )

    async def async_step_include_exclude_filter(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask for any include or exclude filters to be used when sending entities update to Zabbix."""
        errors: dict[str, str] = {}
        domains: list[str] = []
        state: State

        assert isinstance(self.data, dict)
        if user_input is not None:
            self.data.update({INCLUDE_EXCLUDE_FILTER: user_input})
            if self.data.get(CONF_USE_API):
                return await self.async_step_sensor_filter()

            # create a config entry
            return self.async_create_entry(title="Zabbix integration", data=self.data)

        for state in self.hass.states.async_all():
            if state.domain not in domains:
                domains.append(state.domain)

        include_exclude_schema = vol.Schema(
            {
                vol.Optional(CONF_INCLUDE_ENTITIES): EntitySelector(
                    EntitySelectorConfig(multiple=True)
                ),
                vol.Optional(CONF_INCLUDE_ENTITY_GLOBS): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.TEXT)
                ),
                vol.Optional(CONF_INCLUDE_DOMAINS): SelectSelector(
                    SelectSelectorConfig(
                        options=domains,
                        multiple=True,
                        mode=SelectSelectorMode.DROPDOWN,
                        sort=True,
                    )
                ),
                vol.Optional(CONF_EXCLUDE_ENTITIES): EntitySelector(
                    EntitySelectorConfig(multiple=True)
                ),
                vol.Optional(CONF_EXCLUDE_ENTITY_GLOBS): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.TEXT)
                ),
                vol.Optional(CONF_EXCLUDE_DOMAINS): SelectSelector(
                    SelectSelectorConfig(
                        options=domains,
                        multiple=True,
                        mode=SelectSelectorMode.DROPDOWN,
                        sort=True,
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="include_exclude_filter",
            data_schema=include_exclude_schema,
            errors=errors,
        )

    async def async_step_sensor_filter(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask for sensor configuration parameters for Zabbix triggers."""
        errors: dict[str, str] = {}
        hosts: list[SelectOptionDict] = []
        result: Optional[Any] = None
        result_element: dict[str, str]
        description_placeholders: dict[str, str] = {}

        assert isinstance(self.data, dict)
        if user_input is not None:
            if self.data.get(CONF_USE_SENSORS) is None:
                self.data.update({CONF_USE_SENSORS: user_input.get(CONF_USE_SENSORS)})
            if self.data.get(CONF_USE_SENSORS):
                if self.data.get(CONF_SENSORS) is None:
                    self.data.update({CONF_SENSORS: []})
                individual: bool = cast(
                    bool, user_input.get(CONF_SENSOR_TRIGGERS_INDIVIDUAL)
                )
                hostids: list[str] = cast(
                    list[str], user_input.get(CONF_SENSOR_TRIGGERS_HOSTIDS)
                )
                if individual and not hostids:
                    errors[CONF_SENSOR_TRIGGERS_HOSTIDS] = "need_hostids"
                else:
                    old_sensors: list = self.data.get(CONF_SENSORS, [])
                    assert isinstance(old_sensors, list)
                    old_sensors.append(
                        {
                            CONF_SENSOR_TRIGGERS: {
                                CONF_SENSOR_TRIGGERS_NAME: user_input.get(
                                    CONF_SENSOR_TRIGGERS_NAME
                                ),
                                CONF_SENSOR_TRIGGERS_HOSTIDS: hostids,
                                CONF_SENSOR_TRIGGERS_INDIVIDUAL: individual,
                            }
                        }
                    )
                    self.data.update({CONF_SENSORS: old_sensors})
                    if user_input.get(CONF_ADD_ANOTHER_SENSOR):
                        return await self.async_step_sensor_filter()
            if not errors:
                # create a config entry
                self.data.pop(ALL_ZABBIX_HOSTS)
                return self.async_create_entry(
                    title="Zabbix integration", data=self.data
                )

        # Get list of hosts from Zabbix
        if not self.data.get(ALL_ZABBIX_HOSTS):
            try:
                result = await self.hass.async_add_executor_job(
                    lambda: self.zabbix_api.host.get(output=["hostid", "host"])
                )
                assert isinstance(result, list)
                for result_element in result:
                    hosts.append(
                        SelectOptionDict(
                            value=result_element["hostid"], label=result_element["host"]
                        )
                    )
                self.data[ALL_ZABBIX_HOSTS] = hosts
            except (APIRequestError, ProcessingError) as e:
                # No zabbix permissions to read data
                errors["base"] = "no_permission_read"
                description_placeholders["error"] = str(e.args)

        if self.data.get(CONF_USE_SENSORS, False):
            sensor_schema = vol.Schema(
                {
                    vol.Optional(CONF_SENSOR_TRIGGERS_NAME): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.TEXT)
                    ),
                    vol.Optional(CONF_SENSOR_TRIGGERS_HOSTIDS): SelectSelector(
                        SelectSelectorConfig(
                            options=cast(
                                Sequence[SelectOptionDict],
                                self.data.get(ALL_ZABBIX_HOSTS),
                            ),
                            multiple=True,
                            mode=SelectSelectorMode.DROPDOWN,
                            sort=True,
                        )
                    ),
                    vol.Required(
                        CONF_SENSOR_TRIGGERS_INDIVIDUAL, default=False
                    ): BooleanSelector(BooleanSelectorConfig()),
                    vol.Required(
                        CONF_ADD_ANOTHER_SENSOR, default=False
                    ): BooleanSelector(BooleanSelectorConfig()),
                }
            )
        else:
            sensor_schema = vol.Schema(
                {
                    vol.Required(CONF_USE_SENSORS, default=True): BooleanSelector(
                        BooleanSelectorConfig()
                    ),
                    vol.Optional(CONF_SENSOR_TRIGGERS_NAME): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.TEXT)
                    ),
                    vol.Optional(CONF_SENSOR_TRIGGERS_HOSTIDS): SelectSelector(
                        SelectSelectorConfig(
                            options=cast(
                                Sequence[SelectOptionDict],
                                self.data.get(ALL_ZABBIX_HOSTS),
                            ),
                            multiple=True,
                            mode=SelectSelectorMode.DROPDOWN,
                            sort=True,
                        )
                    ),
                    vol.Required(
                        CONF_SENSOR_TRIGGERS_INDIVIDUAL, default=False
                    ): BooleanSelector(BooleanSelectorConfig()),
                    vol.Required(
                        CONF_ADD_ANOTHER_SENSOR, default=False
                    ): BooleanSelector(BooleanSelectorConfig()),
                }
            )

        sensor_number: int = len(self.data.get(CONF_SENSORS, [])) + 1
        description_placeholders["number"] = str(sensor_number)
        return self.async_show_form(
            step_id="sensor_filter",
            data_schema=sensor_schema,
            errors=errors,
            description_placeholders=description_placeholders,
        )


class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""


class InvalidAuth(HomeAssistantError):
    """Error to indicate there is invalid auth."""
