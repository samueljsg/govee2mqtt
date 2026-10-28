use crate::hass_mqtt::base::{Device, EntityConfig, Origin};
use crate::hass_mqtt::instance::{publish_entity_config, EntityInstance};
use crate::hass_mqtt::work_mode::ParsedWorkMode;
use crate::platform_api::{DeviceCapability, DeviceParameters};
use crate::service::device::Device as ServiceDevice;
use crate::service::hass::{
    availability_topic, camel_case_to_space_separated, topic_safe_id, HassClient, IdParameter,
};
use crate::service::state::StateHandle;
use anyhow::Context;
use mosquitto_rs::router::{Params, Payload, State};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Clone, Debug)]
pub struct SelectConfig {
    #[serde(flatten)]
    pub base: EntityConfig,

    pub command_topic: String,
    pub options: Vec<String>,
    pub state_topic: String,
}

impl SelectConfig {
    pub async fn publish(&self, state: &StateHandle, client: &HassClient) -> anyhow::Result<()> {
        publish_entity_config("select", state, client, &self.base, self).await
    }
}

pub struct WorkModeSelect {
    select: SelectConfig,
    device_id: String,
    state: StateHandle,
}

impl WorkModeSelect {
    pub fn new(device: &ServiceDevice, work_modes: &ParsedWorkMode, state: &StateHandle) -> Self {
        let command_topic = format!("gv2mqtt/{id}/set-work-mode", id = topic_safe_id(device),);
        let state_topic = format!("gv2mqtt/{id}/notify-work-mode", id = topic_safe_id(device));
        let availability_topic = availability_topic();
        let unique_id = format!("gv2mqtt-{id}-workMode", id = topic_safe_id(device),);

        Self {
            select: SelectConfig {
                base: EntityConfig {
                    availability_topic,
                    name: Some("Mode".to_string()),
                    device_class: None,
                    origin: Origin::default(),
                    device: Device::for_device(device),
                    unique_id,
                    entity_category: None,
                    icon: None,
                },
                command_topic,
                state_topic,
                options: work_modes.get_mode_names(),
            },
            device_id: device.id.to_string(),
            state: state.clone(),
        }
    }
}

#[async_trait::async_trait]
impl EntityInstance for WorkModeSelect {
    async fn publish_config(&self, state: &StateHandle, client: &HassClient) -> anyhow::Result<()> {
        self.select.publish(state, client).await
    }

    async fn notify_state(&self, client: &HassClient) -> anyhow::Result<()> {
        let device = self
            .state
            .device_by_id(&self.device_id)
            .await
            .expect("device to exist");

        if let Some(mode_value) = device.humidifier_work_mode {
            if let Ok(work_mode) = ParsedWorkMode::with_device(&device) {
                let mode_value_json = json!(mode_value);
                if let Some(mode) = work_mode.mode_for_value(&mode_value_json) {
                    client
                        .publish(&self.select.state_topic, mode.name.to_string())
                        .await?;
                }
            }
        } else {
            let work_modes = ParsedWorkMode::with_device(&device)?;

            if let Some(cap) = device.get_state_capability_by_instance("workMode") {
                if let Some(mode_num) = cap.state.pointer("/value/workMode") {
                    if let Some(mode) = work_modes.mode_for_value(mode_num) {
                        return client
                            .publish(&self.select.state_topic, mode.name.to_string())
                            .await;
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct SceneModeSelect {
    select: SelectConfig,
    device_id: String,
    state: StateHandle,
}

impl SceneModeSelect {
    pub async fn new(device: &ServiceDevice, state: &StateHandle) -> anyhow::Result<Option<Self>> {
        let scenes = state.device_list_scenes(device).await?;
        if scenes.is_empty() {
            return Ok(None);
        }

        let command_topic = format!("gv2mqtt/{id}/set-mode-scene", id = topic_safe_id(device));
        let state_topic = format!("gv2mqtt/{id}/notify-mode-scene", id = topic_safe_id(device));
        let availability_topic = availability_topic();
        let unique_id = format!("gv2mqtt-{id}-mode-scene", id = topic_safe_id(device));

        Ok(Some(Self {
            select: SelectConfig {
                base: EntityConfig {
                    availability_topic,
                    name: Some("Mode/Scene".to_string()),
                    device_class: None,
                    origin: Origin::default(),
                    device: Device::for_device(device),
                    unique_id,
                    entity_category: None,
                    icon: None,
                },
                command_topic,
                state_topic,
                options: scenes,
            },
            device_id: device.id.to_string(),
            state: state.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl EntityInstance for SceneModeSelect {
    async fn publish_config(&self, state: &StateHandle, client: &HassClient) -> anyhow::Result<()> {
        self.select.publish(state, client).await
    }

    async fn notify_state(&self, client: &HassClient) -> anyhow::Result<()> {
        let device = self
            .state
            .device_by_id(&self.device_id)
            .await
            .expect("device to exist");

        if let Some(device_state) = device.device_state() {
            client
                .publish(
                    &self.select.state_topic,
                    device_state.scene.as_deref().unwrap_or(""),
                )
                .await?;
        }

        Ok(())
    }
}

pub async fn mqtt_set_mode_scene(
    Payload(scene): Payload<String>,
    Params(IdParameter { id }): Params<IdParameter>,
    State(state): State<StateHandle>,
) -> anyhow::Result<()> {
    let device = state.resolve_device_for_control(&id).await?;

    state
        .device_set_scene(&device, &scene)
        .await
        .context("mqtt_set_mode_scene: state.device_set_scene")?;

    Ok(())
}

pub struct ModeSelect {
    select: SelectConfig,
    device_id: String,
    state: StateHandle,
    instance_name: String,
}

impl ModeSelect {
    pub async fn new(
        device: &ServiceDevice,
        state: &StateHandle,
        cap: &DeviceCapability,
    ) -> anyhow::Result<Option<Self>> {
        let options = match &cap.parameters {
            Some(DeviceParameters::Enum { options }) => {
                options.iter().map(|o| o.name.clone()).collect::<Vec<_>>()
            }
            _ => return Ok(None),
        };

        if options.is_empty() {
            return Ok(None);
        }

        let command_topic = format!(
            "gv2mqtt/select/{id}/set-mode/{inst}",
            id = topic_safe_id(device),
            inst = cap.instance
        );
        let state_topic = format!(
            "gv2mqtt/select/{id}/notify-mode/{inst}",
            id = topic_safe_id(device),
            inst = cap.instance
        );
        let availability_topic = availability_topic();
        let unique_id = format!(
            "gv2mqtt-{id}-{inst}",
            id = topic_safe_id(device),
            inst = cap.instance
        );

        Ok(Some(Self {
            select: SelectConfig {
                base: EntityConfig {
                    availability_topic,
                    name: Some(camel_case_to_space_separated(&cap.instance)),
                    device_class: None,
                    origin: Origin::default(),
                    device: Device::for_device(device),
                    unique_id,
                    entity_category: None,
                    icon: None,
                },
                command_topic,
                state_topic,
                options,
            },
            device_id: device.id.to_string(),
            state: state.clone(),
            instance_name: cap.instance.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl EntityInstance for ModeSelect {
    async fn publish_config(&self, state: &StateHandle, client: &HassClient) -> anyhow::Result<()> {
        self.select.publish(state, client).await
    }

    async fn notify_state(&self, client: &HassClient) -> anyhow::Result<()> {
        let device = self
            .state
            .device_by_id(&self.device_id)
            .await
            .expect("device to exist");

        if let Some(cap_state) = device.get_state_capability_by_instance(&self.instance_name) {
            let cap_def = device.get_capability_by_instance(&self.instance_name);
            let state_val = cap_state.state.pointer("/value");

            if let Some(DeviceParameters::Enum { options }) =
                cap_def.and_then(|c| c.parameters.as_ref())
            {
                for opt in options {
                    if state_val == Some(&opt.value) {
                        return client
                            .publish(&self.select.state_topic, opt.name.to_string())
                            .await;
                    }
                    if let Some(val_str) = state_val.and_then(|v| v.as_str()) {
                        if val_str.eq_ignore_ascii_case(&opt.name) {
                            return client
                                .publish(&self.select.state_topic, opt.name.to_string())
                                .await;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Deserialize)]
pub struct IdAndInstParameter {
    pub id: String,
    pub instance: String,
}

pub async fn mqtt_select_mode_command(
    Payload(selected_option): Payload<String>,
    Params(IdAndInstParameter { id, instance }): Params<IdAndInstParameter>,
    State(state): State<StateHandle>,
) -> anyhow::Result<()> {
    log::info!("Set mode for {id} {instance}: {selected_option}");
    let device = state.resolve_device_for_control(&id).await?;
    state
        .device_set_mode(&device, &instance, &selected_option)
        .await
        .context("mqtt_select_mode_command: state.device_set_mode")?;
    Ok(())
}
