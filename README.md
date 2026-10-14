# Home Assistant Activity to Slack

A Home Assistant app repository containing an add-on that mirrors Activity entries to Slack and listens for a theatre arming command.

Install this repository in the Home Assistant app store, then install **Activity to Slack**.

## Behavior

- Posts Home Assistant Activity entries to the configured Slack channel.
- Watches a separate Slack security channel through Socket Mode.
- A message beginning with `THEATRE armed` releases the configured door-magnet switch for two seconds, then restores it.
- Repeated qualifying messages are handled one at a time.

## Configuration

Configure the add-on in Home Assistant:

```yaml
slack_app_token: xapp-...
slack_bot_token: xoxb-...
slack_channel_id: C0123456789
security_channel_id: C0987654321
door_switch_entity_id: switch.theatre_door_magnet
initial_lookback_seconds: 10
include_area: true
```

`slack_channel_id` is the destination for Home Assistant activity. `security_channel_id` is the source channel for theatre arming messages. Use Slack channel IDs, not channel names.

## Slack Setup

Create a Slack app and install it into the workspace.

1. Enable Socket Mode.
2. Create an app-level token with `connections:write`; set it as `slack_app_token`.
3. Add the `message.channels` event subscription for a public security channel.
4. Give the bot the `chat:write` scope and use its `xoxb-` token as `slack_bot_token`.
5. Invite the bot to both configured channels.

For a private security channel, use `message.groups` instead of `message.channels`.

## Development

```sh
cd slack-activity
npm ci
npm run check
npm run build
```
