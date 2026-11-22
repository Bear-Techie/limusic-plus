# Broadcast Limusic to Discord

Use **Settings → Discord → Broadcast to Discord** on Windows. The bot sends
Limusic's audio to one regular voice channel. Listeners only need Discord. Keep
Limusic running and control playback from its normal player.

The bot broadcasts at 70% independently of the Limusic volume slider and
Windows mixer. Each listener can adjust the bot's volume in Discord. If you
hear the music twice, mute the bot for yourself in Discord or mute your local
Limusic audio.

After setup, open the **Discord** menu in the title bar and select **Send bot**
to start or stop a broadcast. Pick a saved bot and use its last channel by name,
or look up a different Discord voice channel.

When you start broadcasting outside a Listen Together room, Limusic Plus offers
to host one. **Reopen room** opens your most recently used hosted room with the
same invite. If you haven't hosted a saved room, **Host a room** opens a room
using your Listen Together name. You can also choose **Just broadcast**.

1. Select **Install Discord broadcasting** in the Limusic Plus Windows
   installer. You can rerun the installer later to add the feature. The installer
   includes Node.js and the bot dependencies. To remove them, rerun the installer
   and clear the checkbox.
2. Open **Settings → Discord → Set up my bot** and use the link to the
   [Discord Developer Portal](https://discord.com/developers/applications).
   Create an application, open its **Bot** page, and use **Reset Token** to
   generate the token.
3. Paste the token in Limusic and select **Check bot token**. To customize the
   bot, enter a name and select **Apply name, icon and tags**. This uses the
   Limusic icon for the bot avatar and application icon, and sets the application
   tags to **Limusic**, **Music**, and **Shared**.
4. Use **Invite bot to a server** and complete the invitation in Discord. You need permission
   to add apps to that server. The bot needs **View Channel**, **Connect**, and
   **Speak** in the channel you want to use.
5. Back in Limusic, select **Find voice channels**, pick a channel, and select
   **Save bot**. Select **Join and broadcast** when you’re ready to play.
   Select **Leave channel** to end the session.

Limusic remembers the channel for each saved bot and stores its token in Windows
Credential Manager. Use **Add another bot** to keep another bot in the list or
**Forget bot** to remove the selected local credential.

## Share a bot with room members

In a room hosted by Limusic Plus, the host can choose **Share Discord bot** next
to the member list, pick a saved bot, and send an offer to connected members.
Members using the updated Windows app can decline or choose **Save shared bot**.
An accepted bot appears in their **Saved bots** list in **Settings → Discord**.
They can broadcast with it from their own PC after the room ends.

The offer shows the bot's name before consent. Accepting sends a copy of its token
directly over the encrypted peer connection and saves it in Windows Credential
Manager. Share only with people you trust. To revoke a copy you already shared,
reset the token in the Discord Developer Portal.

You only need to invite the shared bot to a Discord server once. Take turns using
it in that server. Limusic checks whether the bot is already in a voice channel
before joining. This check does not reserve the bot across PCs.

For the bot's avatar, upload `limusic-icon.png` from your Limusic Plus
installation folder. The default path is
`%LOCALAPPDATA%\Programs\Limusic Plus\limusic-icon.png`.

For a first check, start with Limusic paused, then play, pause, seek, and skip
while listening from another Discord client. Play audio in another app to
verify that it stays out of the channel. Confirm that the bot leaves when you
select **Leave channel** or quit Limusic.
Set Limusic's volume to zero and mute it in the Windows mixer while a track
plays. The bot should continue broadcasting at the same level.

For a source checkout in a debug build, install Node.js 22.12 or later and run
`npm.cmd ci` in the repository's `discord-voice` folder. Limusic finds the
dependencies there automatically.
