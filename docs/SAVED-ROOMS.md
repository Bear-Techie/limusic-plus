# Saved friends and automatic per-PC hosting

The Windows Plus installer starts only Limusic. Choosing **Host** starts the
bundled `limusic-sync.exe` silently on a private loopback port. Limusic embeds
Iroh networking to make that local session reachable without terminal commands,
accounts, tunnels, port forwarding, or a fixed public IP.

## Using it

1. Install Plus and open the desktop or Start menu shortcut.
2. Choose Listen Together, enter your name, and choose Host.
3. Copy the complete invite and send it once to a friend using this Plus build.
4. Your friend pastes it into Join. Approve the request in your app.
5. They choose Save session. A persistent Saved checkmark confirms it is kept.
   Later, Reconnect returns to the open room without another approval.
6. Host shows only your saved sessions; Join shows only friends' rooms. Use the
   star to favorite a room and Rename to give it a local label. Forget removes
   a friend's shortcut; owned rooms are permanent and cannot be forgotten.
   Manage saved rooms is available while listening too. Addresses
   and complete invites are kept out of the list; Copy invite shares the invite.
7. Join checks availability immediately, then every 30 seconds while its list
   is visible. Open rooms come first, then most recently used, then favorites
   and name as tie-breakers. Checking, Open, Closed and Unavailable labels
   distinguish a confirmed result from a network failure. A failed check does
   not prevent trying Reconnect.
8. At app startup, the main window checks saved friends once. If any rooms are
   confirmed open, a Friends are listening popup lists them with Join buttons.
   It stays silent when none are open or checks fail. Not now dismisses it for
   this launch; See all friends' rooms opens the regular Join list. It never
   joins automatically, checks owned sessions, or starts a local relay. Opening
   Listen Together manually or joining a room suppresses a late startup popup.

Hosting again reopens your own saved session automatically. Each friend can
also host their own independent session on their PC. A saved entry for Friend A
connects to A; Friend B hosts B's own entry. It does not move A's identity to B.
The address is a cryptographic public identity, not the host's current IP.

The regular Host room is pinned separately in `lt_own_room`, including its private
owner credential. The first existing owned room is migrated on the next Host;
new recovery rooms are marked separately and cannot become that destination.
Repeated Host clicks cannot start overlapping hosting requests. If saved ownership
cannot be read, hosting reports an error instead of creating a new identity.
Existing invites and guest approvals continue to address the same room. This
cannot restore approvals for a different room that was already created in an
older build; reopen the original saved room to use its existing approvals.

The host PC must be on and Limusic must be running and hosting. Previously
approved guests can return while the room is open; credentials cannot open a
closed room or grant hosting ownership. Closing the main
window keeps it in the system tray, along with its hosted relay. Quit from the
tray exits the app and stops its relay; a Windows job also stops it after an app
crash. Normal startup and joining a friend do not start a local relay. Once
started, the local relay remains available until the app exits.

## Availability checks

Checks use the relay's read-only CheckRoom message to ask whether that exact
room has a connected host. A reachable PC or running relay alone is not enough.
No membership, pending approval, playback state or recently-used timestamp is
created by a probe. The client bounds checks to four at a time per scan and
eight seconds per room. Closing the list stops scheduling further list checks.
The startup scan runs once in the main window, not in the mini player or on
every tray restore. Dismissing its popup stops scheduling further startup checks.
Availability is a snapshot, so a host may leave between a check and a click.
Older relays without CheckRoom support show Unavailable until updated.

Recent use is persisted in each saved record as last_used_ms after successful
hosting, joining or reconnecting (and when saving an active room), never for a
background check or a failed join. Existing entries default to zero until used.
Favorites remain available as a tie-breaker instead of a separate group that
could put a closed room above an open one.

## Network service dependency

The Limusic relay runs on the host PC. Iroh tries an encrypted direct connection,
using n0's public discovery and relay infrastructure for discovery/NAT traversal
and encrypted fallback when direct connectivity fails. This is still an external
service dependency; it is not a promise of operation without infrastructure.
The creator's default Limusic relay is not involved in Plus peer hosting.
The app does not change router settings or install another VPN/client.

The pinned Iroh N0 preset uses public services. They have rate limits and no
uptime guarantee, so this is an experimental distribution for friends, not a
production hosting SLA. Public discovery publishes the PC's public endpoint
identity and routing information. The private identity never leaves app storage.
See [Iroh](https://www.iroh.computer/) and the
[versioned N0 preset](https://docs.rs/iroh/1.4.1/iroh/endpoint/presets/struct.N0.html).

A live forced-fallback test exercises public discovery and relay use. Testing on
one PC cannot establish behavior for every ISP, firewall, or independent home
network; test the installer with a friend before treating it as a stable release.
If the service is unavailable, Host/Join reports a connection failure; no manual
network setup is presented as a requirement for normal use.

## What persists

| Data | Location | Lifetime |
| --- | --- | --- |
| Host's private Iroh identity | `lt_peer_secret` in the Plus app database | Across app restarts and IP changes |
| Friend's public identity, reserved room code, host name, local label, favorite and display name | `lt_saved_rooms` in the same database | Across restarts; Forget can remove friends' shortcuts |
| Owner's room credential | Owner's saved record; backend only | Across app restarts |
| Permanent normal Host room and owner credential | `lt_own_room` in the Plus app database; backend only | Across app restarts; independent of saved shortcuts |
| Room reservations and owner credentials | `%LOCALAPPDATA%\LimusicRelay\saved-rooms.sqlite3` | Across relay restarts |
| Guest's private return credential | `lt_guest_passes` in the Plus app database; never included in invites or UI state | Across app restarts |
| Approved guest credentials | Host relay's `approved_guests` SQLite table, scoped to a reserved room | Across relay restarts; revoked by Kick |
| Active playback, queue, pending approvals, temporary memberships | App/relay memory | Active session only |
| Display name input | Webview local storage `lt_name` | Across app restarts |

App database: `%APPDATA%\com.limusic.plus\limusic.sqlite`. Back up this folder
and the relay database together to preserve your hosting identity and code.
Deleting either can break existing saved friend entries. Uninstall preserves
app data. Do not share your private app database or owner credentials with friends.
Invites contain only the public endpoint identity and code, encoded as
`LMSC~base64(limusic-peer://<public-key>|<room-code>)`.

The relay reserves room codes in SQLite, rather than relying on an ephemeral code.
Only the owning local app may create/reopen/reserve rooms through the peer bridge.
Guests require approval once per room and device. The app remembers a private
return credential even if the guest does not save a shortcut. Reconnecting or
restarting the app/host does not ask again while that room is open. A new device
or removal of app data requires approval again. Kick revokes the guest's stored
permission; they must be approved again before returning. Forget removes the
shortcut only and does not revoke approval. Neither an invite nor a display name
is enough to bypass approval. Existing guests from older builds must be approved
once with the updated app and relay to receive a credential. Each
listener streams audio independently; the relay carries controls and metadata.
Existing live host transfers do not relocate the relay: if its PC exits, everyone
in that session loses the connection. The recovery flow below can then offer a
replacement on another member's PC. Opening your own saved session is separate.

## Recovering after the server PC disconnects

Updated Plus clients advertise an independent encrypted recovery endpoint after
joining an updated room server. This listener does not start a local room server,
create a room, or share the PC's private hosting identity. The room server sends
these public recovery addresses only to approved members. They stay in memory for
the current app session; leaving or being kicked clears the recovery context.

If the room connection drops, normal reconnection continues. After a short grace
period, Listen Together opens with **Keep waiting** and **Host and invite friends**.
The latter explicitly authorizes starting a replacement room on that PC. Once it
is ready, other members receive **Join replacement room** with the volunteer's
name. They must accept before moving; dismissing the dialog never moves anyone.
There is no automatic server migration or forced choice of a replacement host.
If several members volunteer, invitations prefer the former playback host, then
a stable member order. Separate volunteers can still create separate rooms.

Invitations travel directly between the surviving apps, using Iroh discovery and
encrypted connections independently of the failed PC. A recipient authenticates
the inviter's endpoint against its last approved-member roster. Each invitation
is scoped to the old session and contains a fresh, recipient-specific return pass
for the new room. The new server installs these passes before invitations become
available. Current members do not need another manual entrance approval. The
replacement room gets its own saved entry; the original room's saved entry and
ownership remain intact. Original owner credentials and other guests' private
passes are never copied to a volunteer.

The local player and playlist are kept while disconnected. Recovery captures the
current track, position, play/pause state and upcoming playlist; new guests then
sync to the volunteer's live player. The shared upcoming queue now supports up to
5,000 tracks rather than 50, with an 8 MiB wire-message limit. Local files cannot
be shared and are filtered as in ordinary Listen Together playback. Played history
and playlist context that was never shared are not reconstructed from the server.

Recovery invitations last ten minutes and require the surviving apps to stay
open and reachable. People who had already left, older clients without recovery
support, and apps restarted after the outage cannot use that session's ephemeral
contacts. Existing saved shortcuts still address their original PC; this flow
does not redirect offline people's bookmarks or transfer the old room's entire
approval registry. Share a new invite with those friends when needed.

If only the playback host disconnects while the room server survives, the existing
20-second automatic host succession still applies. A server shutdown, power cut
or network outage needs the explicit replacement-room flow above. Public discovery
and relay availability remain dependencies for reaching surviving members.

Windows checks: `powershell -File scripts/tests/recovery.check.ps1` exercises
member-only discovery, host changes, kicks, playlist recovery, durable fresh
permissions, storage failures, and an encrypted invitation with no original room
server. Test real power-loss recovery across separate home networks before sharing
an installer as a validated release.
`node scripts/tests/recovery-ui.check.mjs` runs the actual dialog in headless Edge
with native commands mocked, verifying automatic prompting and explicit consent.

## Source and Windows builds

The installer includes the application, relay, libmpv, Microsoft runtime DLLs,
WebView2 bootstrapper, license, and modified source archive. It creates desktop
and Start menu `.lnk` files targeting `LimusicPlus.exe`. The installed app itself
is `Limusic Plus.exe`, with Limusic Plus product metadata, title bar, sidebar,
About name, main/mini window titles, tray and media labels. The launcher starts
the app or restores its existing tray window. Upgrading an existing Plus install
replaces its retired `limusic-app.exe` with a tiny compatibility launcher for
old pinned shortcuts and URL handlers, and repoints an existing login entry while
keeping its arguments and disabled state. App identity and saved data stay the
same; quit the running app from its tray before upgrading. No resident PowerShell supervisor is
needed for the normal shortcut.

Build prerequisites are in [BUILD-PLATFORMS.md](BUILD-PLATFORMS.md): MSVC x64 C++
build tools and Windows SDK, Rust 1.91+, Node/pnpm, libmpv DLL plus MSVC import
library, and Inno Setup 6. With those prepared, from the repository:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/build-windows-installer.ps1
```

Output: `dist/Limusic-Plus-<version>-setup-x64.exe`, SHA256 file, and source zip.
Friends need only the setup exe. They need no Rust, Node, compiler, or commands.
Plus has its own app identity and uses replacement Plus installers for updates.
Upstream automatic updates are disabled to preserve these modifications.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/tests/local-relay.check.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/tests/peer.check.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/tests/peer.check.ps1 -Live
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/tests/windows-installer.check.ps1
```

The live test uses public infrastructure and requires internet access. The local
peer test uses loopback QUIC and the real relay, including approval, playback,
blocked remote room creation, identity reload, relay restart and saved reconnect without a second approval.

Original/source builds retain the original default relay and advanced server
selection. Existing `ws://` and `wss://` saved records remain readable. An old
creator-relay code cannot become a persistent Plus peer identity automatically;
create a Plus session and share its new complete invite once. Legacy manual
`scripts/start-limusic.ps1 -HostRelay` remains available for advanced deployments;
it is not the installed shortcut's normal mode.

# Open room invitations from a browser

From a room you host in Limusic Plus, choose **Copy app invite link**. The
Windows installer registers links that begin with `limusic-plus://`. When you
open a link from a browser such as Firefox, allow it to open Limusic Plus,
then confirm **Join** with the room details already filled in. The room host
still approves new guests. **Copy invite** remains available for chat apps
that only link to web addresses.
