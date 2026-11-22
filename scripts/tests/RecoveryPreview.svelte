<script lang="ts">
    import ListenTogether from '../../ui/src/lib/components/ListenTogether.svelte';
    import { lt } from '../../ui/src/lib/lt.svelte';
    import { ui } from '../../ui/src/lib/player.svelte';
    import '../../ui/src/routes/layout.css';

    Object.assign(lt, {
        peerHosting: true, localRelayAvailable: true, status: 'connected', role: 'guest',
        roomCode: 'OLDROOM', myId: 'me', serverUrl: 'limusic-peer://private-address',
        users: [
            { user_id: 'me', username: 'Me', is_host: false, is_connected: true },
            { user_id: 'host', username: 'Alex', is_host: true, is_connected: true }
        ]
    });
    ui.ltOpen = false;
    // Test fixture API, deliberately outside production code.
    (window as any).__recoveryPreview = {
        set: (patch: Partial<typeof lt>) => Object.assign(lt, patch),
        open: () => ui.ltOpen = true
    };
</script>

<main data-recovery-preview><ListenTogether /></main>
