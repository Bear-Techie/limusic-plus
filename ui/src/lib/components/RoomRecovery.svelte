<script lang="ts">
    import { Button } from '$lib/components/ui/button';
    import * as api from '$lib/api';
    import { lt } from '$lib/lt.svelte';
    import { ui, toast } from '$lib/player.svelte';
    import { t } from '$lib/i18n.svelte';

    let acting = $state(false);
    async function recover(join: boolean) {
        acting = true;
        try {
            if (join) await api.ltAcceptRecovery();
            else await api.ltHostRecovery();
        } catch (error) {
            toast.error(String(error));
        } finally {
            acting = false;
        }
    }
</script>

{#if lt.recoveryDisconnected || lt.recoveryBusy}
    <section role="status" aria-live="polite" class="my-4 rounded-2xl border border-primary/30 bg-primary/5 p-4">
        <p class="text-sm font-medium">{t(lt.recoveryBusy ? 'dialogs.listen_together.recovery_preparing' : 'dialogs.listen_together.recovery_title')}</p>
        <p class="mt-1 text-sm text-muted-foreground">
            {lt.recoveryInviter
                ? t('dialogs.listen_together.recovery_invite', { name: lt.recoveryInviter })
                : t('dialogs.listen_together.recovery_hint')}
        </p>
        <p class="mt-2 text-xs text-muted-foreground">{t('dialogs.listen_together.recovery_playlist')}</p>
        <div class="mt-3 flex flex-wrap gap-2">
            {#if lt.recoveryInviter}
                <Button size="sm" disabled={acting || lt.recoveryBusy} onclick={() => recover(true)}>{t('dialogs.listen_together.recovery_join')}</Button>
            {/if}
            {#if lt.peerHosting && lt.localRelayAvailable}
                <Button size="sm" variant={lt.recoveryInviter ? 'outline' : 'default'} disabled={acting || lt.recoveryBusy} onclick={() => recover(false)}>{t('dialogs.listen_together.recovery_host')}</Button>
            {/if}
            <Button size="sm" variant="ghost" disabled={acting || lt.recoveryBusy} onclick={() => { ui.ltOpen = false; }}>{t('dialogs.listen_together.recovery_wait')}</Button>
        </div>
    </section>
{/if}
