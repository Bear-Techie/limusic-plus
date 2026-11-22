<script lang="ts">
	import { invoke } from '@tauri-apps/api/core';
	import { onMount } from 'svelte';
	import { Button } from '$lib/components/ui/button';
	import * as Dialog from '$lib/components/ui/dialog';
	import { loadBots, type BotOffer } from '$lib/discord-bots.svelte';
	import { t } from '$lib/i18n.svelte';
	import { lt } from '$lib/lt.svelte';
	import { toast } from '$lib/player.svelte';

	let offers = $state<BotOffer[]>([]);
	let busy = $state(false);
	let polling = false;
	const offer = $derived(offers[0]);

	async function refresh() {
		if (busy || polling) return;
		if (lt.status !== 'connected' || lt.role !== 'guest') {
			offers = [];
			return;
		}
		polling = true;
		try {
			offers = await invoke<BotOffer[]>('voice_bot_offers');
		} catch {
			offers = [];
		} finally {
			polling = false;
		}
	}

	onMount(() => {
		const timer = window.setInterval(refresh, 1200);
		return () => window.clearInterval(timer);
	});

	async function respond(accept: boolean) {
		if (!offer || busy) return;
		const id = offer.id;
		busy = true;
		try {
			await invoke('voice_respond_offer', { id, accept });
			offers = offers.filter((entry) => entry.id !== id);
			if (accept) {
				await loadBots().catch(() => {});
				toast.success(t('settings.discord_voice.room_saved'));
			}
		} catch (error) {
			toast.error(String(error));
		} finally {
			busy = false;
		}
	}
</script>

<Dialog.Root open={!!offer} onOpenChange={(open) => { if (!open) void respond(false); }}>
	{#if offer}
		<Dialog.Content class="sm:max-w-md">
			<Dialog.Header>
				<Dialog.Title>{t('settings.discord_voice.room_offer_title')}</Dialog.Title>
				<Dialog.Description>
					{t('settings.discord_voice.room_offer_hint', { host: offer.sender, bot: offer.name })}
				</Dialog.Description>
			</Dialog.Header>
			<Dialog.Footer>
				<Button variant="outline" disabled={busy} onclick={() => respond(false)}>{t('dialogs.listen_together.decline')}</Button>
				<Button disabled={busy} onclick={() => respond(true)}>{t('settings.discord_voice.room_accept')}</Button>
			</Dialog.Footer>
		</Dialog.Content>
	{/if}
</Dialog.Root>
