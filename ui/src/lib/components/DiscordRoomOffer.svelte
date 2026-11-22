<script lang="ts">
	import { onMount } from 'svelte';
	import { ltCreateRoom, ltOpenSaved } from '$lib/api';
	import { Button } from '$lib/components/ui/button';
	import { Input } from '$lib/components/ui/input';
	import { t } from '$lib/i18n.svelte';
	import { canStartRoom, lt } from '$lib/lt.svelte';
	import { sortedSavedRooms } from '$lib/saved-rooms';

	let { onDismiss, onRoomOpening }: { onDismiss: () => void; onRoomOpening: () => void } = $props();
	let name = $state('');
	let rememberedName = $state(false);
	let busy = $state(false);
	let error = $state('');
	const nameId = $props.id();
	const lastRoom = $derived(sortedSavedRooms(lt.savedRooms, 'host', {})[0]);

	onMount(() => {
		name = (localStorage.getItem('lt_name') ?? '').trim();
		rememberedName = !!name;
	});

	$effect(() => {
		if (!busy && !canStartRoom()) onDismiss();
	});

	async function openRoom(event: SubmitEvent) {
		event.preventDefault();
		if (busy || !canStartRoom() || (!lastRoom && !name.trim())) return;
		busy = true;
		error = '';
		try {
			if (lastRoom) {
				await ltOpenSaved(lastRoom.serverUrl, lastRoom.roomCode);
			} else {
				await ltCreateRoom(name.trim());
				try { localStorage.setItem('lt_name', name.trim()); } catch { /* The room is already opening. */ }
			}
			onRoomOpening();
		} catch (cause) {
			error = String(cause);
		} finally {
			busy = false;
		}
	}
</script>

<form class="grid gap-2 rounded-md border bg-muted/30 p-3 text-sm" onsubmit={openRoom}>
	<p class="font-medium">{t('settings.discord_voice.room_offer')}</p>
	{#if lastRoom}
		<p class="text-xs text-muted-foreground">
			{t('settings.discord_voice.room_reopen_hint', { room: lastRoom.name || lastRoom.hostName || lastRoom.roomCode })}
		</p>
	{:else if !rememberedName}
		<label class="grid gap-1.5 text-xs text-muted-foreground" for={nameId}>
			{t('dialogs.listen_together.your_name')}
			<Input id={nameId} bind:value={name} maxlength={24} required disabled={busy} />
		</label>
	{/if}
	{#if error}
		<p role="alert" class="text-xs text-destructive">{error}</p>
	{/if}
	<div class="flex flex-wrap gap-2">
		<Button type="submit" size="sm" disabled={busy || (!lastRoom && !name.trim())}>
			{t(busy ? 'dialogs.listen_together.connecting' : lastRoom ? 'settings.discord_voice.room_reopen' : 'settings.discord_voice.room_host')}
		</Button>
		<Button type="button" size="sm" variant="ghost" disabled={busy} onclick={onDismiss}>
			{t('settings.discord_voice.room_skip')}
		</Button>
	</div>
</form>
