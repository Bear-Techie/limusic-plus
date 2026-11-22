<script lang="ts">
	import { onMount } from 'svelte';
	import * as Dialog from '$lib/components/ui/dialog';
	import { Button } from '$lib/components/ui/button';
	import * as api from '$lib/api';
	import { lt } from '$lib/lt.svelte';
	import { ui, toast } from '$lib/player.svelte';
	import { t } from '$lib/i18n.svelte';
	import { checkFriendRooms, sortedSavedRooms, savedRoomKey, type RoomAvailability } from '$lib/saved-rooms';

	let open = $state(false);
	let shown = $state(false);
	let dismissed = $state(false);
	let joining = $state(false);
	let rooms = $state<api.LtSavedRoom[]>([]);
	let availability = $state<Record<string, RoomAvailability>>({});
	const openRooms = $derived(sortedSavedRooms(rooms, 'join', availability).filter((room) => availability[savedRoomKey(room)] === 'open'));

	// Manual interaction wins over startup discovery. Dismissing never causes another popup.
	$effect(() => {
		if ((shown && !open) || ui.ltOpen || lt.role !== 'none' || lt.requesting) dismissed = true;
		if (dismissed) open = false;
	});

	onMount(() => {
		let disposed = false;
		const active = () => !disposed && !dismissed && !ui.ltOpen && lt.role === 'none' && !lt.requesting;
		void (async () => {
			try {
				const state = await api.ltGetState();
				if (!active() || state.role !== 'none' || state.requesting) return;
				rooms = state.savedRooms.filter((room) => !room.owner);
				await checkFriendRooms(rooms,
					(room) => api.ltCheckSavedRoom(room.serverUrl, room.roomCode),
					(room, result) => {
						availability = { ...availability, [savedRoomKey(room)]: result };
						if (result === 'open') { shown = true; open = true; }
					}, active);
			} catch { /* Startup discovery is silent when no rooms can be checked. */ }
		})();
		return () => { disposed = true; };
	});

	async function join(room: api.LtSavedRoom) {
		joining = true;
		dismissed = true;
		open = false;
		try {
			await api.ltOpenSaved(room.serverUrl, room.roomCode);
			ui.ltOpen = true;
		} catch (error) {
			toast.error(String(error));
		} finally { joining = false; }
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="max-h-[80dvh] overflow-y-auto sm:max-w-md">
		<Dialog.Header>
			<Dialog.Title>{t('dialogs.listen_together.startup_open_title')}</Dialog.Title>
			<Dialog.Description>{t('dialogs.listen_together.startup_open_hint')}</Dialog.Description>
		</Dialog.Header>
		<div class="flex flex-col gap-2">
			{#each openRooms as room (savedRoomKey(room))}
				<div class="flex items-center gap-3 rounded-2xl border bg-muted/20 p-3">
					<span aria-hidden="true" class="h-2 w-2 shrink-0 rounded-full bg-green-500"></span>
					<p class="min-w-0 flex-1 break-words text-sm font-medium">{room.name || room.hostName || room.roomCode}</p>
					<Button size="sm" disabled={joining} onclick={() => join(room)}>{t('dialogs.listen_together.join_tab')}</Button>
				</div>
			{/each}
		</div>
		<div class="flex justify-end gap-2">
			<Button variant="ghost" onclick={() => { dismissed = true; open = false; }}>{t('dialogs.listen_together.not_now')}</Button>
			<Button variant="outline" onclick={() => { dismissed = true; open = false; ui.ltOpen = true; }}>{t('dialogs.listen_together.browse_rooms')}</Button>
		</div>
	</Dialog.Content>
</Dialog.Root>
