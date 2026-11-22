<script lang="ts">
	import { tick, untrack } from 'svelte';
	import { Button } from '$lib/components/ui/button';
	import { Input } from '$lib/components/ui/input';
	import * as api from '$lib/api';
	import { toast } from '$lib/player.svelte';
	import { t } from '$lib/i18n.svelte';
	import { checkFriendRooms, savedRoomKey, sortedSavedRooms, type RoomAvailability, type RoomScope } from '$lib/saved-rooms';

	let { rooms, busy, reconnect, scope, active = true, showReconnect = true }: {
		rooms: api.LtSavedRoom[];
		busy: boolean;
		reconnect: (server: string, code: string) => Promise<void>;
		showReconnect?: boolean;
		scope: RoomScope;
		active?: boolean;
	} = $props();
	let updating = $state(false);
	let editing = $state('');
	let removing = $state('');
	let draft = $state('');
	let input = $state<HTMLInputElement | null>(null);
	const disabled = $derived(busy || updating);
	let availability = $state<Record<string, RoomAvailability>>({});
	const ordered = $derived(sortedSavedRooms(rooms, scope, availability));
	const roomKeys = $derived(rooms.filter((room) => !room.owner).map(savedRoomKey).sort().join('\n'));
	$effect(() => {
		const signature = roomKeys;
		if (!active || scope !== 'join' || !signature) return;
		let disposed = false;
		let checking = false;
		availability = {};
		async function refresh() {
			if (disposed || checking || busy || updating || document.hidden) return;
			checking = true;
			await checkFriendRooms(untrack(() => rooms.slice()),
				(room) => api.ltCheckSavedRoom(room.serverUrl, room.roomCode),
				(room, result) => { availability = { ...availability, [savedRoomKey(room)]: result }; },
				() => !disposed && !document.hidden);
			checking = false;
		}
		untrack(() => { void refresh(); });
		const timer = setInterval(() => { void refresh(); }, 30_000);
		const visible = () => { if (!document.hidden) void refresh(); };
		document.addEventListener('visibilitychange', visible);
		return () => { disposed = true; clearInterval(timer); document.removeEventListener('visibilitychange', visible); };
	});

	const key = savedRoomKey;
	function name(room: api.LtSavedRoom) {
		return room.name || (room.owner ? t('dialogs.listen_together.saved_owner') : room.hostName || room.roomCode);
	}
	async function edit(room: api.LtSavedRoom) {
		editing = key(room);
		removing = '';
		draft = name(room);
		await tick();
		input?.focus();
		input?.select();
	}
	async function update(room: api.LtSavedRoom, changes: { name?: string; favorite?: boolean }) {
		updating = true;
		try {
			await api.ltUpdateSavedRoom(room.serverUrl, room.roomCode, changes);
			editing = '';
		} catch (error) {
			toast.error(String(error));
		} finally {
			updating = false;
		}
	}
	async function forget(room: api.LtSavedRoom) {
		updating = true;
		try {
			await api.ltForgetRoom(room.serverUrl, room.roomCode);
			removing = '';
		} catch (error) {
			toast.error(String(error));
		} finally {
			updating = false;
		}
	}
</script>

<section class="flex min-h-0 flex-col gap-3" aria-label={t('dialogs.listen_together.saved_rooms')}>
	<div>
		<p class="text-sm font-medium">{t('dialogs.listen_together.saved_rooms')}</p>
		<p class="mt-1 text-xs text-muted-foreground">{t(scope === 'join' ? 'dialogs.listen_together.available_rooms_hint' : 'dialogs.listen_together.own_rooms_hint')}</p>
	</div>
	<div class="flex max-h-64 flex-col gap-4 overflow-y-auto pr-1">
			<div class="flex flex-col gap-2">
				<p class="text-xs font-medium text-muted-foreground">{t(scope === 'host' ? 'dialogs.listen_together.my_rooms' : 'dialogs.listen_together.friends_rooms')} <span class="ml-1 tabular-nums">{ordered.length}</span></p>
				{#each ordered as room (key(room))}
					<div class="rounded-2xl border bg-muted/20 p-3">
						{#if editing === key(room)}
							<form class="flex flex-col gap-2" onsubmit={(event) => { event.preventDefault(); update(room, { name: draft }); }}>
								<label class="text-xs text-muted-foreground" for="saved-room-name">{t('dialogs.listen_together.room_name')}</label>
								<Input id="saved-room-name" bind:ref={input} bind:value={draft} maxlength={80} disabled={disabled} onkeydown={(event) => { if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); editing = ''; } }} />
								<div class="flex justify-end gap-1">
									<Button type="button" variant="ghost" size="sm" disabled={disabled} onclick={() => (editing = '')}>{t('common.cancel')}</Button>
									<Button type="submit" size="sm" disabled={disabled}>{t('common.save')}</Button>
								</div>
							</form>
						{:else if removing === key(room)}
							<p class="break-words text-sm font-medium">{name(room)}</p>
							<p class="mt-1 text-xs text-muted-foreground">{t(room.owner ? 'dialogs.listen_together.forget_owner_hint' : 'dialogs.listen_together.forget_hint')}</p>
							<div class="mt-2 flex justify-end gap-1">
								<Button variant="ghost" size="sm" disabled={disabled} onclick={() => (removing = '')}>{t('common.cancel')}</Button>
								<Button variant="destructive" size="sm" disabled={disabled} onclick={() => forget(room)}>{t('dialogs.listen_together.forget')}</Button>
							</div>
						{:else}
							<div class="flex items-center gap-2">
								<button type="button" class="shrink-0 cursor-pointer px-1 text-xl text-primary disabled:opacity-50" disabled={disabled} aria-pressed={room.favorite} aria-label={t(room.favorite ? 'dialogs.listen_together.unfavorite' : 'dialogs.listen_together.favorite')} onclick={() => update(room, { favorite: !room.favorite })}>{room.favorite ? '★' : '☆'}</button>
								<div class="min-w-0 flex-1">
									<p class="break-words text-sm font-medium">{name(room)}</p>
									{#if scope === 'join'}
										<p class="mt-0.5 flex items-center gap-1.5 text-xs text-muted-foreground">
											<span aria-hidden="true" class="h-1.5 w-1.5 rounded-full {availability[key(room)] === 'open' ? 'bg-green-500' : !availability[key(room)] ? 'animate-pulse bg-amber-500' : 'bg-muted-foreground/50'}"></span>
											{t(availability[key(room)] === 'open' ? 'dialogs.listen_together.room_open' : availability[key(room)] === 'closed' ? 'dialogs.listen_together.room_closed' : availability[key(room)] === 'unknown' ? 'dialogs.listen_together.room_unavailable' : 'dialogs.listen_together.room_checking')}
										</p>
									{/if}
								</div>
								{#if showReconnect}<Button size="sm" disabled={disabled} onclick={() => reconnect(room.serverUrl, room.roomCode)}>{t('dialogs.listen_together.reconnect')}</Button>{/if}
							</div>
							<div class="mt-1 flex justify-end gap-1">
								<Button variant="ghost" size="sm" disabled={disabled} onclick={() => edit(room)}>{t('dialogs.listen_together.rename')}</Button>
								{#if !room.owner}<Button variant="ghost" size="sm" disabled={disabled} onclick={() => { removing = key(room); editing = ''; }}>{t('dialogs.listen_together.forget')}</Button>{/if}
							</div>
						{/if}
					</div>
				{/each}
			</div>
	</div>
</section>
