<script lang="ts">
	import { invoke } from '@tauri-apps/api/core';
	import { onMount } from 'svelte';
	import { HugeiconsIcon } from '@hugeicons/svelte';
	import { CheckmarkCircle01Icon, MusicNote01Icon } from '@hugeicons/core-free-icons';
	import { DropdownMenu } from 'bits-ui';
	import { Button } from '$lib/components/ui/button';
	import * as Dialog from '$lib/components/ui/dialog';
	import { getSettings, setSetting } from '$lib/api';
	import { botLibrary, loadBots } from '$lib/discord-bots.svelte';
	import { t } from '$lib/i18n.svelte';
	import { prefs, toast, ui } from '$lib/player.svelte';
	import DiscordIcon from './DiscordIcon.svelte';

	type VoiceChannel = { id: string; name: string; guild: string };
	let open = $state(false);
	let trigger = $state<HTMLButtonElement | null>(null);
	let status = $state('');
	let botId = $state('');
	let channel = $state('');
	let savedChannelName = $state('');
	let resolvedChannelName = $state('');
	let channels = $state<VoiceChannel[] | null>(null);
	let legacyChannel = $state('');
	let busy = $state(false);
	let error = $state('');
	const active = $derived(['connecting', 'live', 'reconnecting', 'stopping'].includes(status));
	const voiceAvailable = $derived(status && status !== 'unsupported' && status !== 'not_installed');
	const channelName = $derived(channels?.find((item) => item.id === channel)?.name
		?? (channels ? '' : savedChannelName || resolvedChannelName));
	const messages = $derived<Record<string, string>>({
		connecting: t('settings.discord_voice.connecting'),
		live: t('settings.discord_voice.live'),
		reconnecting: t('settings.discord_voice.reconnecting'),
		stopping: t('settings.discord_voice.stopping'),
		occupied: t('settings.discord_voice.occupied'),
		channel: t('settings.discord_voice.channel_error'),
		token: t('settings.discord_voice.token_error'),
		credentials: t('settings.discord_voice.credentials_error'),
		connection: t('settings.discord_voice.connection'),
		dependencies: t('settings.discord_voice.dependencies'),
		node: t('settings.discord_voice.node'),
		audio: t('settings.discord_voice.audio'),
		capture: t('settings.discord_voice.capture'),
		busy: t('settings.discord_voice.busy'),
		failed: t('settings.discord_voice.failed')
	});

	async function refresh() {
		status = await invoke<string>('voice_status').catch(() => 'unsupported');
	}
	onMount(() => { void refresh(); });
	$effect(() => {
		if (!open) return;
		const timer = window.setInterval(refresh, 500);
		return () => window.clearInterval(timer);
	});

	// Older saves and imported bots may only have the channel ID.
	$effect(() => {
		if (!open || busy || active || !botId || !channel || savedChannelName || resolvedChannelName || channels) return;
		const id = channel;
		let cancelled = false;
		void invoke<{ channels: VoiceChannel[] }>('voice_discover', { token: null, botId })
			.then(({ channels }) => {
				if (!cancelled) resolvedChannelName = channels.find((item) => item.id === id)?.name ?? '';
			})
			.catch(() => {});
		return () => { cancelled = true; };
	});

	async function toggleDiscord(next: boolean) {
		prefs.discordRpc = next;
		try {
			await setSetting('discord_rpc', String(next));
			toast.success(t(next ? 'integrations.discord_on' : 'integrations.discord_off'));
		} catch (cause) {
			prefs.discordRpc = !next;
			toast.error(String(cause));
		}
	}

	function selectBot(id: string) {
		botId = id;
		const bot = botLibrary.entries.find((bot) => bot.id === id);
		channel = id === 'legacy' ? legacyChannel : bot?.channel ?? '';
		savedChannelName = bot?.channel_name ?? '';
		resolvedChannelName = '';
		channels = null;
		error = '';
	}

	async function show() {
		open = true;
		busy = true;
		error = '';
		try {
			const [settings] = await Promise.all([getSettings(), loadBots(), refresh()]);
			legacyChannel = settings.discord_voice_channel ?? '';
			const preferred = botLibrary.entries.find((bot) => bot.id === settings.discord_voice_bot_id);
			selectBot(preferred?.id ?? botLibrary.entries[0]?.id ?? '');
		} catch {
			error = 'credentials';
		} finally {
			busy = false;
		}
	}

	async function findChannels() {
		busy = true;
		error = '';
		try {
			const bot = await invoke<{ channels: VoiceChannel[] }>('voice_discover', { token: null, botId });
			channels = bot.channels;
			if (!channels.some((item) => item.id === channel)) {
				channel = channels.length === 1 ? channels[0].id : '';
			}
		} catch (cause) {
			error = String(cause);
		} finally {
			busy = false;
		}
	}

	async function sendBot(event: SubmitEvent) {
		event.preventDefault();
		if (busy || !channel || !botId) return;
		busy = true;
		error = '';
		try {
			await invoke('voice_start', { botId, channel, channelName });
			await refresh();
			await Promise.all([
				setSetting('discord_voice_bot_id', botId),
				setSetting('discord_voice_channel', channel)
			]);
		} catch (cause) {
			error = String(cause);
		} finally {
			busy = false;
		}
	}

	async function leave() {
		busy = true;
		error = '';
		try {
			await invoke('voice_stop');
			await refresh();
		} catch {
			error = 'failed';
		} finally {
			busy = false;
		}
	}

	function configure() {
		open = false;
		ui.settingsFocus = 'discord';
		ui.settingsOpen = true;
	}
</script>

<DropdownMenu.Root>
	<DropdownMenu.Trigger bind:ref={trigger}
		class="flex h-full w-8 items-center justify-center text-muted-foreground transition-colors hover:bg-accent/10 hover:text-foreground"
		title="Discord" aria-label="Discord">
		<span class="relative">
			<DiscordIcon class="h-4 w-4" />
			<span class="absolute -right-0.5 -top-0.5 h-1.5 w-1.5 rounded-full ring-[1.5px] ring-background {prefs.discordRpc
				? 'bg-emerald-500'
				: 'bg-red-500'}"></span>
		</span>
	</DropdownMenu.Trigger>
	<DropdownMenu.Portal>
		<DropdownMenu.Content align="end" sideOffset={4}
			class="z-50 min-w-48 rounded-md border bg-popover p-1 text-sm text-popover-foreground shadow-md"
			onCloseAutoFocus={(event) => { if (open) event.preventDefault(); }}>
			<DropdownMenu.Item disabled={!voiceAvailable} onSelect={show}
				class="flex cursor-default items-center gap-2 rounded-sm px-2 py-1.5 font-medium outline-none data-highlighted:bg-accent data-disabled:opacity-50">
				<HugeiconsIcon icon={MusicNote01Icon} class="h-4 w-4" />
				{t('settings.discord_voice.quick_action')}
			</DropdownMenu.Item>
			<DropdownMenu.CheckboxItem checked={prefs.discordRpc} onCheckedChange={toggleDiscord}
				class="flex cursor-default items-center gap-2 rounded-sm px-2 py-1.5 outline-none data-highlighted:bg-accent">
				<span class="size-4">
					{#if prefs.discordRpc}<HugeiconsIcon icon={CheckmarkCircle01Icon} class="size-4" />{/if}
				</span>
				{t('integrations.discord_show_status')}
			</DropdownMenu.CheckboxItem>
		</DropdownMenu.Content>
	</DropdownMenu.Portal>
</DropdownMenu.Root>

<Dialog.Root bind:open>
	<Dialog.Content onCloseAutoFocus={(event) => { event.preventDefault(); trigger?.focus(); }}>
		<Dialog.Header>
			<Dialog.Title>{t('settings.discord_voice.quick_title')}</Dialog.Title>
			<Dialog.Description>{t('settings.discord_voice.quick_hint')}</Dialog.Description>
		</Dialog.Header>
		{#if active}
			<Button size="sm" variant="outline" disabled={busy || status === 'stopping'} onclick={leave}>
				{t('settings.discord_voice.stop')}
			</Button>
		{:else if botLibrary.entries.length}
			<form class="grid gap-3" onsubmit={sendBot}>
				<label class="grid gap-1.5 text-xs font-medium">
					{t('settings.discord_voice.saved_bots')}
					<select value={botId} onchange={(event) => selectBot(event.currentTarget.value)} disabled={busy}
						class="h-9 w-full rounded-md border bg-background px-2 text-sm">
						{#each botLibrary.entries as bot (bot.id)}
							<option value={bot.id}>{bot.name}</option>
						{/each}
					</select>
				</label>
				{#if channels?.length}
					<label class="grid gap-1.5 text-xs font-medium">
						{t('settings.discord_voice.channel')}
						<select bind:value={channel} disabled={busy} required class="h-9 w-full rounded-md border bg-background px-2 text-sm">
							<option value="" disabled>{t('settings.discord_voice.choose_channel')}</option>
							{#each channels as item (item.id)}
								<option value={item.id}>{item.guild} · {item.name}</option>
							{/each}
						</select>
					</label>
				{:else if channelName}
					<p class="text-xs text-muted-foreground">{t('settings.discord_voice.quick_saved_channel', { channel: channelName })}</p>
				{:else if channel}
					<p class="text-xs text-muted-foreground">{t('settings.discord_voice.quick_unnamed_channel')}</p>
				{:else if channels}
					<p class="text-xs text-muted-foreground">{t('settings.discord_voice.no_channels')}</p>
				{/if}
				<div class="flex flex-wrap gap-2">
					<Button type="submit" size="sm" disabled={busy || !botId || !channel}>{t('settings.discord_voice.quick_title')}</Button>
					<Button type="button" size="sm" variant="outline" disabled={busy || !botId} onclick={findChannels}>
						{t('settings.discord_voice.refresh_channels')}
					</Button>
				</div>
			</form>
		{/if}
		{#if error || (status && messages[status])}
			<p role="status" class="text-xs" class:text-destructive={!!error || !active}>
				{error ? messages[error] ?? messages.failed : messages[status]}
			</p>
		{/if}
		{#if !active}
			<Button type="button" size="sm" variant={botLibrary.entries.length ? 'ghost' : 'outline'} disabled={busy} onclick={configure}>
				{t('settings.discord_voice.quick_settings')}
			</Button>
		{/if}
	</Dialog.Content>
</Dialog.Root>
