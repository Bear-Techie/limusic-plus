<script lang="ts">
	import { invoke } from '@tauri-apps/api/core';
	import { onMount, untrack } from 'svelte';
	import { Button } from '$lib/components/ui/button';
	import { Input } from '$lib/components/ui/input';
	import { openExternal, setSetting } from '$lib/api';
	import { t } from '$lib/i18n.svelte';
	import { toast } from '$lib/player.svelte';
	import limusicIcon from '../../../../src-tauri/icons/icon.png?inline';
	import { botLibrary, loadBots } from '$lib/discord-bots.svelte';

	type BotCatalog = {
		id: string;
		name: string;
		channels: { id: string; name: string; guild: string }[];
	};

	let { settings }: { settings: Record<string, string> } = $props();
	let channel = $state(untrack(() => settings.discord_voice_channel ?? ''));
	let token = $state('');
	let botId = $state(untrack(() => settings.discord_voice_bot_id ?? ''));
	let loaded = $state(false);
	let replacing = $state(false);
	let catalog = $state<BotCatalog | null>(null);
	let profileName = $state('Limusic Plus Bot');
	let profileFeedback = $state<'applied' | 'partial' | ''>('');
	let status = $state('');
	let actionError = $state('');
	let busy = $state(false);
	const active = $derived(['connecting', 'reconnecting', 'live', 'stopping'].includes(status));
	const configured = $derived(botLibrary.entries.some((bot) => bot.id === botId));
	const settingUp = $derived(loaded && (!configured || replacing));
	const channelName = $derived(catalog?.channels.find((item) => item.id === channel));
	const validProfileName = $derived([...profileName.trim()].length >= 2 && [...profileName.trim()].length <= 32);

	const messages = $derived<Record<string, string>>({
		connecting: t('settings.discord_voice.connecting'),
		reconnecting: t('settings.discord_voice.reconnecting'),
		live: t('settings.discord_voice.live'),
		stopping: t('settings.discord_voice.stopping'),
		not_installed: t('settings.discord_voice.not_installed'),
		dependencies: t('settings.discord_voice.dependencies'),
		node: t('settings.discord_voice.node'),
		channel: t('settings.discord_voice.channel_error'),
		occupied: t('settings.discord_voice.occupied'),
		token: t('settings.discord_voice.token_error'),
		credentials: t('settings.discord_voice.credentials_error'),
		profile_name: t('settings.discord_voice.profile_name_error'),
		profile_rate_limit: t('settings.discord_voice.profile_rate_limit'),
		profile_rejected: t('settings.discord_voice.profile_rejected'),
		connection: t('settings.discord_voice.connection'),
		capture: t('settings.discord_voice.capture'),
		audio: t('settings.discord_voice.audio'),
		busy: t('settings.discord_voice.busy'),
		failed: t('settings.discord_voice.failed')
	});

	async function refresh() {
		status = await invoke<string>('voice_status').catch(() => 'unsupported');
	}

	onMount(() => {
		void refresh();
		void loadBots().then(() => {
			selectBot(botLibrary.entries.find((bot) => bot.id === botId)?.id ?? botLibrary.entries[0]?.id ?? '');
		}).catch(() => {
			actionError = 'credentials';
		}).finally(() => { loaded = true; });
		const timer = window.setInterval(refresh, 500);
		return () => window.clearInterval(timer);
	});

	$effect(() => {
		const first = botLibrary.entries[0];
		if (loaded && first && !configured && !replacing && !token && !busy) {
			selectBot(first.id);
		}
	});

	function selectBot(id: string) {
		botId = id;
		const bot = botLibrary.entries.find((entry) => entry.id === id);
		channel = bot?.id === 'legacy' ? settings.discord_voice_channel ?? '' : bot?.channel ?? '';
		catalog = null;
		profileFeedback = '';
		actionError = '';
	}

	async function discover() {
		busy = true;
		actionError = '';
		try {
			catalog = await invoke<BotCatalog>('voice_discover', {
				token: settingUp ? token.trim() : null,
				botId: settingUp ? null : botId
			});
			if (!settingUp) profileName = catalog.name;
			if (!catalog?.channels.some((item) => item.id === channel)) channel = '';
			if (!channel && catalog?.channels.length === 1) channel = catalog.channels[0].id;
		} catch (error) {
			actionError = String(error);
		} finally {
			busy = false;
		}
	}

	async function applyProfile() {
		if (busy || !catalog || !validProfileName) return;
		busy = true;
		actionError = '';
		profileFeedback = '';
		try {
			const result = await invoke<{ name: string; application_error: string | null }>('voice_apply_profile', {
				token: settingUp ? token.trim() : null,
				botId: settingUp ? null : botId,
				name: profileName.trim()
			});
			catalog.name = result.name;
			profileName = result.name;
			profileFeedback = result.application_error ? 'partial' : 'applied';
			if (result.application_error) actionError = result.application_error;
		} catch (error) {
			actionError = String(error);
		} finally {
			busy = false;
		}
	}

	function inviteBot() {
		if (!catalog) return;
		const params = new URLSearchParams({
			client_id: catalog.id,
			scope: 'bot',
			permissions: String((1 << 10) | (1 << 20) | (1 << 21)),
			integration_type: '0'
		});
		void openExternal(`https://discord.com/oauth2/authorize?${params}`)
			.catch(() => { actionError = 'failed'; });
	}

	async function saveChannel() {
		await setSetting('discord_voice_channel', channel.trim());
		settings.discord_voice_channel = channel.trim();
		await setSetting('discord_voice_bot_id', botId);
		settings.discord_voice_bot_id = botId;
	}

	async function saveBot(event: SubmitEvent) {
		event.preventDefault();
		if (busy || !catalog || !channelName) return;
		busy = true;
		actionError = '';
		try {
			await invoke('voice_save_bot', { id: catalog.id, name: catalog.name, channel, channelName: channelName.name, token: token.trim() });
			botId = catalog.id;
			replacing = false;
			token = '';
			await saveChannel();
			await loadBots();
		} catch (error) {
			toast.error(messages[String(error)] ?? messages.failed);
		} finally {
			busy = false;
		}
	}

	function replaceBot() {
		catalog = null;
		replacing = true;
		actionError = '';
		profileName = 'Limusic Plus Bot';
		profileFeedback = '';
	}

	function cancelReplacement() {
		token = '';
		selectBot(botId);
		replacing = false;
		actionError = '';
		profileName = 'Limusic Plus Bot';
		profileFeedback = '';
	}

	async function forgetBot() {
		busy = true;
		actionError = '';
		try {
			await invoke('voice_forget_bot', { id: botId });
			await loadBots();
			botId = botLibrary.entries[0]?.id ?? '';
			cancelReplacement();
		} catch (error) {
			actionError = String(error);
		} finally {
			busy = false;
		}
	}

	async function broadcast(event: SubmitEvent) {
		event.preventDefault();
		if (busy || active || !configured || !channel.trim()) return;
		busy = true;
		actionError = '';
		try {
			await invoke('voice_start', { botId, channel: channel.trim(), channelName: channelName?.name });
			await refresh();
			await saveChannel().catch((error) => toast.error(String(error)));
			await loadBots().catch(() => {});
		} catch (error) {
			actionError = String(error);
		} finally {
			busy = false;
		}
	}

	async function leave() {
		busy = true;
		actionError = '';
		try {
			await invoke('voice_stop');
			await refresh();
		} catch {
			actionError = 'failed';
		} finally {
			busy = false;
		}
	}
</script>

{#snippet profileFields()}
	<div class="flex items-center gap-3">
		<img src={limusicIcon} alt="" width="40" height="40" class="size-10 shrink-0 rounded-md" />
		<label class="grid min-w-0 flex-1 gap-1.5 text-xs font-medium">
			{t('settings.discord_voice.profile_name')}
			<Input bind:value={profileName} disabled={busy} aria-invalid={!validProfileName}
				oninput={() => { profileFeedback = ''; }} />
		</label>
	</div>
	<p class="text-xs text-muted-foreground">
		{t('settings.discord_voice.profile_hint', { tags: 'Limusic, Music, Shared' })}
	</p>
	{#if !validProfileName}
		<p class="text-xs text-destructive">{t('settings.discord_voice.profile_name_error')}</p>
	{/if}
{/snippet}

{#snippet profileAction()}
	<Button type="button" variant="outline" size="sm" class="justify-self-start" disabled={busy || !validProfileName} onclick={applyProfile}>
		{t('settings.discord_voice.profile_apply')}
	</Button>
	{#if profileFeedback}
		<p class="text-xs" role="status">
			{profileFeedback === 'applied'
				? t('settings.discord_voice.profile_applied')
				: t('settings.discord_voice.profile_partial')}
		</p>
	{/if}
{/snippet}

{#snippet channelPicker()}
	<label for="discord-voice-channel" class="grid gap-1.5 text-xs font-medium">
		{t('settings.discord_voice.channel')}
		{#if catalog}
			<select id="discord-voice-channel" bind:value={channel} disabled={busy} required class="h-9 w-full rounded-md border bg-background px-2 text-sm">
				<option value="" disabled>{t('settings.discord_voice.choose_channel')}</option>
				{#each catalog.channels as item (item.id)}
					<option value={item.id}>{item.guild} · {item.name}</option>
				{/each}
			</select>
		{:else}
			<Input id="discord-voice-channel" bind:value={channel} inputmode="numeric" required />
		{/if}
	</label>
{/snippet}

{#if status && status !== 'unsupported'}
	<section class="mb-7 rounded-xl border bg-card p-4">
		<h3 class="text-sm font-semibold">{t('settings.discord_voice.title')}</h3>
		<p class="mt-1 text-xs text-muted-foreground">{t('settings.discord_voice.description')}</p>
		{#if !active && status !== 'not_installed' && !replacing && botLibrary.entries.length}
			<label class="mt-4 grid gap-1.5 text-xs font-medium">
				{t('settings.discord_voice.saved_bots')}
				<select value={botId} onchange={(event) => selectBot(event.currentTarget.value)}
					disabled={busy} class="h-9 w-full rounded-md border bg-background px-2 text-sm">
					{#if !configured}<option value="">{t('settings.discord_voice.select_bot')}</option>{/if}
					{#each botLibrary.entries as bot (bot.id)}
						<option value={bot.id}>{bot.name}</option>
					{/each}
				</select>
			</label>
		{/if}
		{#if !active && status !== 'not_installed' && settingUp}
			<form class="mt-4 grid gap-3" onsubmit={saveBot}>
				<h4 class="text-sm font-semibold">{t('settings.discord_voice.setup_bot')}</h4>
				{@render profileFields()}
				<p class="text-xs text-muted-foreground">{t('settings.discord_voice.portal_hint')}</p>
				<Button type="button" variant="outline" size="sm" class="justify-self-start"
					onclick={() => openExternal('https://discord.com/developers/applications').catch(() => { actionError = 'failed'; })}>
					{t('settings.discord_voice.portal')}
				</Button>
				<label class="grid gap-1.5 text-xs font-medium">
					{t('settings.discord_voice.token')}
					<Input bind:value={token} type="password" autocomplete="off" disabled={busy}
						oninput={() => { catalog = null; profileFeedback = ''; }} required />
				</label>
				{#if catalog}
					<span class="text-sm font-medium">{catalog.name}</span>
					{@render profileAction()}
					<div class="flex flex-wrap gap-2">
						<Button type="button" variant="outline" size="sm" disabled={busy} onclick={inviteBot}>{t('settings.discord_voice.invite')}</Button>
						<Button type="button" variant="outline" size="sm" disabled={busy} onclick={discover}>{t('settings.discord_voice.refresh_channels')}</Button>
					</div>
					{#if catalog.channels.length}
						{@render channelPicker()}
					{:else}
						<p class="text-xs text-muted-foreground">{t('settings.discord_voice.no_channels')}</p>
					{/if}
				{:else}
					<Button type="button" variant="outline" size="sm" class="justify-self-start" disabled={busy || !token.trim()} onclick={discover}>
						{t('settings.discord_voice.check_bot')}
					</Button>
				{/if}
				<div class="flex flex-wrap gap-2">
					{#if channelName}
						<Button type="submit" size="sm" disabled={busy}>{t('settings.discord_voice.save_bot')}</Button>
					{/if}
					{#if replacing}
						<Button type="button" variant="ghost" size="sm" disabled={busy} onclick={cancelReplacement}>{t('settings.discord_voice.cancel_setup')}</Button>
					{/if}
				</div>
			</form>
		{:else if !active && status !== 'not_installed' && configured}
			<form class="mt-4 grid gap-3" onsubmit={broadcast}>
				{@render channelPicker()}
				{#if catalog && !catalog.channels.length}
					<p class="text-xs text-muted-foreground">{t('settings.discord_voice.no_channels')}</p>
				{/if}
				<Button type="submit" size="sm" class="justify-self-start" disabled={busy || !channel.trim()}>
					{t('settings.discord_voice.start')}
				</Button>
				<div class="flex flex-wrap gap-2">
					<Button type="button" variant="outline" size="sm" disabled={busy} onclick={discover}>
						{t('settings.discord_voice.refresh_channels')}
					</Button>
					{#if catalog}
						<Button type="button" variant="outline" size="sm" disabled={busy} onclick={inviteBot}>{t('settings.discord_voice.invite')}</Button>
					{/if}
					<Button type="button" variant="ghost" size="sm" disabled={busy} onclick={replaceBot}>{t('settings.discord_voice.add_bot')}</Button>
					<Button type="button" variant="ghost" size="sm" disabled={busy} onclick={forgetBot}>{t('settings.discord_voice.forget_bot')}</Button>
				</div>
				{#if catalog}
					{@render profileFields()}
					{@render profileAction()}
				{/if}
			</form>
		{/if}
		<div aria-live="polite">
			{#if actionError || messages[status]}
				<p class="mt-3 text-xs text-muted-foreground" class:text-destructive={!!actionError || (!active && status !== 'not_installed')}>
					{actionError ? messages[actionError] ?? messages.failed : messages[status]}
				</p>
			{/if}
		</div>
		{#if active}
			<Button class="mt-3" size="sm" variant="outline" onclick={leave} disabled={busy || status === 'stopping'}>
				{t('settings.discord_voice.stop')}
			</Button>
		{/if}
	</section>
{/if}
