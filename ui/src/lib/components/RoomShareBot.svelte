<script lang="ts">
	import { invoke } from '@tauri-apps/api/core';
	import { onMount } from 'svelte';
	import { Button } from '$lib/components/ui/button';
	import { botLibrary, loadBots } from '$lib/discord-bots.svelte';
	import { t } from '$lib/i18n.svelte';
	import { toast } from '$lib/player.svelte';

	let expanded = $state(false);
	let selected = $state('');
	let busy = $state(false);

	onMount(() => { void loadBots().catch(() => {}); });

	async function share() {
		if (busy || !botLibrary.entries.some((bot) => bot.id === selected)) return;
		busy = true;
		try {
			const count = await invoke<number>('voice_share_bot', { id: selected });
			await loadBots().catch(() => {});
			if (count) {
				toast.success(t('settings.discord_voice.room_sent', { count }));
				expanded = false;
			} else {
				toast.error(t('settings.discord_voice.room_no_recipients'));
			}
		} catch (error) {
			toast.error(String(error));
		} finally {
			busy = false;
		}
	}
</script>

{#if botLibrary.entries.length}
	<div class="mt-3 grid gap-2">
		<Button type="button" variant="outline" size="sm" class="justify-self-start"
			aria-expanded={expanded}
			onclick={() => {
				if (!expanded) selected = botLibrary.entries[0]?.id ?? '';
				expanded = !expanded;
			}}>
			{t('settings.discord_voice.room_share')}
		</Button>
		{#if expanded}
			<label class="grid gap-1.5 text-xs font-medium">
				{t('settings.discord_voice.select_bot')}
				<select bind:value={selected} disabled={busy} class="h-9 rounded-md border bg-background px-2 text-sm">
					{#each botLibrary.entries as bot (bot.id)}
						<option value={bot.id}>{bot.name}</option>
					{/each}
				</select>
			</label>
			<p class="text-xs text-muted-foreground">{t('settings.discord_voice.room_share_hint')}</p>
			<Button type="button" size="sm" class="justify-self-start" disabled={busy || !selected} onclick={share}>
				{t('settings.discord_voice.room_send')}
			</Button>
		{/if}
	</div>
{/if}
