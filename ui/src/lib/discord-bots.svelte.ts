import { invoke } from '@tauri-apps/api/core';

export type SavedBot = { id: string; name: string; channel: string; channel_name: string };
export type BotOffer = { id: string; name: string; sender: string };

export const botLibrary = $state({ entries: [] as SavedBot[] });

export async function loadBots() {
	botLibrary.entries = await invoke<SavedBot[]>('voice_list_bots');
}
