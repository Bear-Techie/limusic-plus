import { getName } from '@tauri-apps/api/app';

export const appBrand = $state({ name: import.meta.env.VITE_APP_NAME || 'Limusic' });

/** The native product name also covers custom builds outside the Windows installer script. */
export async function loadAppBrand() {
	try {
		const name = await getName();
		appBrand.name = name === 'limusic' ? 'Limusic' : name;
	} catch { /* Keep the build's name if native metadata is unavailable. */ }
}
