// Reactive Listen Together state, driven by the Rust `lt-state` event. The modal + home-header
// button read this; mutations go through `api` commands. context/19.
import type { LtState } from './api';

export const roomInvite = $state({ link: '' });

export const lt = $state<LtState>({
    recoveryDisconnected: false,
    recoveryInviter: null,
    recoveryBusy: false,
	localRelayAvailable: false,
	localRelayRunning: false,
	peerHosting: false,
	savedRoom: false,
	savedRoomsError: false,
	savedRooms: [],
	status: 'disconnected',
	role: 'none',
	requesting: false,
	roomCode: null,
	myId: null,
	serverUrl: '',
	defaultServerUrl: '',
	users: [],
	currentTrack: null,
	queue: [],
	pendingJoins: [],
	suggestions: []
});

/** True when we're in (or joining) a room. */
export function inRoom(): boolean {
	return lt.role !== 'none';
}

export function canStartRoom(): boolean {
	return !inRoom() && lt.status === 'disconnected' && !lt.requesting
		&& !lt.recoveryDisconnected && !lt.recoveryBusy;
}

/** Replace the reactive state from a fresh `lt-state` snapshot. */
export function applyLtState(s: LtState) {
	Object.assign(lt, s);
}
