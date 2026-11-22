import type { LtSavedRoom } from './api';

export type RoomAvailability = 'open' | 'closed' | 'unknown' | 'checking';
export type RoomScope = 'host' | 'join';
export type RoomAddress = Pick<LtSavedRoom, 'serverUrl' | 'roomCode'>;

/** Shared by startup discovery and the Join list. Four read-only probes at a time. */
export async function checkFriendRooms(
	rooms: LtSavedRoom[],
	probe: (room: RoomAddress) => Promise<Exclude<RoomAvailability, 'checking'>>,
	onResult: (room: RoomAddress, result: RoomAvailability) => void = () => {},
	active: () => boolean = () => true
): Promise<Record<string, RoomAvailability>> {
	const queue = rooms.filter((room) => !room.owner);
	const results: Record<string, RoomAvailability> = {};
	let next = 0;
	await Promise.all(Array.from({ length: Math.min(4, queue.length) }, async () => {
		while (active() && next < queue.length) {
			const room = queue[next++];
			let result: RoomAvailability = 'unknown';
			try { result = await probe(room); } catch { /* Unreachable is not proof of a closed room. */ }
			if (active()) {
				results[savedRoomKey(room)] = result;
				onResult(room, result);
			}
		}
	}));
	return results;
}

export function savedRoomKey(room: Pick<LtSavedRoom, 'serverUrl' | 'roomCode'>): string {
	return JSON.stringify([room.serverUrl, room.roomCode]);
}

/** Open rooms always precede closed/unknown favorites. A check never changes recency. */
export function sortedSavedRooms(rooms: LtSavedRoom[], scope: RoomScope, availability: Record<string, RoomAvailability>): LtSavedRoom[] {
	return rooms.filter((room) => room.owner === (scope === 'host')).sort((a, b) =>
		Number(availability[savedRoomKey(b)] === 'open') - Number(availability[savedRoomKey(a)] === 'open') ||
		(b.lastUsedMs || 0) - (a.lastUsedMs || 0) ||
		Number(b.favorite) - Number(a.favorite) ||
		(a.name || a.hostName || a.roomCode).localeCompare(b.name || b.hostName || b.roomCode) ||
		savedRoomKey(a).localeCompare(savedRoomKey(b))
	);
}
